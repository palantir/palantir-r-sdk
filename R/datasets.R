#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#' @include schema.R
NULL

#' Reads a tabular Foundry dataset as data.frame.
#'
#' Note that types may not match exactly the Foundry column types.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from an arrow Table to a data.frame.
#' For more advanced usage, use `read_table_arrow`.
#'
#' @param dataset_rid The RID of the Dataset. It must be tabular, i.e. have a schema.
#' @param branch_id The identifier (name) of the Branch.
#' @param start_transaction_rid The Resource Identifier (RID) of the start Transaction.
#' @param end_transaction_rid The Resource Identifier (RID) of the end Transaction.
#'
#' @return A data.table
#'
#' @export
read_table <- function(dataset_rid, branch_id = NULL, start_transaction_rid = NULL, end_transaction_rid = NULL,
                       columns = NULL, row_limit = NULL) {
  arrow_table <- read_table_arrow(dataset_rid = dataset_rid, branch_id = branch_id,
                                  start_transaction_rid = start_transaction_rid,
                                  end_transaction_rid = end_transaction_rid, columns = columns, row_limit = row_limit)
  df <- arrow_table$to_data_frame()
  head(df, n = nrow(df))
}

#' Reads a tabular Foundry dataset as an arrow Table.
#'
#' @param dataset_rid The RID of the Dataset. It must be tabular, i.e. have a schema.
#' @param branch_id The identifier (name) of the Branch.
#' @param start_transaction_rid The Resource Identifier (RID) of the start Transaction.
#' @param end_transaction_rid The Resource Identifier (RID) of the end Transaction.
#'
#' @return An arrow Table
#'
#' @export
read_table_arrow <- function(dataset_rid, branch_id = NULL, start_transaction_rid = NULL, end_transaction_rid = NULL,
                             columns = NULL, row_limit = NULL) {
  response <- get_datasets_client()$export_table(dataset_rid = dataset_rid, format = "ARROW", branch_id = branch_id,
                                                 start_transaction_rid = start_transaction_rid,
                                                 end_transaction_rid = end_transaction_rid, columns = columns,
                                                 row_limit = row_limit)

  stream <- arrow::BufferReader$create(httr::content(response, "raw"))
  reader <- arrow::RecordBatchStreamReader$create(stream)
  reader$read_table()
}

#' Writes a data.frame to a Foundry dataset.
#'
#' Note that types may not be exactly preserved and all types are not supported.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from a data.frame to an arrow Table. Use arrow::arrow_table to use more granular types.
#'
#' @param data A data.frame or an arrow Table.
#' @param dataset_rid The RID of the Dataset.
#' @param branch_id The identifier (name) of the Branch.
#'
#' @export
write_table <- function(data, dataset_rid, branch_id = NULL) {
  if (inherits(data, "data.frame")) {
    data <- arrow::arrow_table(data)
  }
  if (!inherits(data, "Table")) {
    stop("data must be a data.frame or an arrow Table")
  }

  foundry_schema <- arrow_to_foundry_schema(data)
  local_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data, local_path)

  datasets <- get_datasets_client()
  datasets$upload_file(
    dataset_rid,
    "dataframe.parquet",
    transaction_type = "SNAPSHOT",
    body = readBin(local_path, "raw", n = file.info(local_path)$size))
  file.remove(local_path)

  datasets$put_schema(dataset_rid, branch_id = branch_id, foundry_schema = foundry_schema)
  invisible(NULL)
}

#' Lists the files stored in a Foundry Dataset.
#'
#' @param dataset_rid The RID of the Dataset.
#' @param branch_id The identifier (name) of the Branch.
#' @param start_transaction_rid The Resource Identifier (RID) of the start Transaction.
#' @param end_transaction_rid The Resource Identifier (RID) of the end Transaction.
#' @param limit The maximum number of files to return.
#'
#' @return A list of File objects representing the files in the dataset.
#'
#' @export
list_files <- function(dataset_rid, branch_id = NULL, start_transaction_rid = NULL, end_transaction_rid = NULL,
                       limit = NULL) {

  datasets <- get_datasets_client()

  files <- datasets$list_files(dataset_rid, branch_id = branch_id, start_transaction_rid = start_transaction_rid,
                               end_transaction_rid = end_transaction_rid, page_size = limit)

  if (is.null(files$nextPageToken)) {
    return(files$data)
  }

  data <- list(files$data)
  nfiles <- length(files$data)
  while (!is.null(files$nextPageToken)) {
    if (!is.null(limit)) {
      if (length(files$data) >= limit) {
          break
      }
      limit <- limit - length(files$data)
    }
    files <- datasets$list_files(dataset_rid, branch_id = branch_id, start_transaction_rid = start_transaction_rid,
                                 end_transaction_rid = end_transaction_rid, page_token = files$nextPageToken,
                                 page_size = limit)
    data[[length(data) + 1]] <- files$data
    nfiles <- nfiles + length(files$data)
  }
  unlist(files$data, recursive = FALSE)
}

#' Download a Foundry File locally.
#'
#' @param dataset_rid The RID of the Dataset.
#' @param file_path A character string representing the logical path of the file in the Dataset.
#' @param target A character string representing the location where the file should be downloaded.
#' @param branch_id The identifier (name) of the Branch.
#' @param start_transaction_rid The Resource Identifier (RID) of the start Transaction.
#' @param end_transaction_rid The Resource Identifier (RID) of the end Transaction.
#' @param overwrite whether the target should be overwritten.
#'
#' @export
download_file <- function(dataset_rid, file_path, target, branch_id = NULL, start_transaction_rid = NULL,
                          end_transaction_rid = NULL, overwrite = FALSE) {
  file <- get_datasets_client()$get_file_content(dataset_rid, file_path, branch_id = branch_id,
                                                 start_transaction_rid = start_transaction_rid,
                                                 end_transaction_rid = end_transaction_rid)
  writeBin(file$content, target)
}

#' Download multiple files from a Foundry Dataset locally.
#'
#' @param dataset_rid The RID of the Dataset.
#' @param target A character string representing the directory where the file should be downloaded.
#' The directory or its parent should exist. If it exists, the directory must be empty.
#' @param branch_id The identifier (name) of the Branch.
#' @param start_transaction_rid The Resource Identifier (RID) of the start Transaction.
#' @param end_transaction_rid The Resource Identifier (RID) of the end Transaction.
#'
#' @export
download_files <- function(dataset_rid, target, branch_id = NULL, start_transaction_rid = NULL,
                           end_transaction_rid = NULL) {
  if (!dir.exists(target)) {
    dir.create(target)
  } else if (length(list.files(target)) != 0) {
    stop("Target directory is not empty, files will not be downloaded.")
  }
  files <- list_files(dataset_rid, branch_id = NULL, start_transaction_rid = NULL, end_transaction_rid = NULL)
  if (length(files) == 0) {
    stop("No files found in the dataset.")
  }
  create_parent_and_download <- function(file_) {
    file_path <- file_$path
    path <- paste(target, file_path, sep = "/")
    if (!dir.exists(dirname(path))) {
      dir.create(dirname(path), recursive = TRUE)
    }
    download_file(dataset_rid, file_path, path, end_transaction_rid = file_$transactionRid)
  }
  cat(paste0("Downloading ", length(files), " files to local directory ", target, "\n"))
  lapply(files, create_parent_and_download)
  invisible(NULL)
}

#' Upload a local file or folder to a Foundry Dataset.
#'
#' @param local_file A character string representing the location of the file or folder to upload.
#' If a folder is provided, all files found recursively in subfolders will be uploaded.
#' @param dataset_rid The RID of the Dataset.
#' @param branch_id The identifier (name) of the Branch.
#' @param transaction_type The type of the Transaction to create.
#'
#' @export
upload_file <- function(local_file, dataset_rid, branch_id = NULL, transaction_type = NULL) {
  if (!file.exists(local_file)) {
    stop("The file to upload does not exist: ", local_file)
  }
  if (!dir.exists(local_file)) {
    get_datasets_client()$upload_file(dataset_rid, basename(local_file), branch_id = branch_id,
                                      transaction_type = transaction_type,
                                      body = readBin(local_file, "raw", n = file.info(local_file)$size))
    return(invisible(NULL))
  }

  datasets <- get_datasets_client()
  txn <- datasets$create_transaction(dataset_rid, branch_id = branch_id, transaction_type = transaction_type)
  upload_file_to_transaction <- function(file_path, file_name) {
    datasets$upload_file(dataset_rid, file_name, transaction_rid = txn$rid,
                         body = readBin(file_path, "raw", n = file.info(file_path)$size))
  }
  tryCatch(
    {
      lapply(list.files(local_file, recursive = TRUE), function(path_) {
        upload_file_to_transaction(file.path(local_file, path_), path_)
      })
      datasets$commit_transaction(dataset_rid, txn$rid)
    },
    error = function(cond) {
      datasets$abort_transaction(dataset_rid, txn$rid)
      stop("An error occurred while uploading files, aborted the transaction: \n", cond)
    }
  )
  invisible(NULL)
}

#' @keywords internal
get_datasets_client <- function() {
  version <- toString(utils::packageVersion("palantir"))
  DatasetsApiService$new(
    hostname = get_config("PALANTIR_HOSTNAME", "palantir.hostname"),
    auth_token = get_config("PALANTIR_TOKEN", "palantir.token"),
    user_agent = sprintf("palantir-r-sdk/%s", version),
    timeout = getOption("palantir.timeout", 150))
}

#' @keywords internal
get_config <- function(environment_variable, option_name) {
  value <- Sys.getenv(environment_variable, unset = NA)
  if (is.na(value)) {
    value <- getOption(option_name)
    if (is.null(value)) {
      stop(sprintf("The %s environment variable or the %s option must be set.", environment_variable, option_name))
    }
  }
  return(value)
}
