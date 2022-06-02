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

#' Retrieves a Foundry Dataset.
#'
#' Returns an object representing a specific view of a Foundry dataset.
#'
#' @param dataset_ref A dataset RID or path.
#' @param branch The dataset branch.
#' @param transaction_range A list of 2 nullable characters representing the start and end transaction RIDs.
#' @param create A boolean indicating whether the dataset should be created if it does not exist yet.
#'
#' @return A Dataset object representing the dataset view.
#'
#' @export
get_dataset <- function(dataset_ref, branch = NULL, transaction_range = NULL, create = FALSE) {
  pypalantir$datasets$dataset(
    dataset_ref,
    branch,
    transaction_range = transaction_range,
    ctx = get_context(),
    create = create)
}

#' Reads a tabular Foundry dataset as data.frame.
#'
#' Note that types may not match exactly the Foundry column types.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from an arrow Table to a data.frame.
#' For more advanced usage, use `read_table_arrow`.
#'
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' The Foundry dataset must be tabular, i.e. have a schema.
#'
#' @return A data.table
#'
#' @export
read_table <- function(dataset) {
  read_table_arrow(dataset)$to_data_frame()
}

#' Reads a tabular Foundry dataset as an arrow Table.
#'
#' The `arrow` library must be loaded prior to calling this function.
#'
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' The Foundry dataset must be tabular, i.e. have a schema.
#'
#' @return An arrow Table
#'
#' @export
#' @examples
#' \dontrun{
#' library(arrow)
#' library(dplyr)
#'
#' df <- read_table_arrow("/path/to/dataset")
#'     %>% select("column" == "value")
#'     %>% collect()
#' }
read_table_arrow <- function(dataset) {
  dataset <- get_dataset_internal(dataset)
  context <- get_context()
  sql_query <- SqlQueryService$new(
    hostname = context$hostname,
    auth_token = context$auth_token,
    user_agent = get_user_agent())
  sql_query$read_dataset(dataset$locator)
}

#' Writes a data.frame to a Foundry dataset.
#'
#' Note that types may not be exactly preserved and all types are not supported.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from a data.frame to an arrow Table. Use arrow::arrow_table to use more granular types.
#'
#' @param data A data.frame or an arrow Table.
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#'
#' @export
write_table <- function(data, dataset) {
  dataset <- get_dataset_internal(dataset, create = TRUE)

  if (inherits(data, "data.frame")) {
    data <- arrow::arrow_table(data)
  }
  if (!inherits(data, "Table")) {
    stop("data must be a data.frame or an arrow Table")
  }

  foundry_schema <- arrow_to_foundry_schema(data)
  target <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data, target)
  upload_file(target, dataset, txn_type = "SNAPSHOT")
  file.remove(target)

  dataset$client$put_schema(dataset, foundry_schema)
  invisible(NULL)
}

#' Lists the files stored in a Foundry Dataset.
#'
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' @param path If present, only the specified file or the files in the specified folder will be returned.
#'
#' @return A list of File objects representing the files in the dataset.
#'
#' @export
list_files <- function(dataset, path = NULL) {
  dataset <- get_dataset_internal(dataset)
  response <- reticulate::iterate(dataset$list_files(path))
  response
}

#' Download a Foundry File locally.
#'
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' @param logical_path A character string representing the logical path of the file in the Dataset.
#' @param target A character string representing the location where the file should be downloaded.
#' @param ... Extra parameters to pass to the download.file() function.
#'
#' @export
download_file <- function(dataset, logical_path, target, ...) {
  if (!is.character(logical_path)) {
    stop("logical_path should be a character string")
  }
  dataset <- get_dataset_internal(dataset)
  utils::download.file(
    get_file_in_txn_view_url(dataset, logical_path),
    target,
    headers = c("Authorization" = paste("Bearer", dataset$client$ctx$auth_token)),
    ...)
}

#' Download multiple files from a Foundry Dataset locally.
#'
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' @param target A character string representing the directory where the file should be downloaded.
#' The directory or its parent should exist.
#' @param path If present, only the specified file or the files in the specified folder will be downloaded.
#' @param ... Extra parameters to pass to the download.file() function.
#'
#' @export
download_files <- function(dataset, target, path = NULL, ...) {
  files <- list_files(dataset, path)
  if (length(files) == 0) {
    stop("No files found in the dataset.")
  }
  if (!dir.exists(target)) {
    dir.create(target)
  } else if (length(list.files(target)) != 0) {
    stop("Target directory is not empty, files will not be downloaded.")
  }
  if (!is.null(path) && !endsWith(path, "/")) {
    path <- paste0(path, "/")
  }
  create_parent_and_download <- function(file_) {
    file_path <- reticulate::py_to_r(file_$path)
    if (!is.null(path)) {
      file_path <- substr(file_path, nchar(path) + 1, nchar(file_path))
    }
    path_ <- paste(target, file_path, sep = "/")
    if (!dir.exists(dirname(path_))) {
      dir.create(dirname(path_), recursive = TRUE)
    }
    download_file(dataset, file_path, path_, ...)
  }
  cat(paste0("Downloading ", length(files), " files to local directory ", target, "\n"))
  lapply(files, create_parent_and_download)
  invisible(NULL)
}

#' Upload a local file or folder to a Foundry Dataset.
#'
#' @param local_file A character string representing the location of the file or folder to upload.
#' If a folder is provided, all files found recursively in subfolders will be uploaded.
#' @param dataset A Dataset or a character string representing the RID or dataset path.
#' @param txn_type The type of the transaction, must be one of 'UPDATE', 'APPEND', 'SNAPSHOT'.
#'
#' @export
upload_file <- function(local_file, dataset, txn_type = "UPDATE") {
  if (!file.exists(local_file)) {
    stop("The file to upload does not exist: ", local_file)
  }
  if (!txn_type %in% c("UPDATE", "APPEND", "SNAPSHOT")) {
    stop("The transaction type must be one of 'UPDATE', 'APPEND', 'SNAPSHOT'")
  }
  dataset <- get_dataset_internal(dataset, create = TRUE)
  txn <- dataset$start_transaction(txn_type = txn_type)
  tryCatch(
    {
      if (dir.exists(local_file)) {
        lapply(list.files(local_file, recursive = TRUE), function(path_) {
          upload_file_internal(txn, file.path(local_file, path_), path_)
        })
      } else {
        upload_file_internal(txn, local_file, basename(local_file))
      }
      txn$commit()
    },
    error = function(cond) {
      txn$abort()
      stop("An error occurred while uploading files, aborted the transaction: \n", cond)
    }
  )
  invisible(NULL)
}

#' @keywords internal
upload_file_internal <- function(txn, local_path, logical_path) {
  txn$write(logical_path, readBin(local_path, "raw", n = file.info(local_path)$size))
}

#' @keywords internal
get_context <- function() {
  pypalantir$core$context(hostname = Sys.getenv("PALANTIR_HOSTNAME"), token = Sys.getenv("PALANTIR_TOKEN"))
}

#' @keywords internal
get_user_agent <- function() {
  user_agent <- reticulate::py_to_r(pypalantir$core$rpc$USER_AGENT)
  user_agent <- lapply(user_agent, paste, collapse = "/")
  paste(user_agent, collapse = " ")
}

#' @keywords internal
get_dataset_internal <- function(dataset, create = FALSE) {
  if (is.character(dataset)) {
    dataset <- get_dataset(dataset_ref = dataset, create = create)
  }
  if (!inherits(dataset, "palantir.datasets.core.Dataset")) {
    stop("dataset must be a Dataset or a character string")
  }
  dataset
}

#' @keywords internal
get_file_in_txn_view_url <- function(dataset, logical_path) {
  paste(
    dataset$client$`_data_proxy_service`$`_uri`,
    "dataproxy",
    "datasets",
    dataset$locator$rid,
    "views",
    dataset$locator$end_transaction_rid,
    utils::URLencode(logical_path),
    sep = "/")
}
