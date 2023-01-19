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

#' Reads a tabular Foundry dataset as data.frame or an arrow Table.
#'
#' Note that types may not match exactly the Foundry column types.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from an arrow Table to a data.frame.
#' For more advanced usage, use `read_table_arrow`.
#'
#' @param alias The alias representing the Dataset. It must be tabular, i.e. have a schema.
#' @param columns The subset of columns to retrieve.
#' @param row_limit The maximum number of rows to retrieve.
#' @param format The output format, can be 'arrow' or 'data.frame'.
#'
#' @return A data.table or an arrow Table
#'
#' @export
datasets.read_table <- function(alias, columns = NULL, row_limit = NULL, format = "data.frame") { # nolint: object_name_linter
  if (!format %in% c("arrow", "data.frame")) {
    stop(sprintf("Expected 'format' to be 'arrow' or 'data.frame', found %s", format))
  }
  dataset <- get_alias(alias)

  response <- get_datasets_client()$read_table(dataset_rid = dataset$rid, format = "ARROW",
                                               branch_id = dataset$branch_id,
                                               start_transaction_rid = dataset$start_transaction_rid,
                                               end_transaction_rid = dataset$end_transaction_rid,
                                               columns = columns, row_limit = row_limit)

  stream <- arrow::BufferReader$create(httr::content(response, "raw"))
  reader <- arrow::RecordBatchStreamReader$create(stream)
  arrow_table <- reader$read_table()

  if (format == "arrow") {
    return(arrow_table)
  }
  df <- arrow_table$to_data_frame()
  head(df, n = nrow(df))
}

#' Writes a data.frame to a Foundry dataset.
#'
#' Note that types may not be exactly preserved and all types are not supported.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from a data.frame to an arrow Table. Use arrow::arrow_table to use more granular types.
#'
#' @param data A data.frame or an arrow Table.
#' @param alias The alias representing the Dataset.
#'
#' @export
datasets.write_table <- function(data, alias) { # nolint: object_name_linter
  if (inherits(data, "data.frame")) {
    data <- arrow::arrow_table(data)
  }
  if (!inherits(data, "Table")) {
    stop("data must be a data.frame or an arrow Table")
  }
  dataset <- get_alias(alias)

  foundry_schema <- arrow_to_foundry_schema(data)
  local_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data, local_path)

  datasets <- get_datasets_client()
  datasets$write_file(
    dataset$rid,
    "dataframe.parquet",
    branch_id = dataset$branch,
    transaction_type = "SNAPSHOT",
    body = readBin(local_path, "raw", n = file.info(local_path)$size))
  file.remove(local_path)

  datasets$put_schema(dataset$rid, branch_id = dataset$branch, foundry_schema = foundry_schema)
  invisible(NULL)
}

#' Lists the files stored in a Foundry Dataset.
#'
#' @param alias The alias representing the Dataset.
#'
#' @return The lists of file properties.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # List all PDF files in dataset
#' all_files <- datasets.list_files("my_dataset")
#' pdf_files <- all_files[sapply(all_files, function(x) grepl(".*\\.pdf", x$path))]
#'
#' # Get all file names
#' file_names <- sapply(all_files, function(x) x$path)
#' }
datasets.list_files <- function(alias) { # nolint: object_name_linter
  dataset <- get_alias(alias)

  datasets <- get_datasets_client()
  files <- datasets$list_files(dataset$rid, branch_id = dataset$branch,
                               start_transaction_rid = dataset$start_transaction_rid,
                               end_transaction_rid = dataset$end_transaction_rid)

  if (is.null(files$nextPageToken)) {
    return(files$data)
  }
  data <- list(files$data)
  nfiles <- length(files$data)
  while (!is.null(files$nextPageToken)) {
    files <- datasets$list_files(dataset$rid, branch_id = dataset$branch,
                                 start_transaction_rid = dataset$start_transaction_rid,
                                 end_transaction_rid = dataset$end_transaction_rid, page_token = files$nextPageToken)
    data[[length(data) + 1]] <- files$data
    nfiles <- nfiles + length(files$data)
  }
  return(unlist(files$data, recursive = FALSE))
}

#' Download Foundry Files locally.
#'
#' @param alias The alias representing the Dataset.
#' @param files The file paths or file properties.
#'
#' @return A named vector with local file paths where files were downloaded.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Download all files in dataset
#' all_files <- datasets.list_files("my_alias")
#' downloaded_files <- datasets.download_files("my_alias", all_files)
#' read.csv(downloaded_files[["my_file.csv"]])
#'
#' # Download a single file in dataset
#' downloaded_file <- datasets.download_files("my_alias", c("my_file.txt"))
#' }
datasets.download_files <- function(alias, files) { # nolint: object_name_linter
  dataset <- get_alias(alias)

  if (class(files) != "character") {
    files <- sapply(files, function(x) x$path)
  }

  datasets <- get_datasets_client()
  if (is_internal()) {
    return(datasets$download_files(alias, list(files = files)))
  }

  target <- tempdir()
  target <- file.path(target, alias)
  if (!dir.exists(target)) {
    dir.create(target)
  }

  create_parent_and_download <- function(file_path) {
    path <- file.path(target, gsub("/", .Platform$file.sep, file_path))
    if (!dir.exists(dirname(path))) {
      dir.create(dirname(path), recursive = TRUE)
    }
    file <- datasets$read_file(dataset$rid, file_path, branch_id = dataset$branch,
                               start_transaction_rid = dataset$start_transaction_rid,
                               end_transaction_rid = dataset$end_transaction_rid)
    writeBin(file$content, path)
    return(path)
  }

  return(sapply(files, create_parent_and_download))
}

#' Upload a local file or folder to a Foundry Dataset.
#'
#' @param files The local files and folders to upload.
#' If a folder is provided, all files found recursively in subfolders will be uploaded.
#' @param alias The alias representing the Dataset.
#'
#' @return The list of local file paths and corresponding file name in the Foundry dataset.
#'
#' @export
datasets.upload_files <- function(files, alias) { # nolint: object_name_linter
  dataset <- get_alias(alias)

  missing_files <- sapply(files, function(x) !file.exists(x))
  if (length(missing_files) > 0) {
    stop(sprintf("The following local files do not exist: %s", paste(missing_files, collapse = ", ")))
  }

  get_file_to_upload <- function(local_file) {
    if (!dir.exists(local_file)) {
      return(list(list(file_name = basename(local_file), file = local_file)))
    }
    return(lapply(list.files(local_file, recursive = TRUE), function(path) {
        list(file_name = gsub(.Platform$file.sep, "/", path), file = file.path(local_file, path))
      }))
  }

  files_to_upload <- unlist(lapply(files, get_file_to_upload), recursive = FALSE)
  files_to_upload <- files_to_upload[!duplicated(files_to_upload)]

  # Find duplicates names corresponding to different files
  file_names <- lapply(files_to_upload, function(file_to_upload) file_to_upload$file_name)
  duplicate_file_names <- file_names[duplicated(file_names)]
  if (length(duplicate_file_names) > 1) {
    stop(sprintf("Multiple files would be uploaded to the same location, aborting: %s",
                 paste(file_names, collapse = ", ")))
  }

  datasets <- get_datasets_client()
  if (is.null(dataset$transaction_rid)) {
    txn <- datasets$create_transaction(dataset$rid, branch_id = dataset$branch_id,
                                       transaction_type = dataset$transaction_type)
    transaction_rid <- txn$rid
  } else {
    transaction_rid <- dataset$transaction_rid
  }

  upload_file_to_transaction <- function(file_path, file_name) {
    datasets$write_file(dataset$rid, file_name, transaction_rid = transaction_rid,
                        body = readBin(file_path, "raw", n = file.info(file_path)$size))
  }
  tryCatch(
    {
      lapply(files_to_upload, function(file_to_upload) {
        upload_file_to_transaction(file_to_upload$file, file_to_upload$file_name)
      })
      if (is.null(dataset$transaction_rid)) {
        datasets$commit_transaction(dataset$rid, transaction_rid)
      }
    },
    error = function(cond) {
      if (is.null(dataset$transaction_rid)) {
        datasets$abort_transaction(dataset$rid, transaction_rid)
        stop("An error occurred while uploading files, aborted the transaction: \n", cond)
      }
    }
  )
  return(files_to_upload)
}
