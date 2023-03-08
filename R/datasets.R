#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

#' @include config.R
#' @include datasets_api_client.R
#' @include schema.R
#' @include utils.R
NULL

#' @keywords internal
FOUNDRY_DATA_SIDECAR_RUNTIME <- "foundry-data-sidecar"

#' Reads a tabular Foundry dataset as data.frame or an Apache Arrow Table.
#'
#' @section Column types:
#' Note that types may not match exactly the Foundry column types.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from an arrow Table to a data.frame.
#'
#' @param alias The alias representing the Dataset. The Dataset must be tabular, i.e. have a schema.
#' @param columns The subset of columns to retrieve.
#' @param row_limit The maximum number of rows to retrieve.
#' @param format The output format, can be 'arrow' or 'data.frame'.
#'
#' @return A data.table or an Arrow Table
#'
#' @examples
#' \dontrun{
#' # Download a subset of a tabular Dataset
#' df <- datasets.read_table("my_input", columns = c("columnA", "columnB"), row_limit = 1000)
#' }
#'
#' @export
datasets.read_table <- function(alias, columns = NULL, row_limit = NULL, format = "data.frame") { # nolint: object_name_linter
  if (!format %in% c("arrow", "data.frame")) {
    stop(sprintf("Expected 'format' to be 'arrow' or 'data.frame', found %s", format))
  }
  dataset <- get_alias(alias)

  response <- get_datasets_client()$read_table(dataset_rid = dataset$rid, format = "ARROW",
                                               branch_id = dataset$branch,
                                               start_transaction_rid = dataset$start_transaction_rid,
                                               end_transaction_rid = dataset$end_transaction_rid,
                                               columns = columns, row_limit = row_limit)
  arrow_table <- bin_to_arrow(httr::content(response, "raw"))

  if (format == "arrow") {
    return(arrow_table)
  }
  copy_dataframe(arrow_table$to_data_frame())
}

#' Writes a data.frame to a Foundry dataset.
#'
#' @section Column types:
#' Note that types may not be exactly preserved and all types are not supported.
#' See https://arrow.apache.org/docs/r/articles/arrow.html for details on type conversions
#' from a data.frame to an arrow Table. Use arrow::Table$create to use more granular types.
#'
#' @section Row Names:
#' Row names are silently removed.
#'
#' @param data A data.frame or an arrow Table.
#' @param alias The alias representing the Dataset.
#'
#' @examples
#' \dontrun{
#' datasets.write_table(mtcars, "my_output")
#' }
#'
#' @export
datasets.write_table <- function(data, alias) { # nolint: object_name_linter
  if (inherits(data, "data.frame")) {
    data <- arrow::Table$create(data)
  }
  if (!inherits(data, "Table")) {
    stop("data must be a data.frame or an arrow Table")
  }

  datasets <- get_datasets_client()
  if (get_runtime() == FOUNDRY_DATA_SIDECAR_RUNTIME) {
    return(datasets$foundry_data_sidecar_write_table(alias, body = arrow_to_bin(data)))
  }

  dataset <- get_alias(alias)

  foundry_schema <- arrow_to_foundry_schema(data)
  local_path <- tempfile(fileext = ".parquet")
  arrow::write_parquet(data, local_path)

  datasets$write_file(
    dataset$rid,
    "dataframe.parquet",
    branch_id = dataset$branch,
    transaction_type = "SNAPSHOT",
    body = file_to_bin(local_path))
  file.remove(local_path)

  datasets$put_schema(dataset$rid, branch_id = dataset$branch, foundry_schema = foundry_schema)
  invisible(NULL)
}

#' Lists the files stored in a Foundry Dataset.
#'
#' @param alias The alias representing the Dataset.
#' @param regex A regex used to filter files by path.
#'
#' @return The lists of file properties.
#'
#' @examples
#' \dontrun{
#' # List all PDF files in a Dataset
#' all_files <- datasets.list_files("my_dataset", regex=".*\\.pdf")
#'
#' # Get all file names
#' file_names <- sapply(all_files, function(x) x$path)
#' }
#'
#' @export
datasets.list_files <- function(alias, regex=".*") { # nolint: object_name_linter
  if (!missing(regex)) {
    # Match the regex against the full file path
    regex <- paste0("^", regex, "$")
  }
  dataset <- get_alias(alias)

  datasets <- get_datasets_client()
  files <- datasets$list_files(dataset$rid, branch_id = dataset$branch,
                               start_transaction_rid = dataset$start_transaction_rid,
                               end_transaction_rid = dataset$end_transaction_rid)

  page <- get_files_page(files)
  if (!missing(regex)) {
    page <- page[sapply(page, function(x) grepl(regex, x$path))]
  }
  if (is.null(files$nextPageToken)) {
    return(page)
  }
  data <- list(page)
  while (!is.null(files$nextPageToken)) {
    files <- datasets$list_files(dataset$rid, branch_id = dataset$branch,
                                 start_transaction_rid = dataset$start_transaction_rid,
                                 end_transaction_rid = dataset$end_transaction_rid, page_token = files$nextPageToken)
    page <- get_files_page(files)
    if (!missing(regex)) {
      page <- page[sapply(page, function(x) grepl(regex, x$path))]
    }
    data[[length(data) + 1]] <- page
  }
  return(unlist(data, recursive = FALSE))
}

#' @keywords internal
get_files_page <- function(response) {
  if (get_runtime() == FOUNDRY_DATA_SIDECAR_RUNTIME) {
    return(response$files)
  } else {
    return(response$data)
  }
}

#' Download Foundry Files locally.
#'
#' @param alias The alias representing the Dataset.
#' @param files The file paths or file properties.
#'
#' @return A list mapping Foundry Dataset files to the local file paths where files were downloaded.
#'
#' @examples
#' \dontrun{
#' # Download a single file in a Dataset
#' downloaded_file <- datasets.download_files("my_alias", c("dir/my_file.csv"))
#' read.csv(downloaded_file$`dir/my_file.csv`)
#'
#' # Extract text from all PDF files in a Dataset
#' pdf_files <- datasets.list_files("my_alias", regex = ".*\\.pdf")
#' downloaded_files <- datasets.download_files("my_alias", pdf_files)
#' contents <- lapply(downloaded_files, pdftools::pdf_text)
#' }
#'
#' @export
datasets.download_files <- function(alias, files) { # nolint: object_name_linter
  dataset <- get_alias(alias)

  if (!inherits(files, "character")) {
    files <- sapply(files, function(x) x$path)
  }

  datasets <- get_datasets_client()
  if (get_runtime() == FOUNDRY_DATA_SIDECAR_RUNTIME) {
    return(datasets$foundry_data_sidecar_download_files(alias, files)$files)
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

  downloads <- sapply(files, create_parent_and_download)
  downloads <- split(downloads, names(downloads))
  return(lapply(downloads, unname))
}

#' Upload a local file or folder to a Foundry Dataset.
#'
#' @param files The local files and folders to upload.
#' If a folder is provided, all files found recursively in subfolders will be uploaded.
#' @param alias The alias representing the Dataset.
#'
#' @return A list mapping local file paths to the corresponding Foundry Dataset file.
#'
#' @examples
#' \dontrun{
#' # Upload RDS files to a Dataset
#' local_dir <- file.path(tempdir(), "to_upload")
#' dir.create(local_dir)
#' saveRDS(iris, file.path(local_dir, "iris.rds"))
#' saveRDS(Titanic, file.path(local_dir, "Titanic.rds"))
#'
#' datasets.upload_files(local_dir, "my_output")
#' }
#'
#' @export
datasets.upload_files <- function(files, alias) { # nolint: object_name_linter
  dataset <- get_alias(alias)

  missing_files <- files[sapply(files, function(x) !file.exists(x))]
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

  if (get_runtime() == FOUNDRY_DATA_SIDECAR_RUNTIME) {
    upload_id <- get_upload_id()
    file_count <- length(files_to_upload)
    upload_file_to_transaction <- function(file_path, file_name) {
      datasets$foundry_data_sidecar_write_file(alias, file_name,
                                               upload_id = upload_id, file_count = file_count,
                                               body = file_to_bin(file_path))
    }
    lapply(files_to_upload, function(file_to_upload) {
      upload_file_to_transaction(file_to_upload$file, file_to_upload$file_name)
    })
  } else {
    is_transaction_managed <- is.null(dataset$transaction_rid)

    if (is_transaction_managed) {
      txn <- datasets$create_transaction(dataset$rid, branch_id = dataset$branch,
                                         transaction_type = dataset$transaction_type)
      transaction_rid <- txn$rid
    } else {
      transaction_rid <- dataset$transaction_rid
    }

    upload_file_to_transaction <- function(file_path, file_name) {
      datasets$write_file(dataset$rid, file_name, transaction_rid = transaction_rid,
                          body = file_to_bin(file_path))
    }
    tryCatch(
      {
        lapply(files_to_upload, function(file_to_upload) {
          upload_file_to_transaction(file_to_upload$file, file_to_upload$file_name)
        })
        if (is_transaction_managed) {
          datasets$commit_transaction(dataset$rid, transaction_rid)
        }
      },
      error = function(cond) {
        if (is_transaction_managed) {
          datasets$abort_transaction(dataset$rid, transaction_rid)
          stop("An error occurred while uploading files, aborted the transaction: \n", cond)
        }
        warning("An error occurred while uploading files to the provided transaction.\n")
      }
    )
  }
  # Return a named list instead of an array of mappings
  names(files_to_upload) <- lapply(files_to_upload, function(x) x$file)
  return(lapply(files_to_upload, function(x) x$file_name))
}
