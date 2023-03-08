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

# R 4.2.0 does not consider code in classes created at install time, so the dependency check fails
# Workaround per https://community.rstudio.com/t/new-r-cmd-check-note-in-r-4-2-0-for-imports-field/143153/2
#' @keywords internal
unused <- function() {
  R6::R6Class
  jsonlite::toJSON
}

#' @keywords internal
get_upload_id <- function() {
  paste0(sample(letters, 8, replace = TRUE), collapse = "")
}

#' @keywords internal
file_to_bin <- function(path) {
  readBin(path, "raw", n = file.info(path)$size)
}

#' @keywords internal
arrow_to_bin <- function(arrow_table) {
  local_path <- tempfile(fileext = ".arrow")
  sink <- arrow::FileOutputStream$create(local_path)
  writer <- arrow::RecordBatchStreamWriter$create(sink, arrow_table$schema)
  writer$write_table(arrow_table)
  writer$close()
  sink$close()
  file_to_bin(local_path)
}

#' @keywords internal
bin_to_arrow <- function(bytes) {
  stream <- arrow::BufferReader$create(bytes)
  reader <- arrow::RecordBatchStreamReader$create(stream)
  reader$read_table()
}

#' @keywords internal
copy_dataframe <- function(df) {
  df[, , drop = FALSE]
}
