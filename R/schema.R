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

#' @keywords internal
ARROW_TO_FOUNDRY_FIELDS <- c(
  int8 = "BYTE",
  int16 = "SHORT",
  int32 = "INTEGER",
  int64 = "LONG",
  float16 = "FLOAT",
  halffloat = "FLOAT",
  float32 = "FLOAT",
  float = "FLOAT",
  float64 = "DOUBLE",
  double = "DOUBLE",
  boolean = "BOOLEAN",
  bool = "BOOLEAN",
  utf8 = "STRING",
  large_utf8 = "STRING",
  string = "STRING",
  binary = "BINARY",
  large_binary = "BINARY",
  date32 = "DATE",
  timestamp = "TIMESTAMP"
)

#' @keywords internal
arrow_to_foundry_schema <- function(arrow_data) {
  list(
    fieldSchemaList = lapply(arrow_data$schema, get_field),
    dataFrameReaderClass = "com.palantir.foundry.spark.input.ParquetDataFrameReader",
    customMetadata = list(
      format = "parquet"
    )
  )
}

#' @keywords internal
get_field <- function(field) {
  foundry_type <- ARROW_TO_FOUNDRY_FIELDS[field$type$name]
  if (is.na(foundry_type)) {
    stop("Unsupported column type: ", field$type$name)
  }
  list(
    name = field$name,
    type = foundry_type
  )
}
