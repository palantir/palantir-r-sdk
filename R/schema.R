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

#' @keywords internal
ARROW_TO_FOUNDRY_FIELDS <- c( # nolint
  int8 = "byte",
  int16 = "short",
  int32 = "integer",
  int64 = "long",
  float16 = "float",
  halffloat = "float",
  float32 = "float",
  float = "float",
  float64 = "double",
  double = "double",
  boolean = "boolean",
  bool = "boolean",
  utf8 = "string",
  large_utf8 = "string",
  string = "string",
  binary = "binary",
  large_binary = "binary",
  date32 = "date",
  timestamp = "timestamp"
)

#' @keywords internal
arrow_to_foundry_schema <- function(arrow_data) {
  pypalantir$datasets$types$FoundrySchema(
    fields = lapply(arrow_data$schema, get_field)
  )
}

#' @keywords internal
get_field <- function(field) {
  foundry_type <- ARROW_TO_FOUNDRY_FIELDS[field$type$name]
  if (is.na(foundry_type)) {
    stop("Unsupported column type: ", field$type$name)
  }
  pypalantir$datasets$types$Field(
    name = field$name,
    field_type = foundry_type,
    nullable = field$nullable
  )
}
