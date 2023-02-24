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

#' @include api_client.R
#' @include config.R
NULL

#' @keywords internal
DatasetsApiService <- R6::R6Class(
  "DatasetsApiService",
  public = list(
    api_client = NULL,
    initialize = function(auth_token, base_path) {
      self$api_client <- ApiClient$new(
        base_path = base_path,
        access_token = auth_token)
    },
    abort_transaction = function(dataset_rid, transaction_rid) {
      url_path <- sprintf("/%s/transactions/%s/abort", dataset_rid, transaction_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list("preview" = TRUE)

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        header_params = header_params,
        query_params = query_params)

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    commit_transaction = function(dataset_rid, transaction_rid) {
      url_path <- sprintf("/%s/transactions/%s/commit", dataset_rid, transaction_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list("preview" = TRUE)

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        header_params = header_params,
        query_params = query_params)

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    create_transaction = function(dataset_rid, branch_id = NULL, transaction_type = NULL) {
      url_path <- sprintf("/%s/transactions", dataset_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["branchId"] <- branch_id

      if (!is.null(transaction_type)) {
        body <- jsonlite::toJSON(list(transactionType = transaction_type), auto_unbox = TRUE)
      } else {
        body <- "{}"
      }

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        header_params = header_params,
        query_params = query_params,
        body = charToRaw(body))

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    read_table = function(dataset_rid, format, branch_id = NULL, start_transaction_rid = NULL,
                            end_transaction_rid = NULL, columns = NULL, row_limit = NULL) {
      url_path <- sprintf("/%s/readTable", dataset_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["branchId"] <- branch_id
      query_params["startTransactionRid"] <- start_transaction_rid
      query_params["endTransactionRid"] <- end_transaction_rid
      query_params["format"] <- format
      if (!is.null(columns)) {
        query_params["columns"] <- paste(columns, collapse = ",")
      }
      query_params["rowLimit"] <- row_limit

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "GET",
        header_params = header_params,
        query_params = query_params)

      self$api_client$stop_for_status(response)
    },
    list_files = function(dataset_rid, branch_id = NULL, start_transaction_rid = NULL, end_transaction_rid = NULL,
                          page_size = NULL, page_token = NULL) {
      url_path <- sprintf("/%s/files", dataset_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["branchId"] <- branch_id
      query_params["startTransactionRid"] <- start_transaction_rid
      query_params["endTransactionRid"] <- end_transaction_rid
      query_params["pageSize"] <- page_size
      query_params["pageToken"] <- page_token

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "GET",
        header_params = header_params,
        query_params = query_params)

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    put_schema = function(dataset_rid, branch_id = NULL, foundry_schema = NULL) {
      url_path <- sprintf("/%s/schema", dataset_rid)
      header_params <- c("Content-Type" = "application/json")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["branchId"] <- branch_id

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "PUT",
        header_params = header_params,
        query_params = query_params,
        body = jsonlite::toJSON(foundry_schema, auto_unbox = TRUE))

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    write_file = function(
        dataset_rid,
        file_path,
        branch_id = NULL,
        transaction_type = NULL,
        transaction_rid = NULL,
        body = NULL) {
      url_path <- sprintf("/%s/files:upload", dataset_rid)
      header_params <- c("Content-Type" = "application/octet-stream")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["filePath"] <- file_path
      query_params["branchId"] <- branch_id
      query_params["transactionType"] <- transaction_type
      query_params["transactionRid"] <- transaction_rid

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        query_params = query_params,
        header_params = header_params,
        body = body)

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    read_file = function(
        dataset_rid,
        file_path,
        branch_id = NULL,
        start_transaction_rid = NULL,
        end_transaction_rid = NULL) {
      url_path <- sprintf("/%s/files/%s/content", dataset_rid, utils::URLencode(file_path, reserved = TRUE))
      header_params <- c("Content-Type" = "application/json")
      query_params <- list(
        "preview" = TRUE
      )
      query_params["branchId"] <- branch_id
      query_params["startTransactionRid"] <- start_transaction_rid
      query_params["endTransactionRid"] <- end_transaction_rid

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "GET",
        header_params = header_params,
        query_params = query_params)

      self$api_client$stop_for_status(response)
    },
    # Foundry Data-Sidecar
    foundry_data_sidecar_download_files = function(alias, file_paths) {
      url_path <- sprintf("/%s/files/download", alias)
      header_params <- c("Content-Type" = "application/json")
      body <- jsonlite::toJSON(list(files = file_paths))
      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        header_params = header_params,
        body = body)

      self$api_client$stop_for_status(response)
      httr::content(response, as = "parsed")
    },
    foundry_data_sidecar_write_file = function(alias, file_path, upload_id, file_count, body = NULL) {
      url_path <- sprintf("/%s/files/%s/content", alias, utils::URLencode(file_path, reserved = TRUE))
      header_params <- c("Content-Type" = "application/octet-stream",
                         "X-Upload-Id" = upload_id,
                         "X-File-Count" = as.character(file_count))

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "PUT",
        header_params = header_params,
        body = body)

      self$api_client$stop_for_status(response)
    },
    foundry_data_sidecar_write_table = function(alias, body = NULL) {
      url_path <- sprintf("/%s/writeTable", alias)
      header_params <- c("Content-Type" = "application/octet-stream")

      response <- self$api_client$call_api(
        url = paste0(self$api_client$base_path, url_path),
        method = "POST",
        header_params = header_params,
        body = body)

      self$api_client$stop_for_status(response)
    }
  ),
)

#' @keywords internal
get_datasets_client <- function() {
  hostname <- get_hostname()
  context_path <- get_config("datasets_context_path", "/api/v1/datasets")
  base_path <- paste0("https://", hostname, context_path)
  DatasetsApiService$new(
    base_path = base_path,
    auth_token = get_config("token"))
}
