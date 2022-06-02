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
SqlQueryService <- R6::R6Class( # nolint
  "SqlQueryService",
  public = list(
    api_client = NULL,
    initialize = function(hostname, auth_token, user_agent, timeout) {
      self$api_client <- ApiClient$new(
        base_path = paste0("https://", hostname, "/foundry-sql-server/api/queries"),
        user_agent = user_agent,
        access_token = auth_token,
        # match java remoting
        # https://github.com/palantir/conjure-java-runtime/tree/3.12.0#quality-of-service-retry-failover-throttling
        retry_status_codes = c(308, 429, 503),
        timeout = timeout)
    },

    execute = function(query, dialect="ANSI", serialization_protocol="ARROW", fallback_branch_ids=vector()) {
      sql_execute_request <- list(
        query = jsonlite::unbox(query),
        dialect = jsonlite::unbox(dialect),
        serializationProtocol = jsonlite::unbox(serialization_protocol),
        fallbackBranchIds = fallback_branch_ids,
        timeout = jsonlite::unbox(1000 * self$api_client$timeout)
      )

      api_response <- self$execute_with_http_info(sql_execute_request)
      resp <- api_response$response
      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        api_response$content
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 599) {
        httr::stop_for_status(resp)
      }
    },
    execute_with_http_info = function(sql_execute_request) {
      query_params <- list()
      header_params <- c()

      if (missing(`sql_execute_request`)) {
        stop("Missing required parameter `sql_execute_request`.")
      }

      body <- jsonlite::toJSON(sql_execute_request)
      url_path <- "/execute"

      # OAuth token
      header_params["Authorization"] <- paste("Bearer", self$api_client$access_token, sep = " ")

      resp <- self$api_client$CallApi(url = paste0(self$api_client$base_path, url_path),
                                      method = "POST",
                                      query_params = query_params,
                                      header_params = header_params,
                                      body = body)

      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        deserialized_resp_obj <- tryCatch(
          jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8")),
          error = function(e) {
            stop("Failed to deserialize response")
          }
        )
        ApiResponse$new(deserialized_resp_obj, resp)
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 399) {
        ApiResponse$new(paste("Server returned ", httr::status_code(resp), " response status code."), resp)
      } else if (httr::status_code(resp) >= 400 && httr::status_code(resp) <= 499) {
        ApiResponse$new("API client error", resp)
      } else if (httr::status_code(resp) >= 500 && httr::status_code(resp) <= 599) {
        ApiResponse$new("API server error", resp)
      }
    },

    get_status = function(query_id) {
      api_response <- self$get_status_with_http_info(query_id)
      resp <- api_response$response
      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        api_response$content
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 599) {
        httr::stop_for_status(resp)
      }
    },
    get_status_with_http_info = function(query_id) {
      query_params <- list()
      header_params <- c()

      if (missing(`query_id`)) {
        stop("Missing required parameter `query_id`.")
      }

      body <- NULL
      url_path <- "/{queryId}/status"
      url_path <- gsub(paste0("\\{", "queryId", "\\}"), URLencode(as.character(`query_id`), reserved = TRUE), url_path)

      # OAuth token
      header_params["Authorization"] <- paste("Bearer", self$api_client$access_token, sep = " ")

      resp <- self$api_client$CallApi(url = paste0(self$api_client$base_path, url_path),
                                      method = "GET",
                                      query_params = query_params,
                                      header_params = header_params,
                                      body = body)

      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        deserialized_resp_obj <- tryCatch(
          jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8")),
          error = function(e) {
            stop("Failed to deserialize response")
          }
        )
        ApiResponse$new(deserialized_resp_obj, resp)
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 399) {
        ApiResponse$new(paste("Server returned ", httr::status_code(resp), " response status code."), resp)
      } else if (httr::status_code(resp) >= 400 && httr::status_code(resp) <= 499) {
        ApiResponse$new("API client error", resp)
      } else if (httr::status_code(resp) >= 500 && httr::status_code(resp) <= 599) {
        ApiResponse$new("API server error", resp)
      }
    },

    get_results = function(query_id) {
      api_response <- self$get_results_with_http_info(query_id)
      resp <- api_response$response
      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        api_response$content
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 599) {
        httr::stop_for_status(resp)
      }
    },
    get_results_with_http_info = function(query_id) {
      query_params <- list()
      header_params <- c()

      if (missing(`query_id`)) {
        stop("Missing required parameter `query_id`.")
      }

      body <- NULL
      url_path <- "/{queryId}/results"
      url_path <- gsub(paste0("\\{", "queryId", "\\}"), URLencode(as.character(`query_id`), reserved = TRUE), url_path)

      # OAuth token
      header_params["Authorization"] <- paste("Bearer", self$api_client$access_token, sep = " ")

      resp <- self$api_client$CallApi(url = paste0(self$api_client$base_path, url_path),
                                      method = "GET",
                                      query_params = query_params,
                                      header_params = header_params,
                                      body = body)

      if (httr::status_code(resp) >= 200 && httr::status_code(resp) <= 299) {
        stream <- arrow::BufferReader$create(httr::content(resp, "raw"))
        dtype <- rawToChar(stream$Read(1)$data())
        if (dtype != "A") {
          # N.B. we assume since the query is a simple select star that we are direct read eligible, if the stack is not
          # properly configured for direct read this will fail
          stop("Foundry is not configured for direct reads, please contact your Palantir administrator")
        }
        reader <- arrow::RecordBatchStreamReader$create(stream)
        tab <- tryCatch(
          reader$read_table(),
          error = function(e) {
            stop("Failed to read the arrow Table. You may retry as this could be due to a network issue.", e)
          }
        )
        ApiResponse$new(tab, resp)
      } else if (httr::status_code(resp) >= 300 && httr::status_code(resp) <= 399) {
        ApiResponse$new(paste("Server returned ", httr::status_code(resp), " response status code."), resp)
      } else if (httr::status_code(resp) >= 400 && httr::status_code(resp) <= 499) {
        ApiResponse$new("API client error", resp)
      } else if (httr::status_code(resp) >= 500 && httr::status_code(resp) <= 599) {
        ApiResponse$new("API server error", resp)
      }
    },

    read_dataset = function(locator) {
      if (missing(`locator`)) {
        stop("Missing required parameter `locator`.")
      }

      query <- paste0('SELECT * FROM "', locator$end_transaction_rid, '@', locator$branch_id, '"."', locator$rid, '"') # nolint
      response <- self$execute(query = query)
      query_status <- response$status

      while (!self$is_query_status_terminal(query_status)) {
        Sys.sleep(1)
        query_status <- self$get_status(query_id = response$queryId)$status
      }

      self$get_results(query_id = response$queryId)
    },
    is_query_status_terminal = function(query_status) {
      if (query_status$type == "canceled") {
        return(TRUE)
      } else if (query_status$type == "failed") {
        stop(
          "Read failed. Failure reason: ",
          query_status$failed$failure_reason,
          ". Message: ",
          query_status$failed$error_message,
          ".")
      } else if (query_status$type == "ready") {
        return(TRUE)
      } else if (query_status$type == "running") {
        return(FALSE)
      } else {
        stop("Unexpected query status", query_status)
      }
    }
  )
)
