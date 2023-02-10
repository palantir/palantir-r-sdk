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

with_mocks <- function (expr) {
  withr::with_envvar(
    list(
      FOUNDRY_HOSTNAME = "example.com",
      FOUNDRY_TOKEN = "token"), {
        withr::with_options(
          list(foundry.config.dir = "config"), {
            httptest::with_mock_api({
              eval.parent(expr)
            })
          })
      })
}

with_mocks({
  test_that("datasets.list_files", {
    foundry_files <- datasets.list_files("my_input")
    expect_equal(length(foundry_files), 2)
    expect_equal(sapply(foundry_files, function(x) x$path), c("file.csv", "reports/file.txt"))
  })

  test_that("datasets.download_files", {
    local_files <- datasets.download_files("my_input", c("file.csv", "reports/file.txt"))
    expect_equal(length(local_files), 2)
    expect_true(file.exists(local_files["file.csv"]))
    expect_true(file.exists(local_files["reports/file.txt"]))
    local_file <- local_files["reports/file.txt"]
    expect_equal(readChar(local_file, file.info(local_file)$size), "file content")
  })

  test_that("datasets.upload_files", {
    uploaded_files <- datasets.upload_files(c("data", file.path("config", "aliases.yml")), "my_output")
    expect_equal(uploaded_files, list(
      list(file_name = "file.csv", file = "data/file.csv"),
      list(file_name = "reports/file.txt", file = "data/reports/file.txt"),
      list(file_name = "aliases.yml", file = "config/aliases.yml")))
  })

  test_that("datasets.upload_files throws if file not found", {
    expect_error({
      datasets.upload_files(c("path/to/missing/file.txt"), "my_output")
    }, "The following local files do not exist: path/to/missing/file.txt")
  })

  test_that("datasets.read_table", {
    df <- datasets.read_table("my_input")
    expect_is(df, "data.frame")
    expect_equal(nrow(df), 5)
    expect_equal(ncol(df), 6)
  })

  test_that("datasets.read_table as arrow", {
    df <- datasets.read_table("my_input", format = "arrow")
    expect_is(df, "Table")
    expect_equal(nrow(df), 5)
    expect_equal(ncol(df), 6)
  })

  test_that("datasets.list_files calls internal API when FOUNDRY_INTERNAL is set", {
    withr::with_envvar(
      list(FOUNDRY_INTERNAL_DATASETS_URL = "example.com/internal-api",
           FOUNDRY_INTERNAL = "true"), {
        foundry_files <- datasets.list_files("my_input")
        expect_equal(length(foundry_files), 1)
        expect_equal(sapply(foundry_files, function(x) x$path), c("internal/file.txt"))
    })
  })
})
