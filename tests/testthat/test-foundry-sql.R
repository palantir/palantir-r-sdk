
DATASET_RID_SUCCESS <- "ri.foundry.main.dataset.12345678-1234-1234-1234-000000000000" # nolint
DATASET_RID_ERROR <- "ri.foundry.main.dataset.12345678-1234-1234-1234-000000000001" # nolint
BRANCH_ID <- "master" # nolint
END_TRANSACTION_RID <- "ri.foundry.main.transaction.12345678-1234-1234-1234-000000000001" # nolint

SQL_QUERY <- SqlQueryService$new( # nolint
  hostname = "example.com",
  auth_token = "token",
  user_agent = "palantir-r-sdk/test",
  timeout = 10)

withr::with_options(
  # Backward-compatibility with R 3.x
  list(stringsAsFactors = FALSE), {
    httptest::with_mock_api({
      test_that("Successful query returns an arrow Table", {
        # execute-d3baf7
        df <- data.frame(list("foo" = as.integer(c(1, 2, 3)), "bar" = as.character(c("a", "b", "c"))))
        dataset <- SQL_QUERY$read_dataset(list(
          rid = DATASET_RID_SUCCESS,
          branch_id = BRANCH_ID,
          end_transaction_rid = END_TRANSACTION_RID))
        expect_true(dplyr::all_equal(dataset$to_data_frame(), df))
    })
  })
})

httptest::with_mock_api({
  test_that("Failed query throws an error", {
    # execute-77966a
    expect_error(
      SQL_QUERY$read_dataset(list(
        rid = DATASET_RID_ERROR,
        branch_id = BRANCH_ID,
        end_transaction_rid = END_TRANSACTION_RID)),
      "Read failed. Failure reason: NOT_FOUND. Message: Dataset not found."
    )
  })
})
