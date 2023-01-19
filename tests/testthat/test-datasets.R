ALIAS <- "my_dataset"
DATASET_RID <- "ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da"
TRANSACTION_RID <- "ri.foundry.main.transaction.00000029-5a26-c1ff-91c5-454d28c9af90"

DATASETS_CLIENT <- R6::R6Class(
  "DatasetsApiService",
  public = list(
    read_file = function(dataset_rid, file_path, ...) {
      list(content = charToRaw(sprintf("Content of %s", file_path)))
    }
  )
)

mock_get_client <- function() {
  DATASETS_CLIENT$new()
}

mock_get_alias <- function(alias) {
  testthat::expect_equal(alias, ALIAS)
  list(rid = DATASET_RID)
}

mock_file <- function(path) {
  list(
    path = path,
    updatedTime = "2022-02-10T12:22:35.532Z",
    transactionRid = TRANSACTION_RID,
    sizeBytes = 111102
  )
}

test_that("download_files supports downloads by name or file", {
  with_mock(
    "foundry:::get_datasets_client" = mock_get_client,
    "foundry:::get_alias" = mock_get_alias,
    {
      downloads_by_name <- datasets.download_files(ALIAS, c("file1.txt", "file2.txt"))
      expect_true(all(file.exists(downloads_by_name)))

      downloads_by_file <- datasets.download_files(ALIAS, list(mock_file("file1.txt"), mock_file("file2.txt")))
      expect_true(all(file.exists(downloads_by_file)))

      expect_equal(downloads_by_name, downloads_by_file)
    }
  )
})
