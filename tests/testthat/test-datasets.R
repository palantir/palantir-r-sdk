DATASET_RID <- "ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da"
TRANSACTION_RID <- "ri.foundry.main.transaction.00000029-5a26-c1ff-91c5-454d28c9af90"

mock_file <- function(path) {
  list(
    path = path,
    updatedTime = "2022-02-10T12:22:35.532Z",
    transactionRid = TRANSACTION_RID,
    sizeBytes = 111102
  )
}

mocked_download_file <- function(dataset_rid, file_path, target, end_transaction_rid = NULL, ...) {
  testthat::expect_equal(dataset_rid, DATASET_RID)
  testthat::expect_equal(end_transaction_rid, TRANSACTION_RID)
  file.create(target)
}

test_that("download_files works as expected", {
  mocked_list_files <- function(dataset_rid, ...) {
    expect_equal(dataset_rid, DATASET_RID)
    list(
      mock_file("file1.txt"),
      mock_file("file2.txt"),
      mock_file("folder/file3.txt"),
      mock_file("folder/file4.txt"))
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          download_files(DATASET_RID, dir)
          expect_equal(list.files(dir), c("file1.txt", "file2.txt", "folder"))
          expect_equal(list.files(file.path(dir, "folder")), c("file3.txt", "file4.txt"))
        }
      )
    }
  )
})

test_that("download_files throws an error when dataset is empty", {
  mocked_list_files <- function(dataset_rid, ...) {
    expect_equal(dataset_rid, DATASET_RID)
    list()
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          expect_error(download_files(DATASET_RID, dir), "No files found in the dataset.")
        }
      )
    }
  )
})

test_that("download_files throws an error when target directory is not empty", {
  mocked_list_files <- function(dataset_rid, ...) {
    expect_equal(dataset_rid, DATASET_RID)
    list(mock_file("file1.txt"), mock_file("file2.txt"))
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          download_files(DATASET_RID, dir)
          expect_equal(list.files(dir), c("file1.txt", "file2.txt"))
          expect_error(download_files(DATASET, dir), "Target directory is not empty, files will not be downloaded.")
        }
      )
    }
  )
})
