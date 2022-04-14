DATASET <- "/path/to/dataset" # nolint

mock_file <- function(path) {
  reticulate::r_to_py(list(path = path))
}

mocked_download_file <- function(dataset, logical_path, target, ...) {
  testthat::expect_equal(dataset, DATASET)
  file.create(target)
}

test_that("download_files works as expected", {
  mocked_list_files <- function(dataset, path) {
    expect_equal(dataset, DATASET)
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
          download_files(DATASET, dir)
          expect_equal(list.files(dir), c("file1.txt", "file2.txt", "folder"))
          expect_equal(list.files(file.path(dir, "folder")), c("file3.txt", "file4.txt"))
        }
      )
    }
  )
})

test_that("download_files can download a subfolder of a dataset", {
  PATH <- "path/to/subfolder" # nolint

  mocked_list_files <- function(dataset, path) {
    expect_equal(dataset, DATASET)
    expect_equal(path, PATH)
    list(
      mock_file("path/to/subfolder/file1.txt"),
      mock_file("path/to/subfolder/file2.txt"),
      mock_file("path/to/subfolder/folder/file3.txt"),
      mock_file("path/to/subfolder/folder/file4.txt"))
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          download_files(DATASET, dir, path = PATH)
          expect_equal(list.files(dir), c("file1.txt", "file2.txt", "folder"))
          expect_equal(list.files(file.path(dir, "folder")), c("file3.txt", "file4.txt"))
        }
      )
    }
  )
})

test_that("download_files throws an error when dataset is empty", {
  mocked_list_files <- function(dataset, path) {
    expect_equal(dataset, DATASET)
    list()
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          expect_error(download_files(DATASET, dir), "No files found in the dataset.")
        }
      )
    }
  )
})

test_that("download_files throws an error when target directory is not empty", {
  mocked_list_files <- function(dataset, path) {
    expect_equal(dataset, DATASET)
    list(mock_file("file1.txt"), mock_file("file2.txt"))
  }

  withr::with_tempfile(
    "dir", {
      with_mock(
        download_file = mocked_download_file,
        list_files = mocked_list_files,
        {
          download_files(DATASET, dir)
          expect_equal(list.files(dir), c("file1.txt", "file2.txt"))
          expect_error(download_files(DATASET, dir), "Target directory is not empty, files will not be downloaded.")
        }
      )
    }
  )
})
