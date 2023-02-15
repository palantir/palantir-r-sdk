
test_that("arrow-to-bin roundtrip", {
  expected_table <- arrow::Table$create(mtcars)
  bytes <- arrow_to_bin(expected_table)
  actual_table <- bin_to_arrow(bytes)

  expect_equal(actual_table, expected_table)
})
