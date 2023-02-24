
test_that("default datasets client has expected path", {
  withr::with_envvar(
    list(
      FOUNDRY_HOSTNAME = "example.com",
      FOUNDRY_TOKEN = "token"), {
    datasets_client <- get_datasets_client()
    expect_equal(datasets_client$api_client$base_path, "https://example.com/api/v1/datasets")
  })
})

test_that("datasets client path can be overriden", {
  withr::with_envvar(
    list(
      FOUNDRY_HOSTNAME = "localhost:8080",
      FOUNDRY_DATASETS_CONTEXT_PATH = "/internal-api",
      FOUNDRY_TOKEN = "token"), {
    datasets_client <- get_datasets_client()
    expect_equal(datasets_client$api_client$base_path, "https://localhost:8080/internal-api")
  })
})
