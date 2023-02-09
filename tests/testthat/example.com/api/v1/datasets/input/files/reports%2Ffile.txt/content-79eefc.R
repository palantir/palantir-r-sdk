# Adapted from code generated with httptest::start_capturing
structure(list(
  url = "https://example.palantircloud.com/api/v1/datasets/ri.foundry.main.dataset.00000000-0000-0000-0000-000000000000/files/file.txt/content?preview=TRUE",
  status_code = 200L,
  headers = structure(
    list(`content-length` = "12", `content-type` = "application/octet-stream"),
    class = c("insensitive", "list")),
  all_headers = list(list(
    status = 200L,
    version = "HTTP/2",
    headers = structure(
      list(`content-length` = "12", `content-type` = "application/octet-stream"),
      class = c("insensitive", "list")))),
  content = as.raw(c(0x66, 0x69, 0x6c, 0x65, 0x20, 0x63, 0x6f,
                     0x6e, 0x74, 0x65, 0x6e, 0x74))), class = "response")
