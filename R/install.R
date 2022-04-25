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
pypalantir <- NULL

#' Installs pypalantir
#'
#' Installs the palantir python package and required dependencies. If Miniconda is not
#' installed yet, you will be prompted to install it first. We recommend restarting the
#' R session after calling this function.
#'
#' @param version Palantir Python SDK version (https://github.com/palantir/palantir-python-sdk)
#' @param python_version Version of python
#' @param envname Conda environment name
#' @param restart_session Whether the R session should be restarted to ensure the python package is updated
#' @param recreate_environment Whether to recreate the conda environment from scratch
#' @export
install_palantir <- function(
  version = "0.7.0",
  python_version = "3.9",
  envname = "r-reticulate",
  restart_session = TRUE,
  recreate_environment = FALSE
) {
  packages <- c(paste0("palantir-sdk==", version), "numpy", "pandas", "pyarrow")

  # check for anaconda installation
  if (!reticulate::py_available(initialize = TRUE)) {
    stop("Python is not installed or not in system path.")
  }

  if (envname %in% reticulate::conda_list()$name && recreate_environment) {
    cat("Recreating the conda environment...")
    reticulate::conda_remove(envname)
  }

  # install packages
  reticulate::py_install(
    packages = packages,
    envname = envname,
    method = "conda",
    conda = "auto",
    python_version = python_version,
    pip = TRUE)

  if (restart_session) {
    if (requireNamespace("rstudioapi", quietly = TRUE) &&
        rstudioapi::isAvailable() &&
        rstudioapi::hasFun("restartSession")) {
      rstudioapi::restartSession()
      reticulate::use_condaenv(envname)
    } else {
      warning(paste0("Please run `reticulate::use_condaenv(\"", envname, "\")`."))
    }
  }

  cat("\nInstallation complete.\n\n")

  invisible(NULL)
}

.onLoad <- function(libname, pkgname) { # nolint
  pypalantir <<- reticulate::import("palantir", convert = FALSE, delay_load = list(
    on_load = function() {
      version <- toString(utils::packageVersion("palantir"))
      pypalantir$core$rpc$USER_AGENT$append(reticulate::tuple("palantir-r-sdk", version))
    },

    on_error = function(error) {
      stop(paste("Use palantir::install_palantir() to install palantir", error$message))
    }
  ))
  invisible(NULL)
}
