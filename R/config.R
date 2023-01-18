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
CONFIG_DIR <- file.path(path.expand("~"), ".foundry")

#' @keywords internal
ALIASES_FILE <- "aliases.yml"

#' @keywords internal
get_config_file <- function(filename) {
  file.path(CONFIG_DIR, filename)
}

#' @keywords internal
load_config_file <- function(filename, force = FALSE) {
  file <- get_config_file(filename)
  if (!file.exists(file)) {
    return(NULL)
  }
  yaml::read_yaml(file)
}

#' @keywords internal
get_config_from_env <- function(name, default = NULL) {
  value <- Sys.getenv(name, unset = NA)
  if (is.na(value)) {
    return(default)
  }
  return(value)
}

#' @keywords internal
get_config_from_file <- function(name, filename = "config", default = NULL) {
  config <- load_config_file(filename)
  if (is.null(config)) {
    return(default)
  }
  value <- config[[name]]
  if (is.null(value)) {
    return(default)
  }
  return(value)
}

#' @keywords internal
get_config <- function(name, default = NULL) {
  # 1. Check for an environment variable
  environment_variable <- sprintf("FOUNDRY_%s", toupper(gsub("\\.", "_", name)))
  value <- get_config_from_env(environment_variable)
  if (!is.null(value)) {
    return(value)
  }
  # 2. Check for an option
  option_variable <- sprintf("foundry.%s", name)
  value <- getOption(option_variable)
  if (!is.null(value)) {
    return(value)
  }
  # 3. Check for a value in the global config file
  config_variable <- gsub("\\.", "_", name)
  value <- get_config_from_file(config_variable)
  if (!is.null(value)) {
    return(value)
  }
  # Return default value
  if (!missing(default)) {
    return(default)
  }
  stop(sprintf("The '%s' environment variable, the '%s' option, or the configuration file property '%s' must be set.",
               environment_variable, option_variable, config_variable))
}

#' @keywords internal
is_internal <- function() {
  !is.null(get_config("internal", NULL))
}

#' @keywords internal
get_alias <- function(alias) {
  if (is_internal()) {
    return(list(rid = alias))
  }
  value <- get_config_from_file(alias, filename = ALIASES_FILE, default = NULL)
  if (is.null(value)) {
    stop(sprintf("No alias '%s' is registered, please add it to %s", alias, get_config_file(ALIASES_FILE)))
  }
  value
}
