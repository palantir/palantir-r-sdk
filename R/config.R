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
CONFIG_FILE <- file.path(path.expand("~"), ".foundry", "config")

#' @keywords internal
load_config_file <- function(force = FALSE) {
  if (!file.exists(CONFIG_FILE)) {
    return(NULL)
  }
  yaml::read_yaml(CONFIG_FILE)
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
get_config_from_options <- function(name, default = NULL) {
  getOption(name, default = default)
}

#' @keywords internal
get_config_from_file <- function(name, default = NULL) {
  config <- load_config_file()
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
  environment_variable <- sprintf("PALANTIR_%s", toupper(gsub("\\.", "_", name)))
  value <- get_config_from_env(environment_variable)
  if (!is.null(value)) {
    return(value)
  }
  # 2. Check for an option
  option_variable <- sprintf("palantir.%s", name)
  value <- get_config_from_options(option_variable)
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
get_alias <- function(alias) {
  aliases <- get_config_from_file("aliases", default = list())
  value <- aliases[[alias]]
  if (is.null(value)) {
    stop(sprintf("No alias '%s' is registered, please add it to %s", alias, CONFIG_FILE))
  }
  value
}
