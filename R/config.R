#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
DEFAULT_CONFIG_DIR <- file.path(path.expand("~"), ".foundry")

#' @keywords internal
ALIASES_FILE <- "aliases.yml"

#' @keywords internal
get_config_from_env <- function(name, default = NULL) {
  value <- Sys.getenv(name, unset = NA)
  if (is.na(value)) {
    return(default)
  }
  return(value)
}

#' Loads a config from an environment variable with format `FOUNDRY_CONFIG_KEY`
#' or from an option with format `foundry.config.key`.
#'
#' @keywords internal
get_config <- function(name, default = NULL) {
  # 1. Check for an option
  option_variable <- sprintf("foundry.%s", name)
  value <- getOption(option_variable)
  if (!is.null(value)) {
    return(value)
  }
  # 2. Check for an environment variable
  environment_variable <- sprintf("FOUNDRY_%s", toupper(gsub("\\.", "_", name)))
  value <- get_config_from_env(environment_variable)
  if (!is.null(value)) {
    return(value)
  }
  # Return default value
  if (!missing(default)) {
    return(default)
  }
  stop(sprintf("The '%s' option or the '%s' environment variable must be set.",
               option_variable, environment_variable))
}

#' @keywords internal
get_config_from_yaml_file <- function(name, filename, default = NULL) {
  config <- load_yaml_config_file(filename)
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
get_yaml_config_file <- function(filename) {
  file.path(get_config("config_dir", DEFAULT_CONFIG_DIR), filename)
}

#' @keywords internal
load_yaml_config_file <- function(filename) {
  file <- get_yaml_config_file(filename)
  if (!file.exists(file)) {
    return(NULL)
  }
  yaml::read_yaml(file)
}

#' @keywords internal
should_resolve_aliases <- function() {
  tolower(get_config("resolve_aliases", "true")) == "true"
}

#' @keywords internal
get_hostname <- function() {
  hostname <- get_config("hostname")
  if (grepl("/", hostname)) {
    stop(sprintf("Hostname should not contain slashes, found `%s`", hostname))
  }
  hostname
}

#' @keywords internal
get_runtime <- function() {
  tolower(get_config("runtime", "default"))
}

#' @keywords internal
get_alias <- function(alias) {
  if (!should_resolve_aliases()) {
    return(list(rid = alias))
  }
  value <- get_config_from_yaml_file(alias, filename = ALIASES_FILE, default = NULL)
  if (is.null(value)) {
    stop(sprintf("No alias '%s' is registered, please add it to %s", alias, get_yaml_config_file(ALIASES_FILE)))
  }
  value
}
