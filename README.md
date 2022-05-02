# Palantir R SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)
[![Autorelease](https://img.shields.io/badge/Perform%20an-Autorelease-success.svg)](https://autorelease.general.dmz.palantir.tech/palantir/palantir-r-sdk)

This SDK is incubating and subject to change.

## Setup

Install the library from Github:
```R
install.packages("remotes")
remotes::install_github("https://github.com/palantir/palantir-r-sdk", ref = "0.2.0")
```

This library relies on [palantir-python-sdk](https://github.com/palantir/palantir-python-sdk), so Python must be installed on the system.
The following command can be used to install the Python SDK and required dependencies:
```
palantir::install_palantir()
```

Configuration for hostname and an authentication token are provided by environment variables (`PALANTIR_HOSTNAME`, `PALANTIR_TOKEN`)

* `PALANTIR_HOSTNAME` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `PALANTIR_TOKEN` is a token acquired from the `Tokens` section of Foundry Settings 
 
Authentication tokens serve as a private password and allows a connection to Foundry data. Keep your token secret and do not share it with anyone else. Do not add a token to a source controlled or shared file.

## Examples

### Read a tabular Foundry dataset into a data.frame

```R
library(palantir)
df <- read_table("/Path/to/dataset")
```

### Download raw files from a Dataset

```R
download_files("/Path/to/dataset", file.path("~", "Downloads", "example"))
```

### Write a data.frame to Foundry

```R
write_table(df, "/Path/to/dataset")
```

### Upload a local file or folder to a Dataset

```R
upload_file(file.path("~", "Downloads", "example"), "/Path/to/dataset")
```

## Troubleshooting
You may run into the following error, in particular after restarting your session:
```
 Error in on_error(result) : 
  Use palantir::install_palantir() to install palantir
  ModuleNotFoundError: No module named 'palantir'
```
This indicates `reticulate` is not set up to use the right version of python.
You can set it with the environment variable `RETICULATE_PYTHON`.
By default, `palantir::install_palantir()` will install the python module in the default conda environment
so you can add the following to your `.Rprofile`:
```
Sys.setenv(RETICULATE_PYTHON = reticulate::conda_python())
```
