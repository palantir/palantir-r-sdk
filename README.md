# Palantir R SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/licenses/Apache-2.0)
[![Autorelease](https://img.shields.io/badge/Perform%20an-Autorelease-success.svg)](https://autorelease.general.dmz.palantir.tech/palantir/palantir-r-sdk)

This SDK is incubating and subject to change.

## Setup

Install the library from Github:
```R
install.packages("remotes")
remotes::install_github("https://github.com/palantir/palantir-r-sdk", ref = "0.5.0")
```

Configuration for hostname and an authentication token can be provided by environment variables (`PALANTIR_HOSTNAME`, `PALANTIR_TOKEN`)

* `PALANTIR_HOSTNAME` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `PALANTIR_TOKEN` is a token acquired from the `Tokens` section of Foundry Settings 
 
Alternatively, you can set the following options:

* `palantir.hostname` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `palantir.token` is a token acquired from the `Tokens` section of Foundry Settings 
* `palantir.timeout` is the timeout of any HTTPS request made to Foundry

When present, environment variables take precedence over options.
 
Authentication tokens serve as a private password and allows a connection to Foundry data. Keep your token secret and do not share it with anyone else. Do not add a token to a source controlled or shared file.

## Examples

### Read a tabular Foundry dataset into a data.frame

```R
library(palantir)
df <- read_table("ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da")
```

### Download raw files from a Dataset

```R
download_files("ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da", file.path("~", "Downloads", "example"))
```

### Write a data.frame to Foundry

```R
write_table(df, "ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da")
```

### Upload a local file or folder to a Dataset

```R
upload_file(file.path("~", "Downloads", "example"), "ri.foundry.main.dataset.c26f11c8-cdb3-4f44-9f5d-9816ea1c82da")
```

