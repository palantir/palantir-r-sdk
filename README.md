# Palantir R SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://opensource.org/license/Apache-2-0/)
[![Autorelease](https://img.shields.io/badge/Perform%20an-Autorelease-success.svg)](https://autorelease.general.dmz.palantir.tech/palantir/palantir-r-sdk)

## Setup

Install the library from Github:
```R
install.packages("remotes")
remotes::install_github("https://github.com/palantir/palantir-r-sdk", ref = "0.12.0")
```

### Configuration

Configuration for hostname and an authentication token can be provided using options:

* `foundry.hostname` is the hostname of your instance e.g. `example.palantirfoundry.com`
* `foundry.token` is a token acquired from the `Tokens` section of Foundry Settings 

Alternatively, you can set the environment variables `FOUNDRY_HOSTNAME` and `FOUNDRY_TOKEN`.

Configuration will be loaded in the following order: options if present, otherwise environment variables.
 
Authentication tokens serve as a private password and allows a connection to Foundry data. Keep your token secret and do not share it with anyone else. Do not add a token to a source controlled or shared file.

#### Extra configuration options

* `foundry.config_dir` is the directory where configuration files are stored, defaults to `~/.foundry`
* `foundry.requests.timeout` is the timeout of HTTPS requests made to Foundry, defaults to 150s

### Aliases

To read datasets, aliases must first be registered. Create a YAML file called `~/.foundry/aliases.yml` and define an alias for every dataset you wish to read or write to.

```yaml
my_dataset:
    rid: ri.foundry.main.dataset.31388c03-2854-443e-b6cd-fe51c5908371
my_dataset_on_branch:
    rid: ri.foundry.main.dataset.5db9d133-6c87-4917-b11e-18b095ac4d30
    branch: develop
```

## Examples

### Read a tabular Foundry dataset into a data.frame or Apache Arrow Table

```R
library(foundry)
df <- datasets.read_table("my_dataset")
```

### Download raw files from a Dataset

```R
all_dataset_files <- datasets.list_files("my_dataset")
downloaded_files <- datasets.download_files("my_dataset", all_dataset_files)
```

### Write a data.frame or Apache Arrow Table to Foundry

```R
datasets.write_table(df, "my_dataset")
```

### Upload local files or folders to a Dataset

```R
datasets.upload_files(file.path("~", "Downloads", "example"), "my_dataset")
```

