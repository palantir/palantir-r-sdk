# Palantir R SDK

This SDK is incubating and subject to change.

## Setup

Install the library from Github:
```R
install.packages("remotes")
remotes::install_github("https://github.com/palantir/palantir-r-sdk", ref = "0.1.1")
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
