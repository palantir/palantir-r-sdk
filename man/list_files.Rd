% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/datasets.R
\name{list_files}
\alias{list_files}
\title{Lists the files stored in a Foundry Dataset.}
\usage{
list_files(dataset, branch = NULL, transaction = NULL, path = NULL)
}
\arguments{
\item{dataset}{A Dataset or a character string representing the RID or dataset path.}

\item{branch}{The dataset branch.}

\item{transaction}{The dataset transaction.}

\item{path}{If present, only the file or the files with the specified prefix will be returned.}
}
\value{
A list of File objects representing the files in the dataset.
}
\description{
Lists the files stored in a Foundry Dataset.
}
