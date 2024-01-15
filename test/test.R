library(arrow)

temp <- tempfile(fileext = ".parquet")
download.file(
  url = "https://github.com/ONSV/prfdata/releases/download/v0.1.0/prf_sinistros.parquet",
  destfile = temp
)
prf_sinistros <- read_parquet(temp)

head(prf_sinistros)