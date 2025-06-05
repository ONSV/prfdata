library(lubridate)
library(dplyr)
library(arrow)
library(purrr)
library(lubridate)
library(stringr)

unzip_acidentes <- function(pattern) {
  if (file.exists("data-raw/datatran2007.csv")) {
    message("Files already unzipped")
  } else {
    message("Unzipping files")
    file_list <- list.files("data-raw", pattern = pattern)
    for (file in file_list) {
      unzip(paste0("data-raw/", file), exdir = "data-raw/", overwrite = TRUE)
    }
  }
}

read_arrow_acidentes <- function(pattern) {
  file_list <- list.files("data-raw", pattern = pattern, full.names = TRUE)
  arrow_list <- lapply(
    file_list, 
    read_csv2_arrow, 
    col_types = schema(
      id = utf8(),
      data_inversa = utf8(),
      dia_semana = utf8(),
      uf = utf8(),
      horario = utf8(),
      br = utf8(),
      km = utf8(),
      municipio = utf8(),
      causa_acidente = utf8(),
      tipo_acidente = utf8(),
      classificacao_acidente = utf8(),
      fase_dia = utf8(),
      sentido_via = utf8(),
      condicao_metereologica = utf8(),
      tipo_pista = utf8(),
      tracado_via = utf8(),
      uso_solo = utf8(),
      ano = int32(),
      pessoas = int32(),
      mortos = int32(),
      feridos_leves = int32(),
      feridos_graves = int32(),
      ilesos = int32(),
      ignorados = int32(),
      feridos = int32(),
      veiculos = int32(),
      regional = utf8(),
      delegacia = utf8(),
      uop = utf8(),
      latitude = utf8(),
      longitude = utf8()
    ),
    read_options = csv_read_options(encoding = "latin1")
  )
  arrow_list |> reduce(bind_rows)
}

unzip_acidentes("^datatran.*.zip")

sinistros <- read_arrow_acidentes("^datatran.*.csv") |> 
  as_arrow_table()

sinistros <- sinistros |> 
  mutate(
    data_inversa = case_when(
      str_ends(data_inversa, "2007|2008|2009|2010|2011|/16") ~ dmy(data_inversa),
      TRUE ~ ymd(data_inversa)
    ),
    dia_semana = wday(
      data_inversa,
      locale = "pt_BR.UTF-8",
      label = TRUE,
      abbr = FALSE
    ),
    uf = case_when(
      uf == "(null)" ~ NA_character_,
      TRUE ~ uf
    ),
    br = case_when(
      br == "(null)" ~ NA_character_,
      TRUE ~ br
    ),
    causa_acidente = tolower(causa_acidente),
    causa_acidente = case_when(
      causa_acidente == "(null)" ~ NA_character_,
      TRUE ~ causa_acidente
    ),
    tipo_acidente = case_when(
      tipo_acidente == "" ~ NA_character_,
      TRUE ~ tipo_acidente
    ),
    classificacao_acidente = case_when(
      classificacao_acidente == "" ~ NA_character_,
      classificacao_acidente == "(null)" ~ NA_character_,
      TRUE ~ classificacao_acidente
    ),
    fase_dia = tolower(fase_dia),
    fase_dia = case_when(
      fase_dia == "" ~ NA_character_,
      fase_dia == "(null)" ~ NA_character_,
      TRUE ~ fase_dia
    ),
    condicao_metereologica = tolower(condicao_metereologica),
    condicao_metereologica = case_when(
      condicao_metereologica == "" ~ NA_character_,
      condicao_metereologica == "(null)" ~ NA_character_,
      condicao_metereologica == "ignorado" ~ "ignorada",
      condicao_metereologica == "céu claro" ~ "ceu claro",
      TRUE ~ condicao_metereologica
    ),
    tipo_pista = case_when(
      tipo_pista == "(null)" ~ NA_character_,
      TRUE ~ tipo_pista
    ),
    tracado_via = case_when(
      tracado_via == "(null)" ~ NA_character_,
      tracado_via == "Não Informado" ~ NA_character_,
      TRUE ~ tracado_via
    ),
    uso_solo = case_when(
      uso_solo == "(null)" ~ NA_character_,
      uso_solo == "Sim" ~ "Urbano",
      uso_solo == "Não" ~ "Rural",
      TRUE ~ uso_solo
    ),
    ano = year(data_inversa),
    regional = case_when(
      regional == "(N/A)" ~ NA_character_,
      TRUE ~ regional
    ),
    delegacia = case_when(
      delegacia == "(N/A)" ~ NA_character_,
      TRUE ~ delegacia
    ),
    uop = case_when(
      uop == "(N/A)" ~ NA_character_,
      TRUE ~ uop
    )
  )

write_parquet(sinistros, "data/prf_sinistros.parquet")
zip("data/prf_sinistros.zip", "data/prf_sinistros.parquet")