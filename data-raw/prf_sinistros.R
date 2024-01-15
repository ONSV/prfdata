library(data.table)
library(lubridate)
library(magrittr)
library(duckdb)
library(arrow)

file_list <- paste0(
  "unzip -p data-raw/",
  list.files("data-raw", pattern = "^datatran")
)

accidents_list <- lapply(file_list, fread, encoding = "Latin-1")

fix_date <- function(accidents) {
  if (!is.Date(accidents$data_inversa)) {
    accidents[, data_inversa := dmy(data_inversa)]
  } else {
    accidents[, data_inversa := ymd(data_inversa)]
  }
}

lapply(accidents_list, fix_date)

prf_crashes <- rbindlist(accidents_list, fill = TRUE)

prf_crashes[, id := as.character(id)]

prf_crashes[
  , 
  dia_semana := wday(
    data_inversa,
    locale = "pt_BR.UTF-8",
    label = TRUE,
    abbr = FALSE
  )
]

prf_crashes[uf == "(null)", uf := NA]

prf_crashes[br == "(null)", br := NA]

prf_crashes[, causa_acidente := tolower(causa_acidente)]
prf_crashes[causa_acidente == "(null)", causa_acidente := NA]

prf_crashes[tipo_acidente == "", tipo_acidente := NA]

prf_crashes[
  classificacao_acidente %in% c("", "(null)"),
  classificacao_acidente := NA
]

prf_crashes[fase_dia %in% c("", "(null)"), fase_dia := NA][
  , fase_dia := tolower(fase_dia)]

prf_crashes %>% 
  .[
    condicao_metereologica %in% c("", "(null)"),
    condicao_metereologica := NA
  ] %>% 
  .[, condicao_metereologica := tolower(condicao_metereologica)] %>% 
  .[
    condicao_metereologica == "ignorado",
    condicao_metereologica := "ignorada"
  ] %>% 
  .[
    condicao_metereologica == "céu claro",
    condicao_metereologica := "ceu claro"
  ]

prf_crashes[tipo_pista == "(null)", tipo_pista := NA]

prf_crashes[tracado_via %in% c("(null)", "Não Informado"), tracado_via := NA]

prf_crashes[uso_solo == "(null)", uso_solo := NA]

prf_crashes[uso_solo == "Sim", uso_solo := "Urbano"]

prf_crashes[uso_solo == "Não", uso_solo := "Rural"]

prf_crashes[, ano := year(data_inversa)]

prf_crashes[regional == "N/A", regional := NA]

prf_crashes[delegacia == "N/A", delegacia := NA]

prf_crashes[uop == "N/A", uop := NA]

prf_sinistros <- prf_crashes

write_parquet(prf_sinistros, "data/prf_sinistros.parquet")
