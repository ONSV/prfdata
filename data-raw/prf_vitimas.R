library(data.table)
library(lubridate)

file_list <- paste0(
  "unzip -p data-raw/",
  list.files("data-raw", pattern = "^acidentes")
)

victims_list <- lapply(file_list, fread, encoding = "Latin-1")

fix_date <- function(victims) {
  if (!is.Date(victims$data_inversa)) {
    victims[, data_inversa := dmy(data_inversa)]
  } else {
    victims[, data_inversa := ymd(data_inversa)]
  }
}

lapply(victims_list, fix_date)

prf_victims <- rbindlist(victims_list, fill = TRUE)

prf_victims[, id := as.character(id)]

prf_victims[, pesid := as.character(pesid)]

prf_victims[, dia_semana := wday(
  data_inversa,
  locale = "pt_BR.UTF-8",
  label = TRUE,
  abbr = FALSE
)]

prf_victims[uf == "(null)", uf := NA]

prf_victims[br == "(null)", br := NA]

prf_victims[km == "(null)", km := NA]

prf_victims[, km := sub("\\.", ",", km)]

prf_victims[municipio == "(null)", municipio := NA]

prf_victims[causa_acidente == "(null)", causa_acidente := NA]

prf_victims[, causa_acidente := tolower(causa_acidente)]

prf_victims[tipo_acidente %in% c("", "(null)"), tipo_acidente := NA]

prf_victims[, tipo_acidente := tolower(tipo_acidente)]

prf_victims[
  classificacao_acidente %in% c("", "(null)"),
  classificacao_acidente := NA
]

prf_victims[
  ,
  classificacao_acidente := sub("\\s+$", "", classificacao_acidente)
]

prf_victims[fase_dia %in% c("", "(null)"), fase_dia := NA]

prf_victims[, sentido_via := sub("\\s+$", "", sentido_via)]

prf_victims[
  condicao_metereologica %in% c("", "(null)"),
  condicao_metereologica := NA
]

prf_victims[, condicao_metereologica := tolower(condicao_metereologica)]

prf_victims[
  condicao_metereologica == "ignorado",
  condicao_metereologica := "ignorada"
]

prf_victims[, tipo_pista := sub("\\s+$", "", tipo_pista)]

prf_victims[tipo_pista == "(null)", tipo_pista := NA]

prf_victims[tracado_via == "(null)", tracado_via := NA]

prf_victims[, tracado_via := sub("\\s+$", "", tracado_via)]

prf_victims[, uso_solo := sub("\\s+$", "", uso_solo)]

prf_victims[uso_solo == "Sim", uso_solo := "Urbano"]

prf_victims[uso_solo == "Não", uso_solo := "Rural"]

prf_victims[uso_solo == "(null)", uso_solo := NA]

prf_victims[id_veiculo == "(null)", id_veiculo := NA]

prf_victims[, tipo_veiculo := tolower(tipo_veiculo)]

prf_victims[tipo_veiculo == "carro-de-mao", tipo_veiculo := "carro de mão"]

prf_victims[tipo_veiculo == "micro-ônibus", tipo_veiculo := "microônibus"]

prf_victims[tipo_veiculo == "motocicletas", tipo_veiculo := "motocicleta"]

prf_victims[
  tipo_veiculo %in% c("semi-reboque", "semireboque"),
  tipo_veiculo := "semirreboque"
]

prf_victims[
  tipo_veiculo == "trator de esteiras",
  tipo_veiculo := "trator de esteira"
]

prf_victims[
  tipo_veiculo %in% c("", "(null)", "não identificado", "não informado"),
  tipo_veiculo := NA
]

prf_victims[tipo_veiculo == "bonde / trem", tipo_veiculo := "trem-bonde"]

prf_victims[, marca := sub("\\s+$", "", marca)]

prf_victims[, ano_fabricacao_veiculo := as.numeric(ano_fabricacao_veiculo)]

prf_victims[ano_fabricacao_veiculo > 2023, ano_fabricacao_veiculo := NA]

prf_victims[, estado_fisico := sub("\\s+$", "", estado_fisico)]

prf_victims[
  estado_fisico %in% c("", "(null)", "Ignorado", "Não Informado"),
  estado_fisico := NA
]

prf_victims[estado_fisico == "Lesões Graves", estado_fisico := "Ferido Grave"]

prf_victims[estado_fisico == "Lesões Leves", estado_fisico := "Ferido Leve"]

prf_victims[estado_fisico == "Morto", estado_fisico := "Óbito"]

prf_victims[
  sexo %in% c("", "I", "Ignorado", "Inválido", "Não Informado"),
  sexo := NA
]

prf_victims[sexo == "F", sexo := "Feminino"]

prf_victims[sexo == "M", sexo := "Masculino"]

prf_victims[, nacionalidade := tolower(nacionalidade)]

prf_victims[, naturalidade := tolower(naturalidade)]

prf_victims[, ilesos := NULL]
prf_victims[, feridos_leves := NULL]
prf_victims[, feridos_graves := NULL]
prf_victims[, mortos := NULL]
prf_victims[, latitude := NULL]
prf_victims[, longitude := NULL]
prf_victims[, regional := NULL]
prf_victims[, delegacia := NULL]
prf_victims[, uop := NULL]

prf_vitimas <- prf_victims

write_parquet(prf_vitimas, "data/prf_vitimas.parquet")
