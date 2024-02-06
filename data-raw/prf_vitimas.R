library(lubridate)
library(dplyr)
library(arrow)
library(purrr)
library(lubridate)
library(stringr)

unzip_acidentes <- function(pattern) {
  if (file.exists("data-raw/acidentes2007.csv")) {
    stop("Files already unzipped")
  } else {
    message("Unzipping files")
    file_list <- list.files("data-raw", pattern = pattern)
    for (file in file_list) {
      unzip(paste0("data-raw/", file), exdir = "data-raw/", overwrite = TRUE)
    }
  }
}

read_arrow_vitimas_2015 <- function(pattern) {
  file_list <- list.files("data-raw", pattern = pattern, full.names = TRUE)
  list2015 <- file_list[1:9]
  arrow_list <- lapply(
    list2015, 
    read_csv_arrow, 
    col_types = schema(
      id = utf8(),
      pesid = utf8(),
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
      condicao_metereologica = utf8(),
      tipo_pista = utf8(),
      tracado_via = utf8(),
      uso_solo = utf8(),
      id_veiculo = utf8(),
      tipo_veiculo = utf8(),
      marca = utf8(),
      ano_fabricacao_veiculo = utf8(),
      tipo_envolvido = utf8(),
      estado_fisico = utf8(),
      idade = utf8(),
      sexo = utf8(),
      nacionalidade = utf8(),
      naturalidade = utf8(),
      fase_dia = utf8(),
      sentido_via = utf8(),
      ilesos  = int32(),
      feridos_leves = int32(),
      feridos_graves = int32(),
      mortos = int32(),
      latitude = utf8(),
      longitude = utf8(),
      regional = utf8(),
      delegacia = utf8(),
      uop = utf8()
    ),
    read_options = csv_read_options(encoding = "latin1")
  )
  arrow_list |> reduce(bind_rows)
}

read_arrow_vitimas_2016 <- function(pattern) {
  file_list <- list.files("data-raw", pattern = pattern, full.names = TRUE)
  list2016 <- file_list[10:17]
  arrow_list <- lapply(
    list2016, 
    read_csv2_arrow, 
    col_types = schema(
      id = utf8(),
      pesid = utf8(),
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
      condicao_metereologica = utf8(),
      tipo_pista = utf8(),
      tracado_via = utf8(),
      uso_solo = utf8(),
      id_veiculo = utf8(),
      tipo_veiculo = utf8(),
      marca = utf8(),
      ano_fabricacao_veiculo = utf8(),
      tipo_envolvido = utf8(),
      estado_fisico = utf8(),
      idade = utf8(),
      sexo = utf8(),
      nacionalidade = utf8(),
      naturalidade = utf8(),
      fase_dia = utf8(),
      sentido_via = utf8(),
      ilesos  = int32(),
      feridos_leves = int32(),
      feridos_graves = int32(),
      mortos = int32(),
      latitude = utf8(),
      longitude = utf8(),
      regional = utf8(),
      delegacia = utf8(),
      uop = utf8()
    ),
    read_options = csv_read_options(encoding = "latin1")
  )
  arrow_list |> reduce(bind_rows)
}

unzip_acidentes("^acidentes.*.zip")

vitimas_2015 <- read_arrow_vitimas_2015("^acidentes.*.csv")

vitimas_2016 <- read_arrow_vitimas_2016("^acidentes.*.csv")

vitimas <- bind_rows(vitimas_2015, vitimas_2016) |> 
  as_arrow_table()

prf_vitimas <- vitimas |> 
  mutate(
    data_inversa = case_when(
      str_ends(data_inversa, "2007|2008|2009|2010|2011|2012|2013|2014|2015|/16") ~ dmy(data_inversa),
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
    km = sub("\\.", ",", km),
    municipio = case_when(
      municipio == "(null)" ~ NA_character_,
      TRUE ~ municipio
    ),
    causa_acidente = case_when(
      causa_acidente == "(null)" ~ NA_character_,
      TRUE ~ tolower(causa_acidente)
    ),
    tipo_acidente = case_when(
      tipo_acidente %in% c("", "(null)") ~ NA_character_,
      TRUE ~ tolower(tipo_acidente)
    ),
    classificacao_acidente = case_when(
      classificacao_acidente %in% c("", "(null)") ~ NA_character_,
      TRUE ~ sub("\\s+$", "", classificacao_acidente)
    ),
    fase_dia = tolower(fase_dia),
    fase_dia = case_when(
      fase_dia %in% c("", "(null)") ~ NA_character_,
      TRUE ~ fase_dia
    ),
    condicao_metereologica = tolower(condicao_metereologica),
    condicao_metereologica = case_when(
      condicao_metereologica %in% c("", "(null)") ~ NA_character_,
      condicao_metereologica == "ignorado" ~ "ignorada",
      TRUE ~ condicao_metereologica
    ),
    sentido_via = sub("\\s+$", "", sentido_via),
    tipo_pista = sub("\\s+$", "", tipo_pista),
    tipo_pista = case_when(
      tipo_pista == "(null)" ~ NA_character_,
      TRUE ~ tipo_pista
    ),
    tracado_via = sub("\\s+$", "", tracado_via),
    tracado_via = case_when(
      tracado_via == "(null)" ~ NA_character_,
      TRUE ~ tracado_via
    ),
    uso_solo = sub("\\s+$", "", uso_solo),
    uso_solo = case_when(
      uso_solo == "Sim" ~ "Urbano",
      uso_solo == "Não" ~ "Rural",
      uso_solo == "(null)" ~ NA_character_,
      TRUE ~ uso_solo
    ),
    id_veiculo = case_when(
      id_veiculo == "(null)" ~ NA_character_,
      TRUE ~ id_veiculo
    ),
    tipo_veiculo = tolower(tipo_veiculo),
    tipo_veiculo = case_when(
      tipo_veiculo %in% c("", "(null)", "não identificado", "não informado") ~ NA_character_,
      tipo_veiculo == "carro-de-mao" ~ "carro de mão",
      tipo_veiculo == "micro-ônibus" ~ "microônibus",
      tipo_veiculo == "motocicletas" ~ "motocicleta",
      tipo_veiculo %in% c("semi-reboque", "semireboque") ~ "semirreboque",
      tipo_veiculo == "trator de esteiras" ~ "trator de esteira",
      tipo_veiculo == "bonde / trem" ~ "trem-bonde",
      TRUE ~ tipo_veiculo
    ),
    marca = sub("\\s+$", "", marca),
    # ano_fabricacao_veiculo = as.numeric(ano_fabricacao_veiculo),
    # ano_fabricacao_veiculo = case_when(
    #   ano_fabricacao_veiculo > 2023 ~ NA_real_,
    #   TRUE ~ ano_fabricacao_veiculo
    # ),
    estado_fisico = sub("\\s+$", "", estado_fisico),
    estado_fisico = case_when(
      estado_fisico %in% c("", "(null)", "Ignorado", "Não Informado") ~ NA_character_,
      estado_fisico == "Lesões Graves" ~ "Ferido Grave",
      estado_fisico == "Lesões Leves" ~ "Ferido Leve",
      estado_fisico == "Morto" ~ "Óbito",
      TRUE ~ estado_fisico
    ),
    sexo = case_when(
      sexo %in% c("", "I", "Ignorado", "Inválido", "Não Informado") ~ NA_character_,
      sexo == "F" ~ "Feminino",
      sexo == "M" ~ "Masculino",
      TRUE ~ sexo
    ),
    nacionalidade = tolower(nacionalidade),
    naturalidade = tolower(naturalidade),
    ano = year(data_inversa)
  ) |> 
  select(
    -ilesos, -feridos_leves, -feridos_graves, -mortos,
    -latitude, -longitude, -regional, -delegacia, -uop
  )

prf_vitimas |> 
  group_by(ano) |> 
  write_dataset("data/prf_vitimas")
