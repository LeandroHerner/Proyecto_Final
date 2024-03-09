library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)

# En este script vamos a modelar la incorporacion de EV en la flota de taxis
# resultando en un dataset que contenga la cantidad de vehiculos electricos, 
# vehiculos comunes, la cantidad de creditos tomados y la carga financiera de 
# los mismos entre 2024 y 2032 (mensual)

N_taxi <- 50
EV_value <- 50000

periodo_inicial <- as.Date("2024-01-01")
periodo_list <- seq(periodo_inicial, by = 'month', length = 12*8)

incorp_etapa <- c(rep(0, 12), rep(1, 12), rep(2, 12), rep(3, 12), rep(4, 12), rep(5, 36))

cant_EV <- incorp_etapa * (N_taxi / 5)

creditos_activos <- c(rep(0, 12), rep(1, 12), rep(2, 12), rep(2, 12), rep(2, 12), rep(2, 12), 
                      rep(1, 12), rep(0, 12))

taxi_type_df <- data_frame(periodo = periodo_list, 
                           cant_std = N_taxi - cant_EV, 
                           cant_EV = cant_EV, 
                           cant_creditos = creditos_activos,
                           monto_creditos_mensual = creditos_activos * EV_value * 1.1 * N_taxi / (24 * 5))

# ----------------------------------------------------------------------------
prediccion <- read.csv('./Datasets/predicciones.csv')

prediccion <- left_join(prediccion, regions, join_by(PULocationID == LocationID))
prediccion <- prediccion %>% select(Borough, mes, dia, hora, demand)
temp <- prediccion %>% group_by(across(all_of(c("Borough", "mes", "dia", "hora")))) %>%
  summarise(demanda = sum(demand))

# prediccion de demanda por hora para cada Borough
prediccion %>%  group_by(across(all_of(c("Borough", "hora")))) %>%
  summarize(demanda = sum(demand)) %>%
  ggplot(aes(hora, demanda)) + geom_col(width = 1) + facet_grid(~ Borough) +
  ggtitle('Demanda horaria por Borough de la empresa predecida por ML')

# demanda "real" de la empresa tomando random samples de los parquets
empresa_inic %>% mutate(hora = hour(PUtime)) %>% 
  group_by(across(all_of(c("PUBorough", "hora")))) %>% 
  summarize(demanda = n()) %>% 
  ggplot(aes(hora, demanda)) + geom_col(width = 1) + facet_grid(~ PUBorough) + 
  ggtitle('Demanda horaria por Borough real de la empresa')


