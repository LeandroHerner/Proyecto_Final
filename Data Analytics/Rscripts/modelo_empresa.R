library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)

# En este script vamos a modelar la empresa de taxi, en un principio a como es
# previo a la contratacion de nuestra consultora, es decir es una empresa 
# promedio.

# Suposiciones: La empresa tiene 50 taxis comunes, ejecuta turnos de 8hs
N_taxi <- 50
shift_hrs <- 8

setwd('/home/federico/Documents/soyhenry/PFb/Datasets/')
# --------- Modelando empresa inicial ------------------------------------------
# tomamos la informacion calculada en "autonomia.R" de la cantidad de viajes que
# puede realizar un taxi por borough
trip_duration <- read.csv('./trip_dist_dur_Borough/duration/trip_duration.csv')

trip_duration %>% 
  group_by(PUBorough) %>% 
  summarise(periodo = period, cant_viajes = shift_hrs * 60 / (q3 * 2)) %>% 
  filter(PUBorough != 'EWR') %>% 
  mutate(periodo = ym(periodo)) %>% 
  ggplot() + geom_line(aes(x = periodo, y = cant_viajes, color = PUBorough)) + lims(x = c(ymd("2023-01-01"), ymd("2023-11-01")))

# saco el promedio del ultimo de viajes que un taxi puede realizar en un turno de 8hs
# suponiendo que la empresa es promedio --> toma viajes de acuerdo a la estadistica
# de la ciudad
emp_trip_numb_inic <- trip_duration %>% 
  group_by(PUBorough) %>% 
  summarize(trip_numbers_avg = round(mean(shift_hrs * 60 / (q3 *2)), 2), 
            trip_totalnumbers_avg = round(mean(trip_numbers), 2)) %>% 
  filter(PUBorough == "City") %>% select(1:2)

# obteniendo un promedio de 13.22 viajes por turno de taxi en toda la ciudad 
# o 16 viajes si el turno es de 10hs
# lo que hacen un total de 13 viajes/dia * 30 dias/mes = 390 viajes/mes por taxi
# y 390 viajes/mes taxi * N taxis = 19500 viajes/mes para la empresa
# o 39000 viajes si consideramos que cada taxi se usan en 2 turnos
# o 480 viajes por mes por taxi (10hs shift), 24000 viajes por mes flota (10hs shift)
n_trips <- floor(emp_trip_numb_inic$trip_numbers_avg[1])

temp <- dataset_sample %>% filter(as.Date(PUtime) >= ymd("2022-01-01")) %>% group_by(as.Date(PUtime)) %>% sample_n(3)

# ahora vamos a capturar 390 viajes random de cada dia en los parquets
regions <- read.csv('./taxi_zone_lookup.csv')
regions <- regions %>% select(LocationID, Borough)

# funcion que levanta un parquet y lo procesa para que hacer left join con borough de partida y de llegada
# para obtener la cantidad de viajes por borough de partida y por borough de llegada
func_emp_model_inic <- function(x) {
  fileinputpath <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/yellow_tripdata_', x, '.parquet')
  
  trips <- read_parquet(fileinputpath)
  
  trips_reg <- trips %>% 
    filter(fare_amount > 0 & payment_type != 4 & trip_distance < 80) %>% 
    select(2,3,5,8:19)
  
  trips_reg <- trips_reg %>% rename_at('tpep_pickup_datetime', ~'PUtime')
  trips_reg <- trips_reg %>% rename_at('tpep_dropoff_datetime', ~'DOtime')
  
  trips_reg <- trips_reg %>% sample_n(n_trips * N_taxi * 30)
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "PULocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'PUBorough')
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "DOLocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'DOBorough')
  
  trips_reg <- trips_reg %>% select(-c("DOLocationID", "PULocationID"))
  
  #trips_reg_month_sum <- trips_reg %>% group_by(PUBorough, DOBorough) %>% summarize(count = n())
  
  trips_reg <- trips_reg %>% filter(PUBorough != 'Unknown' & DOBorough != 'Unknown')
  #trips_reg_month_sum <- trips_reg_month_sum %>% mutate(period = x)
  
  filename <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/empresa_inic/empresa_inic_', x, '.csv')
  print(filename)
  write.csv(trips_reg, file = filename)
}


filelist <- append(paste0('2021-0', 1:9), paste0('2021-1', 0:2))

lapply(filelist, func_emp_model_inic)

# ahora concateno todos los archivos mensuales en uno solo
files <- list.files('./empresa_inic/', pattern = "*.csv", full.names = TRUE)

dataset_sample <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",") %>% select(-1)
}))

# ahora le calculamos las metricas pertinentes
dataset_sample$PUtime <- as.POSIXct(dataset_sample$PUtime, format = "%Y-%m-%d %H:%M:%S")  # se necesita pasarlo asi para que despues haga la resta bien
dataset_sample$DOtime <- as.POSIXct(dataset_sample$DOtime, format = "%Y-%m-%d %H:%M:%S")

dataset_sample$trip_duration <- round(as.numeric(dataset_sample$DOtime - dataset_sample$PUtime) / 60, 2)
# dataset_sample$trip_duration <- round(dataset_sample$trip_duration, 2)

dataset_sample$fare_per_mile <- round(dataset_sample$fare_amount / dataset_sample$trip_distance, 2)
dataset_sample$fare_per_minute <- round(dataset_sample$fare_amount / dataset_sample$trip_duration, 2)

write.csv(dataset_sample, 'c:/Users/Intel/Documents/Cursos/Henry/PF/empresa_inic/empresa_inicial.csv', row.names = FALSE)


# ----------- modelo teniendo en cuenta leasing ---------------------------

setwd('/home/federico/Documents/soyhenry/PFb/')

N_taxi <- 50
taxi_leasing_shift <- 100
shift_day <- 1

empresa_inic <- read.csv('./Datasets/empresa_inic/empresa_inicial.csv')
head(empresa_inic)



reg_taxi_fuel_month <- 2569.20 / 12 # costo combustible por mes estimado para taxi comun (chevrolet malibu aaa)
ev_taxi_fuel_month <- 1244 / 12 # costo electricidad por mes estimado para taxi ev (Tesla Y LR)
reg_taxi_operative_cost_month <- (25000 - 8500) / 12 # costo de operacion taxi comun (investopedia), resta 8500 que son los $26k que costaria cambiarlo cada 3 años

taxi_operative_cost_notfuel_month <- reg_taxi_operative_cost_month - reg_taxi_fuel_month # costo de operacion de 1 taxi durante 1 mes sin contar combustible
taxiEV_operative_cost_notfuel_month <- taxi_operative_cost_notfuel_month - (3771-2676.2) / 12 # al costo de operacion de 1 taxi promedio por mes le descuento la diferencia de mantenimiento entre uno normal con uno EV

fleet_operative_cost_notfuel_month <- taxi_operative_cost_notfuel_month * N_taxi # costo de operacion de flota de taxis durante 1 mes sin contar combustible

empresa_inic %>% mutate(periodo = format(as.Date(empresa_inic$PUtime), "%Y-%m")) %>%
  group_by(periodo) %>%
  summarize(income = sum(fare_amount)) %>%
  filter(periodo > "2020-12") %>%
  mutate(periodo = ym(periodo)) %>%
  ggplot() + 
  geom_line(aes(x = periodo, y = income, color = "driver"), size = 1) + 
  geom_line(aes(x = periodo, y = income / 3 + taxi_leasing_shift * shift_day * 30 * N_taxi, color = "company"), size = 1) + 
  geom_line(aes(x = periodo, y = (income / 3 + taxi_leasing_shift * shift_day * 30) - taxi_operative_cost_notfuel_month - reg_taxi_fuel_month, color = "profit"), size = 1) +
  scale_color_manual(name = "income", breaks = c("driver", "company", "profit"), values = c("driver" = "darkblue", "company" = "red", "profit" = "orange")) +
  scale_x_date(date_labels = "%Y-%m")



# --------------------- prediccion de demanda ----------------------------------
# la demanda futura la modelaremos como una extension de la actual de la empresa
# esto supone un escenario pesimista, donde la empresa no mejore su performance

empresa_inic %>% mutate(periodo = format(as.Date(empresa_inic$PUtime), "%Y-%m")) %>%
  group_by(periodo) %>%
  summarize(cant_viajes = n(), fare_median = median(fare_amount), total_fare = sum(fare_amount)) %>%
  filter(periodo > "2020-12") %>%
  mutate(periodo = ym(periodo)) %>% 
  ggplot() + geom_line(aes(x = periodo, y = total_fare), size = 1) + 
  scale_x_date(date_breaks = "6 months", date_labels = "%Y-%m") + 
  ylab("cantidad de viajes realizados")

pred_empresa2 <- empresa_inic %>% 
  mutate(periodo = format(as.Date(empresa_inic$PUtime), "%Y-%m")) %>%
  group_by(periodo) %>%
  summarize(cant_viajes = n(), 
            fare_median = median(fare_amount), 
            total_fare = sum(fare_amount), 
            total_distance = sum(trip_distance)) %>%
  filter(periodo > "2020-12")
  

temp <- pred_empresa2 %>% 
  filter(periodo >= "2023-04-01")
temp$periodo <- ym(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 15000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.5)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 100)
temp$total_distance <- jitter(temp$total_distance, amount = 2000)

pred_empresa2$periodo <- ym(pred_empresa2$periodo)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 10000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.4)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 80)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 8000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.4)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 80)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 7000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 70)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 7000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 70)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 7000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 60)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 6000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 60)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 6000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 60)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 6000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.3)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 60)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

temp$periodo <- ymd(temp$periodo) %m+% months(7)
temp$total_fare <- jitter(temp$total_fare, amount = 5000)
temp$fare_median <- jitter(temp$fare_median, amount = 0.2)
temp$cant_viajes <- jitter(temp$cant_viajes, amount = 60)
temp$total_distance <- jitter(temp$total_distance, amount = 1000)
pred_empresa2 <- rbind(pred_empresa2, temp)

pred_empresa %>% ggplot() + 
  geom_line(aes(x = periodo, y = total_fare, color = "driver"), size = 1) + 
  geom_line(aes(x = periodo, y = total_fare / 3 + taxi_leasing_shift * shift_day * 30 * N_taxi, color = "company"), size = 1) + 
  geom_line(aes(x = periodo, y = (total_fare / 3 + taxi_leasing_shift * shift_day * 30) - taxi_operative_cost_notfuel_month - reg_taxi_fuel_month, color = "profit"), size = 1) +
  scale_color_manual(name = "income", breaks = c("driver", "company", "profit"), values = c("driver" = "darkblue", "company" = "red", "profit" = "orange")) +
  scale_x_date(date_labels = "%Y-%m")

pred_empresa2 <- left_join(pred_empresa2, taxi_type_df, by = "periodo")
pred_empresa2 <- pred_empresa2 %>% select(-6)
pred_empresa2$leasing_income <- taxi_leasing_shift * 30 * (pred_empresa2$cant_std + pred_empresa2$cant_EV)
pred_empresa2 <- pred_empresa2 %>% mutate(profit = total_fare / 3 + leasing_income - operative_cost)
pred_empresa2$std_distance <- (pred_empresa2$cant_std / N_taxi) * pred_empresa2$total_distance
pred_empresa2$EV_distance <- (pred_empresa2$cant_EV / N_taxi) * pred_empresa2$total_distance

pred_empresa %>% ggplot() + 
  geom_line(aes(x = periodo, y = total_fare, color = "driver"), size = 1) + 
  geom_line(aes(x = periodo, y = total_fare / 3 + taxi_leasing_shift * shift_day * 30 * N_taxi, color = "company"), size = 1) + 
  geom_line(aes(x = periodo, y = profit, color = "profit"), size = 1) +
  scale_color_manual(name = "income", breaks = c("driver", "company", "profit"), values = c("driver" = "darkblue", "company" = "red", "profit" = "orange")) +
  scale_x_date(date_labels = "%Y-%m")

# grafico de distancia recorrida por tipo de vehiculo
pred_empresa2 %>% ggplot() + 
  geom_line(aes(periodo, std_distance, colour = "standard"), size = 1) + 
  geom_line(aes(periodo, EV_distance, colour = "EV"), size = 1) +
  scale_color_manual(name = "tipo", values = c("standard" = "darkblue", "EV" = "green")) +
  ylab('distancia recorrida (millas)')

# -------------------- emision ---------------------------------------------
reg_taxi_greenhouse_emission <- 0.342 # chevrolet malibu greenhouse emission (kg/mile)
empresa_inic %>% mutate(periodo = format(as.Date(empresa_inic$PUtime), "%Y-%m")) %>%
  group_by(periodo) %>%
  summarize(total_distance = sum(trip_distance), 
            total_emission = total_distance * reg_taxi_greenhouse_emission) %>%
  filter(periodo > "2020-12") %>%
  mutate(periodo = ym(periodo)) %>%
  ggplot() + 
  geom_line(aes(x = periodo, y = total_emission), size = 1) + 
  scale_x_date(date_labels = "%Y-%m") + 
  ylab('Emision total (kg)')

emission_empresa <- empresa_inic %>% 
  mutate(periodo = format(as.Date(empresa_inic$PUtime), "%Y-%m")) %>%
  group_by(periodo) %>%
  summarize(total_distance = sum(trip_distance), 
            total_emission = total_distance * reg_taxi_greenhouse_emission) %>%
  filter(periodo > "2020-12") %>%
  mutate(periodo = ym(periodo))

temp <- emission_empresa %>% 
  filter(periodo >= ymd("2021-12-01"))
temp$periodo <- temp$periodo %m+% years(2)
temp$total_emission <- jitter(temp$total_emission, amount = 500)


emission_empresa <- rbind(emission_empresa, temp)
temp$periodo <- temp$periodo %m+% years(2)
temp$total_emission <- jitter(temp$total_emission, amount = 500)
emission_empresa <- rbind(emission_empresa, temp)
temp$periodo <- temp$periodo %m+% years(2)
temp$total_emission <- jitter(temp$total_emission, amount = 500)
emission_empresa <- rbind(emission_empresa, temp)

emission_empresa$total_emission[emission_empresa$periodo >= ymd("2025-01-01") & 
                                  emission_empresa$periodo < ymd("2026-01-01")] <- 
  emission_empresa$total_emission[emission_empresa$periodo >= ymd("2025-01-01") & 
                                    emission_empresa$periodo < ymd("2026-01-01")] * 4 /5

emission_empresa$total_emission[emission_empresa$periodo >= ymd("2026-01-01") & 
                                  emission_empresa$periodo < ymd("2027-01-01")] <- 
  emission_empresa$total_emission[emission_empresa$periodo >= ymd("2026-01-01") & 
                                    emission_empresa$periodo < ymd("2027-01-01")] * 3 /5

emission_empresa$total_emission[emission_empresa$periodo >= ymd("2027-01-01") & 
                                  emission_empresa$periodo < ymd("2028-01-01")] <- 
  emission_empresa$total_emission[emission_empresa$periodo >= ymd("2027-01-01") & 
                                    emission_empresa$periodo < ymd("2028-01-01")] * 2 /5

emission_empresa$total_emission[emission_empresa$periodo >= ymd("2028-01-01") & 
                                  emission_empresa$periodo < ymd("2029-01-01")] <- 
  emission_empresa$total_emission[emission_empresa$periodo >= ymd("2028-01-01") & 
                                    emission_empresa$periodo < ymd("2029-01-01")] * 1 /5

emission_empresa$total_emission[emission_empresa$periodo >= ymd("2029-01-01")] <- 
  emission_empresa$total_emission[emission_empresa$periodo >= ymd("2029-01-01")] * 0 /5


emission_empresa %>% ggplot() + 
  geom_line(aes(x = periodo, y = total_emission), size = 1) + 
  scale_x_date(date_breaks = "1 years", date_labels = "%Y") + 
  ylab('Emision total (kg)')


# ahora lo rehago con pred_empresa2 donde ya tengo calculada la evolucion de distancias y la incorporacion de autos
pred_empresa2$emision <- reg_taxi_greenhouse_emission * pred_empresa2$std_distance

write.csv(pred_empresa2, './Datasets/pred_empresa.csv', row.names = FALSE)
