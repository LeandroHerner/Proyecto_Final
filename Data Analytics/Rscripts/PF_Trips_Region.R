library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)

# ----------------- descarga de parquets --------------------------------------

periodos <- append(paste0("2017-0", 1:9),  paste0('2027-1', 0:2))
originurl <- paste0("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_", periodos, ".parquet")
destpath <- "c:/Users/Intel/Documents/Cursos/Henry/PF/"



lapply(originurl, function(x) {
  destino <- paste0(destpath, sub(".*trip-data/", "", x)) 
  download.file(x, destino, mode="wb")
  print(destino)
})


# ----------------- Analisis por regiones --------------------------------------
regions <- read.csv('c:/Users/Intel/Documents/Cursos/Henry/PF/taxi_zone_lookup.csv')
regions <- regions %>% select(LocationID, Borough)

# funcion que levanta un parquet y lo procesa para que hacer left join con borough de partida y de llegada
# para obtener la cantidad de viajes por borough de partida y por borough de llegada
func1 <- function(x) {
  fileinputpath <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/yellow_tripdata_', x, '.parquet')

  trips <- read_parquet(fileinputpath)

  trips_reg <- trips %>% select(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount)

  trips_reg <- merge(x = trips_reg, y = regions, by.x = "PULocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'PUBorough')

  trips_reg <- merge(x = trips_reg, y = regions, by.x = "DOLocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'DOBorough')

  trips_reg <- trips_reg %>% select(-c("DOLocationID", "PULocationID"))

  trips_reg_month_sum <- trips_reg %>% group_by(PUBorough, DOBorough) %>% summarize(count = n())

  trips_reg_month_sum <- trips_reg_month_sum %>% filter(PUBorough != 'Unknown' & DOBorough != 'Unknown')
  trips_reg_month_sum <- trips_reg_month_sum %>% mutate(period = x)

  filename <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/region_month_', x, '.csv')
  print(filename)
  write.csv(trips_reg_month_sum, file = filename)
}

filelist <- append(paste0('2023-0', 1:9), paste0('2023-1', 0:1))

lapply(filelist, func1)


# concatenar archivos
files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_minute_borough/', pattern = "*.csv", full.names = TRUE)

dataset <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))


# funcion que grafica heatmaps tomando todos los csv de un directorio
func_ggplot <- function(x) {
  regiones <- list("Bronx", "Brooklyn", "EWR", "Manhattan", "Queens", "Staten Island")
  month1 <- read.csv(x, header = TRUE, sep = ",")
  data <- expand.grid(X = regiones, Y = regiones)
  data <- month1 %>% select(-X)
  
  p <- ggplot(data, aes(PUBorough, DOBorough, fill = log(count))) + geom_tile() + scale_fill_gradient(low="white", high="blue") +
    ggtitle(paste0("Periodo: ", data$period[1])) + labs(x = "Pick Up Borough", y = "Drop Off Borough")
  
  ggsave(filename = paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/region_month_2022/', data$period[1], '.png'))
}

lapply(files, func_ggplot)
filelist


# funcion que levanta un parquet, asocia con boroughs de partida y llegada y obtiene el fare_per_mile y el fare_per_minute
output_path <- 'c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_mile-minute_borough/'

func_fare_borough_func <- function(x) {
  trips <- read_parquet(x)
  trips_reg <- trips %>% select(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount, trip_distance)
  trips_reg <- trips_reg %>% mutate(date = as.Date(tpep_pickup_datetime), 
                                    PU_hour = hour(tpep_pickup_datetime), 
                                    trip_duration = round((as.numeric(tpep_dropoff_datetime - tpep_pickup_datetime)) / 60, 2), 
                                    fare_per_mile = round(fare_amount / trip_distance, 2), 
                                    fare_per_minute = round(fare_amount / trip_duration, 2))
  trips_reg <- trips_reg %>% select(-tpep_pickup_datetime, -tpep_dropoff_datetime)
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "PULocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'PUBorough')
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "DOLocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'DOBorough')
  
  trips_reg <- trips_reg %>% select(-c("DOLocationID", "PULocationID")) %>%
    filter(trip_distance > 0 & fare_amount > 0)
  
  Temp = fivenum(trips_reg$fare_per_mile, na.rm = TRUE)
  period <- str_replace(str_replace(basename(x), ".parquet", ""), "yellow_tripdata_", "")
  results <- data.frame(Period = period, Minimum = Temp[2] - (1.5 * (Temp[4] - Temp[2])), 
                        FirstPerc = Temp[2], 
                        Median = Temp[3], 
                        ThirdPerc = Temp[4], 
                        Maximum = Temp[4] + 1.5 * (Temp[4] - Temp[2]))

  output_file <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_mile-minute_borough/', period, '_per_mile.csv')
  write.csv(results, output_file, row.names = FALSE)
  
  Temp = fivenum(trips_reg$fare_per_minute, na.rm = TRUE)
  results <- data.frame(Period = period, Minimum = Temp[2] - (1.5 * (Temp[4] - Temp[2])), 
                        FirstPerc = Temp[2], 
                        Median = Temp[3], 
                        ThirdPerc = Temp[4], 
                        Maximum = Temp[4] + 1.5 * (Temp[4] - Temp[2]))
  
  output_file <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_mile-minute_borough/', period, '_per_minute.csv')
  write.csv(results, output_file, row.names = FALSE)
  
}

files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/', pattern = "*.parquet", full.names = TRUE)
lapply(files, func_fare_borough_func)

func_fare_borough_func('c:/Users/Intel/Documents/Cursos/Henry/PF/yellow_tripdata_2021-12.parquet')


# ------------------ ver serie temporal ----------------------
func_trips_day <- function(x) {
  #fileinputpath <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/yellow_tripdata_', x, '.parquet')
  
  trips <- read_parquet(x)
  
  
  trips_reg <- trips %>% select(tpep_pickup_datetime, fare_amount)
  trips_reg <- trips_reg %>% mutate(date = as.Date(tpep_pickup_datetime))
  trips_trips_day <- trips202311_reg %>% group_by(date) %>% summarize(count = n())

  filename <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/trip_by_day/', str_replace(basename(x), "parquet", "csv"))
  print(filename)
  write.csv(trips_trips_day, file = filename)
  print(filename)
}

files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/', pattern = "*.parquet", full.names = TRUE)
lapply(files, func_trips_day)

files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/trip_by_day/', pattern = "*.csv", full.names = TRUE)

dataset <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))

dataset <- dataset %>% select(date, count)

dataset <- dataset %>% group_by(date) %>% summarize(count2 = sum(count)) %>% filter(count2 > 40 & date > 2018-01-01)

dataset %>% 
  ggplot(aes(x = date, y = count2)) + geom_point() + scale_x_date(date_labels = "%Y %b")

dataset <- dataset %>% mutate(date = as.Date(date, format = "%Y-%m-%d"))

write.csv(dataset, file = 'c:/Users/Intel/Documents/Cursos/Henry/PF/trip_by_day/ST_count_by_day.csv')

trips202311 <- read_parquet(fileinputpath)
trips202311 <- trips202311 %>% mutate(date = as.Date(tpep_pickup_datetime))

trips202311_trips_day <- trips202311 %>% group_by(date) %>% summarize(count = n())

print(trips$duracion)
ggplot(trips, aes(duracion, trip_distance)) + geom_point()

trips %>% filter(trip_distance < 100) %>% ggplot(aes(trip_distance, fare_amount)) + geom_point()


# ------------- graficos para primer demos --------------------------------



# -------------- estadistica de viajes por region -------------------------

est_by_borough_func <- function(x) {
  
  filename <- str_replace(basename(x), ".parquet", "")
  filename <- str_replace(filename, "yellow_tripdata_", "")

  trips_reg <- read_parquet(x)
  
  trips_reg <- trips_reg %>% select(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, fare_amount, trip_distance) %>%
    filter(fare_amount > 0 & trip_distance > 0)
  trips_reg <- trips_reg %>% mutate(date = as.Date(tpep_pickup_datetime), 
                                    PU_hour = hour(tpep_pickup_datetime), 
                                    trip_duration = round((as.numeric(tpep_dropoff_datetime - tpep_pickup_datetime)) / 60, 2), 
                                    fare_per_mile = round(fare_amount / trip_distance, 2), 
                                    fare_per_minute = round(fare_amount / trip_duration, 2))
  trips_reg <- trips_reg %>% select(-tpep_pickup_datetime, -tpep_dropoff_datetime)
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "PULocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'PUBorough')
  
  fare_per_mile_month_median <- trips_reg %>% median(fare_per_mile)
  fare_per_minute_month_median <- trips_reg %>% median(fare_per_minute)
  
  sumario <- trips_reg %>%
    group_by(PUBorough) %>% 
    summarize(min = round(min(fare_per_mile, na.rm = TRUE), 2),
              q1 = round(quantile(fare_per_mile, 0.25, na.rm = TRUE), 2),
              median = round(median(fare_per_mile, na.rm = TRUE), 2),
              mean = round(mean(fare_per_mile, na.rm = TRUE), 2),
              q3 = round(quantile(fare_per_mile, 0.75, na.rm = TRUE), 2),
              max = round(quantile(fare_per_mile, 0.75, na.rm = TRUE) + 1.5 * IQR(fare_per_mile, na.rm = TRUE), 2), 
              trip_numbers = n(), 
              month_median = round(fare_per_mile_month_median, 2))
  
  sumario <- sumario %>% mutate(period = filename)
  
  outputfile <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_mile_borough/fpmile_', filename, '.csv')
  write.csv(sumario, outputfile, row.names = FALSE)
  print(outputfile)
  
  sumario <- trips_reg %>%
    group_by(PUBorough) %>% 
    summarize(min = round(min(fare_per_minute, na.rm = TRUE), 2),
              q1 = round(quantile(fare_per_minute, 0.25, na.rm = TRUE), 2),
              median = round(median(fare_per_minute, na.rm = TRUE), 2),
              mean = round(mean(fare_per_minute, na.rm = TRUE), 2),
              q3 = round(quantile(fare_per_minute, 0.75, na.rm = TRUE), 2),
              max = round(quantile(fare_per_minute, 0.75, na.rm = TRUE) + 1.5 * IQR(fare_per_minute, na.rm = TRUE), 2), 
              trip_numbers = n(), 
              month_median = round(fare_per_minute_month_median))
  
  sumario <- sumario %>% mutate(period = filename)
  
  outputfile <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_minute_borough/fpmin_', filename, '.csv')
  write.csv(sumario, outputfile, row.names = FALSE)
  print(outputfile)

}

files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/', pattern = "*.parquet", full.names = TRUE)
lapply(files, est_by_borough_func)


# grafico boxplot con parametros ya calculados
files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/fare_per_mile_borough/', pattern = "*.csv", full.names = TRUE)

dataset <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))

dataset %>% filter(period == '2023-11' & PUBorough != 'Unknown' & PUBorough != 'EWR') %>% 
  ggplot(aes(x = PUBorough, 
             ymin = min, 
             lower = q1, 
             middle = median, 
             upper = q3, 
             ymax = max)) + 
  geom_boxplot(stat = "identity") + 
  ylim(0, 12)

dataset <- dataset %>% mutate(periodo = as.Date(ym(period)))


dataset %>% #filter(PUBorough != "EWR") %>%
  ggplot(aes(x = periodo, y = median, ymin = q1, ymax = q3, group = PUBorough, fill = PUBorough, color = PUBorough)) + 
  geom_line(size = 1) + geom_ribbon(alpha = 0.5) + scale_y_log10() + 
  scale_x_date(date_labels = "%b/%Y", date_breaks = "6 month") + 
  theme(axis.text.x = element_text(angle=45, vjust=1, hjust=1)) + 
  #scale_colour_brewer(type = "qual", palette = 2, aesthetics = "colour") + 
  ggtitle("Tarifa por milla recorrida")


# ------------------------- Sacar un sample de datos ---------------------------
regions <- read.csv('c:/Users/Intel/Documents/Cursos/Henry/PF/taxi_zone_lookup.csv')
regions <- regions %>% select(LocationID, Borough)

# funcion que levanta un parquet y lo procesa para que hacer left join con borough de partida y de llegada
# para obtener la cantidad de viajes por borough de partida y por borough de llegada
func1 <- function(x) {
  fileinputpath <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/yellow_tripdata_', x, '.parquet')
  
  trips <- read_parquet(fileinputpath)
  
  trips_reg <- trips %>% 
    filter(fare_amount > 0 & payment_type != 4 & trip_distance < 80) %>% 
    select(2,3,5,8:19)
  
  trips_reg <- trips_reg %>% sample_frac(0.05)
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "PULocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'PUBorough')
  
  trips_reg <- merge(x = trips_reg, y = regions, by.x = "DOLocationID", by.y = "LocationID", all = TRUE)
  trips_reg <- trips_reg %>% rename_at('Borough', ~'DOBorough')
  
  #trips_reg <- trips_reg %>% select(-c("DOLocationID", "PULocationID"))
  
  #trips_reg_month_sum <- trips_reg %>% group_by(PUBorough, DOBorough) %>% summarize(count = n())
  
  #trips_reg_month_sum <- trips_reg_month_sum %>% filter(PUBorough != 'Unknown' & DOBorough != 'Unknown')
  #trips_reg_month_sum <- trips_reg_month_sum %>% mutate(period = x)
  
  filename <- paste0('c:/Users/Intel/Documents/Cursos/Henry/PF/sample/sampled_csv_', x, '.csv')
  print(filename)
  write.csv(trips_reg, file = filename)
}

filelist <- append(paste0('2022-0', 1:9), paste0('2022-1', 0:2))

lapply(filelist, func1)

files <- list.files('c:/Users/Intel/Documents/Cursos/Henry/PF/sample/', pattern = "*.csv", full.names = TRUE)

dataset_sample <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))

dataset_sample$PUtime <- as.POSIXct(dataset_sample$PUtime, format = "%Y-%m-%d %H:%M:%S")  # se necesita pasarlo asi para que despues haga la resta bien
dataset_sample$DOtime <- as.POSIXct(dataset_sample$DOtime, format = "%Y-%m-%d %H:%M:%S")


dataset_sample <- dataset_sample %>% sample_frac(0.10)
dataset_sample$trip_duration <- dataset_sample$DOtime - dataset_sample$PUtime
dataset_sample$trip_duration <- round(dataset_sample$trip_duration, 2)


dataset_sample$fare_per_mile <- round(dataset_sample$fare_amount / dataset_sample$trip_distance, 2)
dataset_sample$fare_per_minute <- round(dataset_sample$fare_amount / dataset_sample$trip_duration, 2)

write.csv(dataset_sample, 'c:/Users/Intel/Documents/Cursos/Henry/PF/sample/dataset_sampled.csv')
