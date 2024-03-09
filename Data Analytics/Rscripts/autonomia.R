library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)

# Estudiar los viajes que se hacen por distrito y en general y el tiempo que llevan
# de forma de poder ver las distancias recorridas y los tiempos involucrados
# de modo de calcular cual es la distancia recorrida en un turno de taxi, y en 2
# esto para poder relacionarlo con la autonomia de los EV y ver si necesitan ser
# recargados en el medio del turno o pueden aguantar, y si les da para meter un 2do turno

# ----------- carga de datos ---------------------------------------------------

path <- "../PF/trip_dist_dur_Borough/"

files <- list.files(paste0(path, '/distance/'), pattern = "*.csv", full.names = TRUE)

dataset <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))

write.csv(dataset, paste0(path, '/distance/trip_distance.csv'))

files <- list.files(paste0(path, 'duration/'), pattern = "*.csv", full.names = TRUE)

dataset <- do.call("rbind", lapply(files, FUN = function(file) {
  read.csv(file, header=TRUE, sep=",")
}))

write.csv(dataset, paste0(path, '/duration/trip_duration.csv'))

trip_distance <- read.csv(paste0(path, 'distance/trip_distance.csv'))
trip_duration <- read.csv(paste0(path, 'duration/trip_duration.csv'))

# incorporar dataset autos electricos
EV_df <- read_csv("./Datasets/my2012-2024-battery-electric-vehicles.csv")

# -------------------- trabajo -------------------------------------------------
trip_duration %>% 
  group_by(PUBorough) %>% 
  summarise(period = periodo, cant_viajes = 8 * 60 / (q3 * 2)) %>% 
  filter(PUBorough != 'EWR') %>%
  ggplot() + geom_line(aes(x = period, y = cant_viajes, color = PUBorough))

dataset <- left_join(trip_duration, trip_distance, by = c('PUBorough', 'period'))

dataset <- dataset %>% select(1:18)

dataset %>% 
  group_by(PUBorough) %>%
  summarise(period = periodo.x, 
            cant_viajes = 8 * 60 / (q3.x * 2), 
            dist_total = q3.y * cant_viajes * 2) %>%
  filter(PUBorough != "EWR") %>%
  ggplot() + geom_line(aes(x = period, y = dist_total, color = PUBorough))
  

dataset %>% filter(period == '2023-11' & PUBorough != 'Unknown' & PUBorough != 'EWR') %>% 
  ggplot(aes(x = PUBorough, 
             ymin = min, 
             lower = q1, 
             middle = median, 
             upper = q3, 
             ymax = max)) + 
  geom_boxplot(stat = "identity") + 
  ylim(0, 12)

trip_duration <- trip_duration %>% mutate(periodo = as.Date(ym(period)))
trip_distance <- trip_distance %>% mutate(periodo = as.Date(ym(period)))

trip_distance %>% #filter(PUBorough != "EWR") %>%
  ggplot(aes(x = periodo, y = median, ymin = q1, ymax = q3, group = PUBorough, fill = PUBorough, color = PUBorough)) + 
  geom_line(size = 1) + geom_ribbon(alpha = 0.3) + scale_y_log10() + 
  scale_x_date(date_labels = "%b/%Y", date_breaks = "6 month") + 
  theme(axis.text.x = element_text(angle=45, vjust=1, hjust=1)) + 
  #scale_colour_brewer(type = "qual", palette = 2, aesthetics = "colour") + 
  ggtitle("Duracion (min)")

# -------------------- incorporacion EV ----------------------------------------

# keeping only the models approved by NYC
# 23 Hyundai Ioniq 5 Electric Vehicle
# 22-23 Kia Niro Electric Vehicle
# 19-23 Tesla Model 3 RWD Electric Vehicle
# 19-23 Tesla Model 3 Long Range Electric Vehicle
# 21-23 Tesla Model Y long Range AWD Electric Vehicle
# 23 Toyota bZ4X Electric Vehicle

EV_df <- EV_df %>% filter(Make == 'Kia' | Make == 'Tesla' | Make == 'Toyota')
EV_df <- EV_df %>% filter(grepl('Niro', Model) | 
                   grepl('bZ4X', Model) | 
                   grepl('Model 3 RWD', Model) |
                   grepl('Model 3 Long Range', Model) |
                   grepl('Model Y', Model))
EV_df <- EV_df %>% mutate(Range_mile = `Range (km)` * 0.6213688756)

EV_df %>% mutate(Vehicle = paste0(Make, ' ', Model)) %>% 
  select(Vehicle, Range_mile) %>%
  distinct(Vehicle, .keep_all = TRUE) %>% 
  ggplot(aes(Vehicle, Range_mile)) + geom_col() + 
  geom_hline(yintercept= c(230, 250), linetype = c('dashed', 'dashed'), color = c('green', 'red'), size = c(1, 1)) + 
  ylab("Range (miles)") +
  coord_flip() 

