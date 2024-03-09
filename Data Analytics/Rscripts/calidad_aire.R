library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)

setwd('c:/Users/Intel/Documents/Cursos/Henry/PFb/')

air_quality <- read.csv('./Datasets/Air_Quality_20240216.csv')

# ------------------- concentracion de PM2.5 en aire ---------------------------

air_quality_pm25 <- air_quality %>% filter(Indicator.ID == 365) %>% select(1,6:11)

AQPM25_Borough <- air_quality_pm25 %>% filter(Geo.Type.Name == "Borough")
AQPM25_Borough_s <- AQPM25_Borough %>% filter(!grepl("Annual Average", Time.Period))
AQPM25_Borough_y <- AQPM25_Borough %>% filter(grepl("Annual Average", Time.Period))

AQPM25_Borough_s <- AQPM25_Borough_s %>% separate_wider_delim(Time.Period, delim = " ", names = c("season", "year"))

AQPM25_Borough_y$Time.Period <- substr(AQPM25_Borough_y$Time.Period, 16, 19)
AQPM25_Borough_y$Time.Period <- as.numeric(AQPM25_Borough_y$Time.Period)

# ---------- Respiratory hospitalizations due to PM2.5 (age 20+) ---------------
air_quality_Hpm25 <- air_quality %>% filter(Indicator.ID == 650) %>% select(1,6:11)

AQHPM25_Borough <- air_quality_Hpm25 %>% filter(Geo.Type.Name == "Borough")
AQHPM25_Borough_s <- AQHPM25_Borough %>% filter(!grepl("Annual Average", Time.Period))
AQHPM25_Borough_y <- AQHPM25_Borough %>% filter(grepl("Annual Average", Time.Period))

AQHPM25_Borough_s <- AQHPM25_Borough_s %>% separate_wider_delim(Time.Period, delim = " ", names = c("season", "year"))

AQHPM25_Borough_y$Time.Period <- substr(AQHPM25_Borough_y$Time.Period, 16, 19)
AQHPM25_Borough_y$Time.Period <- as.numeric(AQHPM25_Borough_y$Time.Period)

# ---------- Nitrogen dioxide (NO2) --------------------------------------------
air_quality_NO2 <- air_quality %>% filter(Indicator.ID == 375) %>% select(1, 6:11)

AQNO2_Borough <- air_quality_NO2 %>% filter(Geo.Type.Name == "Borough")
AQNO2_Borough_s <- AQNO2_Borough %>% filter(!grepl("Annual Average", Time.Period))
AQNO2_Borough_y <- AQNO2_Borough %>% filter(grepl("Annual Average", Time.Period))

AQNO2_Borough_s <- AQNO2_Borough_s %>% separate_wider_delim(Time.Period, delim = " ", names = c("season", "year"))

AQNO2_Borough_y$Time.Period <- substr(AQNO2_Borough_y$Time.Period, 16, 19)
AQNO2_Borough_y$Time.Period <- as.numeric(AQNO2_Borough_y$Time.Period)

AQNO2_Borough_y %>% ggplot(aes(x = Time.Period, y = Data.Value, color = Geo.Place.Name)) + geom_line()

AQNO2_Borough_s$month <- (month(as.Date(AQNO2_Borough_s$Start_Date, "%m/%d/%Y")))
AQNO2_Borough_s %>% ggplot(aes(x = anio, y = Data.Value, color = Geo.Place.Name)) + geom_line()

AQNO2_Borough_s %>% ggplot(aes(x = fecha, y = Data.Value, color = Geo.Place.Name)) + geom_line() + geom_smooth(method = loess)

# season + smooth + proporcion hibridos
hyb_taxis$year <- paste0(hyb_taxis$Model.Year, "-06-01")
hyb_taxis$year <- as.Date(hyb_taxis$year, "%Y-%m-%d")

ggplot() + geom_bar(aes(x = hyb_taxis$year, y = hyb_taxis$prop), stat = 'identity') +
  geom_line(aes(x = AQNO2_Borough_s$fecha,
                y = AQNO2_Borough_s$Data.Value * max(hyb_taxis$prop) / 14,
                color = AQNO2_Borough_s$Geo.Place.Name), size = 1, 
            stat = 'identity') + 
  scale_y_continuous(sec.axis = sec_axis(~ . * 14 / max(hyb_taxis$prop))) + 
  labs(title = "Evolucion de prop taxis hibridos y contaminacion NO2", color = "Borough") + 
  xlab("Año") + ylab("Proporcion hibridos")

fit <- group_by(AQNO2_Borough_s$year) %>% loess(Data.Value ~ fecha, degree=1, span = 0.01) %>% ungroup()

# veamos derivada
AQNO2_Borough_y_diff <- AQNO2_Borough_y %>% group_by(Geo.Place.Name) %>%
  summarize(rate = diff(Data.Value) / diff(Time.Period))

AQNO2_Borough_y_diff <- AQNO2_Borough_y_diff %>% mutate(anio = sort(unique(AQNO2_Borough_y$Time.Period))[2:13])



ggplot() + geom_line(aes(x = AQNO2_Borough_y_diff$anio, y = AQNO2_Borough_y_diff$rate, color = AQNO2_Borough_y_diff$Geo.Place.Name))

AQNO2_Borough_s$anio <- AQNO2_Borough_s$anio - min(AQNO2_Borough_s$anio)


AQNO2_Borough_s_diff<- AQNO2_Borough_s %>% group_by(Geo.Place.Name) %>%
  summarize(rate = diff(Data.Value) / diff(anio))
AQNO2_Borough_s_diff <- AQNO2_Borough_s_diff %>% mutate(dias = sort(unique(AQNO2_Borough_s$anio))[2:26])
AQNO2_Borough_s_diff$fecha <- AQNO2_Borough_s_diff$dias + min(as.Date(AQNO2_Borough_s$Start_Date, "%m/%d/%Y"))
ggplot() + geom_line(aes(x = AQNO2_Borough_s_diff$dias, y = AQNO2_Borough_s_diff$rate, color = AQNO2_Borough_s_diff$Geo.Place.Name))

# derivada por distritos
borough_lst <- c("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island")

AQPM25_Bdiff <- lapply(borough_lst, function(x) {
  tempdf <- AQPM25_Borough_s %>% filter(Geo.Place.Name == x)
  tempdf <- tempdf %>% mutate(fecha = as.Date(Start_Date, "%m/%d/%Y"), 
                              dia = as.numeric(fecha - min(fecha)))
  #tempdf <- mutate(dias = (tempdf$fecha - min(tempdf$fecha)))
  tempdf <- tempdf[order(tempdf$dia), ]
  fit <- loess(Data.Value ~ dia, degree = 1, span = 0.7, data = tempdf)
  tempdf <- tempdf %>% mutate(smooth = fit$fitted) %>% select(4,7:11)

  temp_deriv <- diff(tempdf$smooth) / diff(tempdf$dia)
  temp_deriv <- c(NaN, temp_deriv)
  tempdf <- tempdf %>% mutate(deriv = temp_deriv)
  tempdf
})

ggplot() + geom_line(aes(AQPM25_Bdiff[[3]]$fecha, AQPM25_Bdiff[[3]]$Data.Value), alpha = 0.7, color = "blue") + 
  geom_line(aes(AQPM25_Bdiff[[3]]$fecha, AQPM25_Bdiff[[3]]$smooth), color = "red")


ggplot() + geom_line(aes(AQPM25_Bdiff[[3]]$fecha, AQPM25_Bdiff[[3]]$deriv), color = "blue") + 
  #geom_tile(paste0("Derivadas de proporcion de taxis hibridos y contenido de NO2 en ", AQNO2_Bdiff[[3]]$Geo.Place.Name[1])) + 
  geom_line(aes(hyb_taxis$year, (-0.02)*hyb_taxis$rate), color = "red") + 
  xlab("Fecha") + ylab("derivada (u.a.)") + ggtitle("Derivada proporcion de taxis hibridos y concentracion PM2.5 en aire") + 
  geom_text(aes(x = as.Date("2005-01-01"), y = -0.002, label = "taxis hibridos (x (-1))"), stat = "unique", size = 5, color = "red") + 
  geom_text(aes(x = as.Date("2005-01-01"), y = -0.001, label = "PM2.5"), stat = "unique", size = 5, color = "blue") + 
  geom_text(aes(x = as.Date("2020-01-01"), y = -0.000, label = AQPM25_Bdiff[[3]]$Geo.Place.Name[1]), stat = "unique", size = 5)
  

sapply(1:5, function(x){
  write.csv(AQNO2_Bdiff[[x]], file = paste0("./Datasets/",x,".csv"))
})
write.csv(hyb_taxis, file = "./Datasets/hyb.csv")
