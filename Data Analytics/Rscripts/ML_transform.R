library(arrow)
library(dplyr)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(gganimate)


temp_region <- read.csv('../PF/taxi_zone_lookup.csv')
temp_NYC_zip <- read.csv('../PF/nyc_zip_borough_neighborhoods_pop.csv')

temp_pred <- read.csv('../PF/demanda_ML_raw.csv')

temp_pred <- left_join(temp_pred, temp_region, by = c("PULocationID" = "LocationID"))



temp_pred['PUMonth'][temp_pred['PUMonth'] == 1] <- 'January'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 2] <- 'February'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 3] <- 'March'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 4] <- 'April'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 5] <- 'May'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 6] <- 'June'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 7] <- 'July'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 'Agost'] <- 'August'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 9] <- 'September'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 10] <- 'October'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 11] <- 'November'
temp_pred['PUMonth'][temp_pred['PUMonth'] == 12] <- 'December'

temp_pred['PUDay'][temp_pred['PUDay'] == 1] <- 'Monday'
temp_pred['PUDay'][temp_pred['PUDay'] == 2] <- 'Tuesday'
temp_pred['PUDay'][temp_pred['PUDay'] == 3] <- 'Wednesday'
temp_pred['PUDay'][temp_pred['PUDay'] == 4] <- 'Thursday'
temp_pred['PUDay'][temp_pred['PUDay'] == 5] <- 'Friday'
temp_pred['PUDay'][temp_pred['PUDay'] == 6] <- 'Saturday'
temp_pred['PUDay'][temp_pred['PUDay'] == 7] <- 'Sunday'

temp_pred <- temp_pred %>% select(1:7)

write.csv(temp_pred, '../PF/temp_prediccion.csv', row.names = FALSE)

