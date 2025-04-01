# Libraries -----------------------------------------------------------------
library(tidyverse)

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
tables_dir <- file.path(data_dir, "tables")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

annualization <- 320
model_years <- c(2023, 2050)

boardings_order <- c("Daily Boardings", "Community Transit", "Everett Transit",
                     "King County Metro", "Kitsap Transit", "Pierce Transit",
                     "Sound Transit", "Washington Ferries",
                     "Daily Transfers", "Daily Fare Transactions")

peak_periods <- c("5to6","6to7","7to8","8to9","14to15","15to16","16to17","17to18")
off_peak_periods <- c("9to10","10to14","18to20","20to5")

# Transit Data for Financial Tool -----------------------------------------

transit_agency_boardings <- NULL
for (y in model_years) {
  
  model_dir <- file.path(data_dir, "model", y)
  transit_agency_outputs_file <- file.path(model_dir, "daily_boardings_by_agency.csv")
  transit_transfers_file <- file.path(model_dir, "transit_transfers.csv")
  network_outputs_file <- file.path(model_dir, "network_results.csv")
  
  print(str_glue("Opening the {transit_agency_outputs_file} file"))
  t <- read_csv(transit_agency_outputs_file, show_col_types = FALSE) |>
    select(Agency = "agency_name", `Daily Boardings` = "boardings") |>
    mutate(`Daily Boardings` = round(`Daily Boardings`, -2)) |>
    arrange(Agency) |>
    mutate(Year = y) |>
    select("Year", "Agency", "Daily Boardings")
  
  print(str_glue("Opening the {network_outputs_file} file and filtering out ferry links for ferry vehicle volumes"))
  n <- read_csv(network_outputs_file, show_col_types = FALSE) |>
    filter(`@facilitytype` == 15) |>
    select(vehicles = "@tveh", "tod") |>
    distinct() |>
    mutate(Agency = "Washington Ferries") |>
    group_by(Agency) |>
    summarise(`Daily Boardings` = round(sum(vehicles), -2)) |>
    as_tibble() |>
    mutate(Year = y) |>
    select("Year", "Agency", "Daily Boardings")
  
  u <- bind_rows(t, n) |>
    group_by(`Year`, Agency) |>
    summarise(`Daily Boardings` = round(sum(`Daily Boardings`), -2)) |>
    as_tibble()
  
  r <- u |>
    mutate(Agency = "Daily Boardings") |>
    group_by(`Year`, Agency) |>
    summarise(`Daily Boardings` = sum(`Daily Boardings`)) |>
    as_tibble()
  
  u <- bind_rows(u, r) 
  
  print(str_glue("Opening the {transit_transfers_file} file"))
  tr <- read_csv(transit_transfers_file, show_col_types = FALSE) |>
    mutate(Agency = "Daily Transfers", Year = y) |>
    select("Agency", `Daily Boardings` = "boardings", "Year") |>
    group_by(`Year`, Agency) |>
    summarise(`Daily Boardings` = round(sum(`Daily Boardings`),-2)) |>
    as_tibble()
  
  u <- bind_rows(u, tr) 
  
  print(str_glue("Calcualting the daily fare transactions for {y}"))
  brds <- r |> select("Daily Boardings") |> pull()
  trns <- tr |> select("Daily Boardings") |> pull()
  tra <- tr |>
    mutate(Agency = "Daily Fare Transactions", `Daily Boardings` = brds - trns)
  
  u <- bind_rows(u, tra) |>
    mutate(Agency = factor(Agency, levels = boardings_order)) |>
    arrange(Agency)
  
  if(is.null(transit_agency_boardings)) {transit_agency_boardings <- u} else {transit_agency_boardings <- bind_rows(transit_agency_boardings, u)}
  rm(t, n, u, r, tr, brds, trns, tra)
  
}

transit_agency_boardings <- transit_agency_boardings |>
  pivot_wider(names_from = Year, values_from = `Daily Boardings`) |>
  as_tibble()

write_csv(transit_agency_boardings, file.path(tables_dir, "transit_boardings_by_agency.csv"))


# Model VMT Outputs for Financial Tool ----------------------------------------

vmt_outputs <- NULL
for (y in model_years) {
  
  network_outputs_file <- file.path(model_dir, "network_results.csv")

  print(str_glue("Opening the {y} {network_outputs_file} file"))
  network_output <- read_csv(network_outputs_file, show_col_types = FALSE) 

  print(str_glue("Calcualting VMT by vehicle type, county and time of day"))
  vmt_data <- network_output |>
    mutate(`Passenger Vehicles` = round((`@hov2_inc1` + `@hov2_inc2` + `@hov2_inc3` + 
          `@hov3_inc1` + `@hov3_inc2` + `@hov3_inc3` + 
          `@sov_inc1` + `@sov_inc2` + `@sov_inc3` +
          `@tnc_inc1` + `@tnc_inc2` + `@tnc_inc3`) * length, 0)) |>
    mutate(`Medium Trucks` = round(`@mveh`* length, 0)) |>
    mutate(`Heavy Trucks` = round(`@hveh`* length, 0)) |>
    mutate(`Buses` = round(`@bveh`* length, 0)) |>
    mutate(`Time of Day` = case_when(
      tod %in% peak_periods ~ "Peak",
      tod %in% off_peak_periods ~ "Off-Peak")) |>
    mutate(County = case_when(
      `@countyid` == 33 ~ "King",
      `@countyid` == 35 ~ "Kitsap",
      `@countyid` == 53 ~ "Pierce",
      `@countyid` == 61 ~ "Snohomish")) |>
    select("County", "Passenger Vehicles", "Medium Trucks", "Heavy Trucks", "Buses", "Time of Day") |>
    group_by(County, `Time of Day`) |>
    summarise(`Passenger Vehicles` = round(sum(`Passenger Vehicles`),-3), 
              `Medium Trucks` = round(sum(`Medium Trucks`),-3),
              `Heavy Trucks` = round(sum(`Heavy Trucks`),-3),
              `Buses` = round(sum(`Buses`),-2)) |>
    as_tibble() |>
    mutate(County = replace_na(County, "Outside the Region")) |>
    drop_na()

  region <- vmt_data |>
    mutate(County = "Region") |>
    group_by(County, `Time of Day`) |>
    summarise(`Passenger Vehicles` = round(sum(`Passenger Vehicles`),-3), 
              `Medium Trucks` = round(sum(`Medium Trucks`),-3),
              `Heavy Trucks` = round(sum(`Heavy Trucks`),-3),
              `Buses` = round(sum(`Buses`),-2)) |>
    as_tibble()

  daily_region <- region |>
    mutate(`Time of Day` = "Daily") |>
    group_by(County, `Time of Day`) |>
    summarise(`Passenger Vehicles` = round(sum(`Passenger Vehicles`),-3), 
              `Medium Trucks` = round(sum(`Medium Trucks`),-3),
              `Heavy Trucks` = round(sum(`Heavy Trucks`),-3),
              `Buses` = round(sum(`Buses`),-2)) |>
    as_tibble()

  daily <- vmt_data |>
    mutate(`Time of Day` = "Daily") |>
    group_by(County, `Time of Day`) |>
    summarise(`Passenger Vehicles`= round(sum(`Passenger Vehicles`),-3), 
              `Medium Trucks` = round(sum(`Medium Trucks`),-3),
              `Heavy Trucks` = round(sum(`Heavy Trucks`),-3),
              `Buses` = round(sum(`Buses`),-2)) |>
    as_tibble()
  
  vmt_data <- bind_rows(vmt_data, region, daily, daily_region) |>
    mutate(`Total` = `Passenger Vehicles` + `Medium Trucks` + `Heavy Trucks` + `Buses`) |>
    mutate(Total = round(Total, -3)) |>
    pivot_longer(cols = !c(County, `Time of Day`), names_to = "Mode", values_to = "Estimate") |>
    pivot_wider(names_from = c(County), values_from = Estimate) |>
    select("Time of Day", "Mode", "Region", "King", "Kitsap", "Pierce", "Snohomish") |>
    mutate(`Time of Day` = factor(`Time of Day`, levels = c("Daily", "Peak", "Off-Peak"))) |>
    mutate(Mode = factor(Mode, levels = c("Total", "Passenger Vehicles", "Medium Trucks", "Heavy Trucks", "Buses"))) |>
    arrange(`Time of Day`, Mode) |>
    mutate(Year = y)

  if(is.null(vmt_outputs)) {vmt_outputs <- vmt_data} else {vmt_outputs <- bind_rows(vmt_outputs, vmt_data)}
  rm(region, daily, daily_region, network_output, vmt_data)

}

write_csv(vmt_outputs, file.path(tables_dir, "vmt_outputs.csv"))


