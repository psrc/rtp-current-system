# Libraries -----------------------------------------------------------------
library(tidyverse)
library(tidytransit)
library(sf)
library(psrcelmer)
library(units)

source("functions.R")

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Transportation/RTP_2026")

transit_dir <- file.path(data_dir, "transit")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

gtfs_year <- 2024
gtfs_service <- "fall"
if (tolower(gtfs_service)=="spring") {gtfs_month = "05"} else (gtfs_month = "10")

buffer_dist <- 0.25

hct_modes <- c("BRT", "Passenger Ferry", "Light Rail", "Streetcar", "Commuter Rail", "Auto Ferry")
bus_modes <- c("Bus", "ST Express")
ferry_modes <- c("Passenger Ferry", "Auto Ferry")
rail_modes<- c("Light Rail", "Streetcar")
transit_modes <- c("Bus", "ST Express", "BRT", "Passenger Ferry", "Light Rail", "Streetcar", "Commuter Rail", "Auto Ferry")

# Transit Typology
minimum_transit_trips <- 2
minimum_transit_start <- 6
minimum_transit_end <- 20
minimum_transit_hours <- minimum_transit_end - minimum_transit_start

all_day_transit_trips <- 3
all_day_transit_start <- 5
all_day_transit_end <- 22
all_day_transit_hours <- all_day_transit_end - all_day_transit_start

frequent_transit_trips <- 4
frequent_transit_start <- 6
frequent_transit_end <- 18
frequent_transit_hours <- frequent_transit_end - frequent_transit_start


# ElmerGeo layer names
water_lyr <- "LARGEST_WATERBODIES"

# Input File Paths -------------------------------------------------------------
gtfs_file <- file.path(transit_dir, "gtfs", tolower(gtfs_service), paste0(gtfs_year,".zip"))
hct_file <- file.path(transit_dir, "hct_ids.csv")
fgdb_file <- file.path(gis_dir,"transit","Transit_Network_2024.gdb")

# General spatial layers for analysis ---------------------------------------------
water <- st_read_elmergeo(water_lyr, project_to_wgs84 = FALSE) |> select(id = "object_id")

# Basic GTFS File Cleaning ---------------------------------------------------
# Load High-capacity Transit Route information for use in GTS file analysis
print(str_glue("Opening the High-Capacity Transit lookup file"))
hct <- read_csv(hct_file, show_col_types = FALSE) 

# Open Regional GTFS File and load into memory
print(str_glue("Opening the {gtfs_service} {gtfs_year} GTFS archive."))
gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))

# Load Stops
print(str_glue("Getting the {gtfs_service} {gtfs_year} stops into a tibble." ))
stops <- as_tibble(gtfs$stops) |> 
  mutate(stop_id = str_to_lower(stop_id)) |>
  select("stop_id", "stop_name", "stop_lat", "stop_lon")

# Load Routes, add HCT modes and update names and agencies
print(str_glue("Getting the {gtfs_service} {gtfs_year} routes into a tibble." ))
routes <- as_tibble(gtfs$routes) |> 
  mutate(route_id = str_to_lower(route_id)) |>
  select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")

print(str_glue("Adding High-Capacity Transit codes to the {gtfs_service} {gtfs_year} routes"))
routes <- left_join(routes, hct, by="route_id") |>
  mutate(type_code = case_when(
    is.na(type_code) ~ route_type,
    !(is.na(type_code)) ~ type_code)) |>
  mutate(route_name = case_when(
    is.na(route_name) ~ route_short_name,
    !(is.na(route_name)) ~ route_name)) |>
  mutate(type_name = case_when(
    is.na(type_name) ~ "Bus",
    !(is.na(type_name)) ~ type_name)) |>
  mutate(agency_name = case_when(
    !(is.na(agency_name)) ~ agency_name,
    is.na(agency_name) & str_detect(route_id, "ct") ~ "Community Transit",
    is.na(agency_name) & str_detect(route_id, "et") ~ "Everett Transit",
    is.na(agency_name) & str_detect(route_id, "kc") ~ "King County Metro",
    is.na(agency_name) & str_detect(route_id, "kt") ~ "Kitsap Transit",
    is.na(agency_name) & str_detect(route_id, "pt") ~ "Pierce Transit",
    is.na(agency_name) & str_detect(route_id, "st") ~ "Sound Transit")) |>
  select("route_id", "route_name", "type_name", "type_code", "agency_name")

# Trips are used to get route id onto stop times
print(str_glue("Getting the {gtfs_service} {gtfs_year} trips into a tibble to add route ID to stop times." ))
trips <- as_tibble(gtfs$trips) |> 
  mutate(route_id = str_to_lower(route_id)) |>
  select("trip_id", "route_id", "direction_id", "shape_id") |>
  mutate(direction_id = replace_na(direction_id, 0))

trips <- left_join(trips, routes, by=c("route_id"))

# Clean Up Stop Times to get routes and mode by stops served
print(str_glue("Getting the {gtfs_service} {gtfs_year} stop times into a tibble to add route information." ))
stoptimes <- as_tibble(gtfs$stop_times) |>
  mutate(stop_id = str_to_lower(stop_id)) |>
  filter(stop_sequence == 1) |>
  select("trip_id", "arrival_time")

stoptimes <- left_join(stoptimes, trips, by="trip_id") |>
  mutate(hour = hour(arrival_time), daily_trips = 1)

# Transit Typology --------------------------------------------------------
print(str_glue("Adding frequent, all-day, minimum and HCT defintions to the {gtfs_service} {gtfs_year} transit routes." ))
trips_by_hour_by_route <- stoptimes |>
  group_by(route_id, direction_id, hour) |>
  summarise(num_trips = sum(daily_trips)) |>
  as_tibble() |>
  mutate(hrs = 1)

# Minimum Transit Service: 2 trips per hour from 6am to 8pm
hrs_by_route <- trips_by_hour_by_route |>
  filter(hour >= minimum_transit_start & hour < minimum_transit_end) |>
  group_by(route_id, direction_id) |>
  summarise(hrs = sum(hrs)) |>
  as_tibble() |>
  filter(hrs >= minimum_transit_hours) |>
  select("route_id", "direction_id", "hrs") |>
  distinct()

trips_by_route <- trips_by_hour_by_route |>
  filter(hour >= minimum_transit_start & hour < minimum_transit_end & num_trips >= minimum_transit_trips) |>
  select("route_id", "direction_id", "num_trips") |>
  distinct()

minimum_transit_routes <- left_join(hrs_by_route, trips_by_route, by=c("route_id", "direction_id")) |>
  drop_na() |>
  select("route_id", "direction_id") |>
  distinct() |>
  mutate(min_routes = 1)

stoproutes <- left_join(stoptimes, minimum_transit_routes, by=c("route_id", "direction_id")) |> mutate(min_routes = replace_na(min_routes, 0))
rm(hrs_by_route, trips_by_route, minimum_transit_routes)  

# Averages 3 trips per hour between 5am and 10pm
hrs_by_route <- trips_by_hour_by_route |>
  filter(hour >= all_day_transit_start & hour < all_day_transit_end) |>
  group_by(route_id, direction_id) |>
  summarise(hrs = sum(hrs)) |>
  as_tibble() |>
  filter(hrs >= all_day_transit_hours) |>
  select("route_id", "direction_id", "hrs") |>
  distinct()

trips_by_route <- trips_by_hour_by_route |>
  filter(hour >= all_day_transit_start & hour < all_day_transit_end & num_trips >= all_day_transit_trips) |>
  select("route_id", "direction_id", "num_trips") |>
  distinct()

all_day_transit_routes <- left_join(hrs_by_route, trips_by_route, by=c("route_id", "direction_id")) |>
  drop_na() |>
  select("route_id", "direction_id") |>
  distinct() |>
  mutate(all_day = 1)

stoproutes <- left_join(stoproutes, all_day_transit_routes, by=c("route_id", "direction_id")) |> mutate(all_day = replace_na(all_day, 0))
rm(hrs_by_route, trips_by_route, all_day_transit_routes)  

# Averages 4 trips per hour between 6am and 6pm
hrs_by_route <- trips_by_hour_by_route |>
  filter(hour >= frequent_transit_start & hour < frequent_transit_end) |>
  group_by(route_id, direction_id) |>
  summarise(hrs = sum(hrs)) |>
  as_tibble() |>
  filter(hrs >= frequent_transit_hours) |>
  select("route_id", "direction_id", "hrs") |>
  distinct()

trips_by_route <- trips_by_hour_by_route |>
  filter(hour >= frequent_transit_start & hour < frequent_transit_end & num_trips >= frequent_transit_trips) |>
  select("route_id", "direction_id", "num_trips") |>
  distinct()

frequent_transit_routes <- left_join(hrs_by_route, trips_by_route, by=c("route_id", "direction_id")) |>
  drop_na() |>
  select("route_id", "direction_id") |>
  distinct() |>
  mutate(frequent = 1)

stoproutes <- left_join(stoproutes, frequent_transit_routes, by=c("route_id", "direction_id")) |> mutate(frequent = replace_na(frequent, 0))
rm(hrs_by_route, trips_by_route, frequent_transit_routes)  

# High-Capacity Transit
stoproutes <- stoproutes |>
  mutate(hct = case_when(
    type_code %in% c(0,1,2,4,5) ~ 1,
    type_code == 3 ~ 0))

# Transit Stops and Routes by Route Typology -----------------------------------------
print(str_glue("Getting the {gtfs_service} {gtfs_year} transit type details onto the stops and route_shapes files for output." ))

# List of trips
frequent_trip_ids <- stoproutes |> filter(frequent ==1) |> select("trip_id") |> pull() |> unique()
all_day_trip_ids <- stoproutes |> filter(all_day ==1) |> select("trip_id") |> pull() |> unique()
min_routes_trip_ids <- stoproutes |> filter(min_routes ==1) |> select("trip_id") |> pull() |> unique()
hct_trip_ids <- stoproutes |> filter(hct ==1) |> select("trip_id") |> pull() |> unique()

# List of stops
stoptimes <- as_tibble(gtfs$stop_times) |> mutate(stop_id = str_to_lower(stop_id)) |> select("trip_id", "stop_id")  
frequent_stop_ids <- stoptimes |> filter(trip_id %in% frequent_trip_ids) |> select("stop_id") |> pull() |> unique()
all_day_stop_ids <- stoptimes |> filter(trip_id %in% all_day_trip_ids) |> select("stop_id") |> pull() |> unique()
min_routes_stop_ids <- stoptimes |> filter(trip_id %in% min_routes_trip_ids) |> select("stop_id") |> pull() |> unique()
hct_stop_ids <- stoptimes |> filter(trip_id %in% hct_trip_ids) |> select("stop_id") |> pull() |> unique()

# List of route shapes
frequent_route_shape_ids <- stoproutes |> filter(trip_id %in% frequent_trip_ids) |> select("shape_id") |> pull() |> unique()
all_day_route_shape_ids <- stoproutes |> filter(trip_id %in% all_day_trip_ids) |> select("shape_id") |> pull() |> unique()
min_routes_route_shape_ids <- stoproutes |> filter(trip_id %in% min_routes_trip_ids) |> select("shape_id") |> pull() |> unique()
hct_route_shape_ids <- stoproutes |> filter(trip_id %in% hct_trip_ids) |> select("shape_id") |> pull() |> unique()

# Stops Layer
print(str_glue("Cleaning and writing stop points to {fgdb_file}"))
stops_lyr <- stops |>
  mutate(frequent = case_when(
    !(stop_id %in% frequent_stop_ids) ~ 0,
    stop_id %in% frequent_stop_ids ~ 1)) |>
  mutate(all_day = case_when(
    !(stop_id %in% all_day_stop_ids) ~ 0,
    stop_id %in% all_day_stop_ids ~ 1)) |>
  mutate(min_routes = case_when(
    !(stop_id %in% min_routes_stop_ids) ~ 0,
    stop_id %in% min_routes_stop_ids ~ 1)) |>
  mutate(hct = case_when(
    !(stop_id %in% hct_stop_ids) ~ 0,
    stop_id %in% hct_stop_ids ~ 1)) |>
  st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
  st_transform(spn) 

st_write(stops_lyr, dsn = fgdb_file, layer = paste0("transit_stops_", gtfs_service, "_", gtfs_year), append = FALSE)

# Minimum Transit Service Stop buffers
print(str_glue("Cleaning and writing the minimum transit stops {buffer_dist} mile buffer to {fgdb_file}"))
minimum_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(min_routes == 1) |>
  group_by(min_routes) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

minimum_transit_lyr <- st_difference(minimum_transit_lyr, water)
st_write(minimum_transit_lyr, dsn = fgdb_file, layer = paste0("minimum_transit_service_", gtfs_service, "_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# All Day Transit Service Stop buffers
print(str_glue("Cleaning and writing the all-day transit stops {buffer_dist} mile buffer to {fgdb_file}"))
all_day_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(all_day == 1) |>
  group_by(all_day) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

all_day_transit_lyr <- st_difference(all_day_transit_lyr, water)
st_write(all_day_transit_lyr, dsn = fgdb_file, layer = paste0("all_day_transit_service_", gtfs_service, "_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# Frequent Transit Service Stop buffers
print(str_glue("Cleaning and writing the frequent transit stops {buffer_dist} mile buffer to {fgdb_file}"))
frequent_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(frequent == 1) |>
  group_by(frequent) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

frequent_transit_lyr <- st_difference(frequent_transit_lyr, water)
st_write(frequent_transit_lyr, dsn = fgdb_file, layer = paste0("frequent_transit_service_", gtfs_service, "_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# High-Capacity Transit Service Stop buffers
print(str_glue("Cleaning and writing the high-capacity transit stops {buffer_dist} mile buffer to {fgdb_file}"))
hct_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(hct == 1) |>
  group_by(hct) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

hct_transit_lyr <- st_difference(hct_transit_lyr, water)
st_write(hct_transit_lyr, dsn = fgdb_file, layer = paste0("high_capacity_transit_service_", gtfs_service, "_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# Route Layer
print(str_glue("Cleaning and writing the transit route layer to {fgdb_file}"))
temp <- trips |> 
  select("shape_id", "direction_id", "route_name", "type_name", "agency_name") |>
  distinct()

routes_lyr <- shapes_as_sf(gtfs$shapes) 
routes_lyr <- left_join(routes_lyr, temp, by=c("shape_id")) |> 
  select("shape_id", direction = "direction_id", route = "route_name", type = "type_name", agency = "agency_name") |>
  mutate(frequent = case_when(
    !(shape_id %in% frequent_route_shape_ids) ~ 0,
    shape_id %in% frequent_route_shape_ids ~ 1)) |>
  mutate(all_day = case_when(
    !(shape_id %in% all_day_route_shape_ids) ~ 0,
    shape_id %in% all_day_route_shape_ids ~ 1)) |>
  mutate(min_routes = case_when(
    !(shape_id %in% min_routes_route_shape_ids) ~ 0,
    shape_id %in% min_routes_route_shape_ids ~ 1)) |>
  mutate(hct = case_when(
    !(shape_id %in% hct_route_shape_ids) ~ 0,
    shape_id %in% hct_route_shape_ids ~ 1)) |>
  st_transform(spn)

st_write(routes_lyr, dsn = fgdb_file, layer = paste0("transit_routes_", gtfs_service, "_", gtfs_year), append = FALSE)
rm(routes, stops, stoproutes, stoptimes, trips, trips_by_hour_by_route, gtfs, hct, temp)
