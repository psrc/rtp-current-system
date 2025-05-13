# Libraries -----------------------------------------------------------------
library(tidyverse)
library(tidytransit)
library(sf)
library(psrcelmer)
library(units)

source("functions.R")

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Sharing/Projects/Transportation/RTP_2026")
transit_dir <- file.path(data_dir, "transit")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
gtfs_year <- 2050
wgs84 <- 4326
spn <- 2285

buffer_dist <- 0.25

bus_type <- 3

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
gtfs_file <- file.path(transit_dir, "GTFS", "combined", gtfs_year, paste0(gtfs_year,".zip"))
tph_file <- file.path(transit_dir, "GTFS", "combined", paste0("tph_by_line_", gtfs_year,".csv"))
fgdb_file <- file.path(transit_dir, paste0("Transit_Network_", gtfs_year, ".gdb"))

# General spatial layers for analysis ---------------------------------------------
water <- st_read_elmergeo(water_lyr, project_to_wgs84 = FALSE) |> select(id = "object_id")

# Basic GTFS File Cleaning ---------------------------------------------------

# Open Regional GTFS File and load into memory
print(str_glue("Opening the {gtfs_year} GTFS archive."))
gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))

# Load Stops
print(str_glue("Getting the {gtfs_year} stops into a tibble." ))
stops <- as_tibble(gtfs$stops) |> 
  mutate(stop_id = str_to_lower(stop_id)) |>
  select("stop_id", "stop_name", "stop_lat", "stop_lon")

# Load Routes
print(str_glue("Getting the {gtfs_year} routes into a tibble." ))
route_details <- as_tibble(gtfs$routes) |> 
  mutate(route_id = str_to_lower(route_id)) |>
  select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")

# Load Route Trips per Hour
print(str_glue("Opening the {gtfs_year} trips per hour file"))
tph <- read_csv(tph_file, show_col_types = FALSE) |>
  group_by(route_id, direction_id) |>
  summarise(across(starts_with("hour_"), sum, .names = "{.col}"), .groups = "drop") |>
  mutate(direction_id = replace_na(direction_id, 0)) |>
  mutate(route_id = str_to_lower(route_id))

routes <- left_join(tph, route_details, by="route_id") |>
  select("route_id", "direction_id", "agency_id", "route_short_name", "route_long_name", "route_type", contains("hour_"))

# Local Transit Routes
print(str_glue("Determining which {gtfs_year} transit routes meet the local transit threshold"))
current_cols <- c()
for(h in seq(minimum_transit_start, (minimum_transit_end-1))) {
  current_cols <- append(current_cols, paste0("hour_",h))
}
  
local_routes <- routes |>
  select("route_id", "direction_id", all_of(current_cols)) |>
  # If all hours are above the threshold, set flag to 1
  rowwise() |>
  mutate(local_transit = if_else(all(c_across(all_of(current_cols)) >= minimum_transit_trips), 1, 0)) |>
  ungroup() |>
  # Count of Hours above the threshold
  rowwise() |>
  mutate(hrs_local_transit = sum(c_across(all_of(current_cols)) >= minimum_transit_trips)) |>
  ungroup() |>
  select("route_id", "direction_id", "local_transit", "hrs_local_transit") 

routes <- left_join(routes, local_routes, by=c("route_id", "direction_id"))

# All Day Transit Routes
print(str_glue("Determining which {gtfs_year} transit routes meet the all-day transit threshold"))
current_cols <- c()
for(h in seq(all_day_transit_start, (all_day_transit_end-1))) {
  current_cols <- append(current_cols, paste0("hour_",h))
}

all_day_routes <- routes |>
  select("route_id", "direction_id", all_of(current_cols)) |>
  # If all hours are above the threshold, set flag to 1
  rowwise() |>
  mutate(all_day_transit = if_else(all(c_across(all_of(current_cols)) >= all_day_transit_trips), 1, 0)) |>
  ungroup() |>
  # Count of Hours above the threshold
  rowwise() |>
  mutate(hrs_all_day_transit = sum(c_across(all_of(current_cols)) >= all_day_transit_trips)) |>
  ungroup() |>
  select("route_id", "direction_id", "all_day_transit", "hrs_all_day_transit")

routes <- left_join(routes, all_day_routes, by=c("route_id", "direction_id"))

# Frequent Transit Routes
print(str_glue("Determining which {gtfs_year} transit routes meet the frequent transit threshold"))
current_cols <- c()
for(h in seq(frequent_transit_start, (frequent_transit_end-1))) {
  current_cols <- append(current_cols, paste0("hour_",h))
}

frequent_routes <- routes |>
  select("route_id", "direction_id", all_of(current_cols)) |>
  # If all hours are above the threshold, set flag to 1
  rowwise() |>
  mutate(frequent_transit = if_else(all(c_across(all_of(current_cols)) >= frequent_transit_trips), 1, 0)) |>
  ungroup() |>
  # Count of Hours above the threshold
  rowwise() |>
  mutate(hrs_frequent_transit = sum(c_across(all_of(current_cols)) >= frequent_transit_trips)) |>
  ungroup() |>
  select("route_id", "direction_id", "frequent_transit", "hrs_frequent_transit")

routes <- left_join(routes, frequent_routes, by=c("route_id", "direction_id"))

# High-Capacity Transit Routes
print(str_glue("Determining which {gtfs_year} transit routes meet the High-Capacity transit threshold"))
current_cols <- "route_type"
hct_routes <- routes |>
  select("route_id", "direction_id", "route_type") |>
  rowwise() |>
  mutate(hct_transit = if_else(all(c_across(all_of(current_cols)) == bus_type), 0, 1)) |>
  ungroup() |>
  select(-"route_type")

routes <- left_join(routes, hct_routes, by=c("route_id", "direction_id"))
rm(all_day_routes, frequent_routes, hct_routes, local_routes, route_details, tph)

# Get a cleaned version of route id and transit typology for stop selection
local <- routes |>
  select("route_id", "local_transit") |>
  filter(local_transit == 1) |>
  distinct() |>
  select("route_id") |>
  pull()

all_day <- routes |>
  select("route_id", "all_day_transit") |>
  filter(all_day_transit == 1) |>
  distinct() |>
  select("route_id") |>
  pull()

frequent <- routes |>
  select("route_id", "frequent_transit") |>
  filter(frequent_transit == 1) |>
  distinct() |>
  select("route_id") |>
  pull()

hct <- routes |>
  select("route_id", "hct_transit") |>
  filter(hct_transit == 1) |>
  distinct() |>
  select("route_id") |>
  pull()

# Get Unique Route ID's with Transit Typology for Stop Selection
route_typology <- routes |>
  select("route_id") |>
  distinct() |>
  mutate(local = case_when(
    route_id %in% local ~ 1,
    !(route_id %in% local) ~ 0)) |>
  mutate(all_day = case_when(
    route_id %in% all_day ~ 1,
    !(route_id %in% all_day) ~ 0)) |>
  mutate(frequent = case_when(
    route_id %in% frequent ~ 1,
    !(route_id %in% frequent) ~ 0)) |>
  mutate(hct = case_when(
    route_id %in% hct ~ 1,
    !(route_id %in% hct) ~ 0))

# Trips are used to get route id onto stop times
print(str_glue("Getting the {gtfs_year} trips into a tibble to add route ID to stop times." ))
trips <- as_tibble(gtfs$trips) |> 
  mutate(route_id = str_to_lower(route_id)) |>
  select("trip_id", "route_id", "direction_id", "shape_id") |>
  mutate(direction_id = replace_na(direction_id, 0))

trips <- left_join(trips, route_typology, by=c("route_id"))

# Clean Up Stop Times to get routes and mode by stops served
print(str_glue("Getting the {gtfs_year} stop times into a tibble to add route information." ))
stoptimes <- as_tibble(gtfs$stop_times) |>
  mutate(stop_id = str_to_lower(stop_id)) |>
  select("stop_id", "trip_id")

stops_by_type <- left_join(stoptimes, trips, by="trip_id") |>
  select("stop_id", "route_id", "local", "all_day", "frequent", "hct") |>
  distinct()

local_stop_ids <- stops_by_type |> filter(local == 1) |> select("stop_id") |> distinct() |> pull()
all_day_stop_ids <- stops_by_type |> filter(all_day == 1) |> select("stop_id") |> distinct() |> pull()
frequent_stop_ids <- stops_by_type |> filter(frequent == 1) |> select("stop_id") |> distinct() |> pull()
hct_stop_ids <- stops_by_type |> filter(hct == 1) |> select("stop_id") |> distinct() |> pull()

stops <- stops |>
  mutate(local = case_when(
    stop_id %in% local_stop_ids ~ 1,
    !(stop_id %in% local_stop_ids) ~ 0)) |>
  mutate(all_day = case_when(
    stop_id %in% all_day_stop_ids ~ 1,
    !(stop_id %in% all_day_stop_ids) ~ 0)) |>
  mutate(frequent = case_when(
    stop_id %in% frequent_stop_ids ~ 1,
    !(stop_id %in% frequent_stop_ids) ~ 0)) |>
  mutate(hct = case_when(
    stop_id %in% hct_stop_ids ~ 1,
    !(stop_id %in% hct_stop_ids) ~ 0))

stops_lyr <- stops |>
  st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
  st_transform(spn) 

# Clean Up Routes Layer
print(str_glue("Getting the {gtfs_year} route layer and adding route information." ))
routes_lyr <- shapes_as_sf(gtfs$shapes) |> st_transform(spn)

trips_shapes <- trips |>
  select("shape_id", "route_id", "direction_id", "local", "all_day", "frequent", "hct") |>
  distinct()

routes_typology <- routes |>
  select("route_id", "direction_id", "agency_id", "route_short_name", "route_long_name", "route_type", 
         hrs_local = "hrs_local_transit", hrs_all_day = "hrs_all_day_transit", hrs_frequent = "hrs_frequent_transit")

routes_lyr <- left_join(routes_lyr, trips_shapes, by="shape_id")
routes_lyr <- left_join(routes_lyr, routes_typology, by=c("route_id", "direction_id")) |>
  select("route_id", "direction_id", "agency_id", short_name = "route_short_name", name = "route_long_name", "route_type",
         "local", "hrs_local", "all_day", "hrs_all_day", "frequent", "hrs_frequent", "hct")

rm(gtfs, stops_by_type, stoptimes, trips, routes_typology, routes, trips_shapes, stops, route_typology)

# Output to FileGeodatabase -----------------------------------------------

# Stops Layer
print(str_glue("Writing the {gtfs_year} stops layer to the file-geodatabase." ))
st_write(stops_lyr, dsn = fgdb_file, layer = paste0("transit_stops_", gtfs_year), append = FALSE)

# Routes Layer
print(str_glue("Writing the {gtfs_year} routes layer to the file-geodatabase." ))
st_write(routes_lyr, dsn = fgdb_file, layer = paste0("transit_routes_", gtfs_year), append = FALSE)

# Local Transit Service Stop buffers
print(str_glue("Cleaning and writing the local transit stops {buffer_dist} mile buffer to the file-geodatabase."))
local_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(local == 1) |>
  group_by(local) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

local_transit_lyr <- st_difference(local_transit_lyr, water)
st_write(local_transit_lyr, dsn = fgdb_file, layer = paste0("local_transit_service_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# All Day Transit Service Stop buffers
print(str_glue("Cleaning and writing the all-day transit stops {buffer_dist} mile buffer to the file-geodatabase."))
all_day_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(all_day == 1) |>
  group_by(all_day) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

all_day_transit_lyr <- st_difference(all_day_transit_lyr, water)
st_write(all_day_transit_lyr, dsn = fgdb_file, layer = paste0("all_day_transit_service_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# Frequent Transit Service Stop buffers
print(str_glue("Cleaning and writing the frequent transit stops {buffer_dist} mile buffer to the file-geodatabase."))
frequent_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(frequent == 1) |>
  group_by(frequent) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

frequent_transit_lyr <- st_difference(frequent_transit_lyr, water)
st_write(frequent_transit_lyr, dsn = fgdb_file, layer = paste0("frequent_transit_service_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)

# High-Capacity Transit Service Stop buffers
print(str_glue("Cleaning and writing the high-capacity transit stops {buffer_dist} mile buffer to the file-geodatabase."))
hct_transit_lyr <- stops_lyr |>
  st_buffer(dist = buffer_dist * 5280) |>
  filter(hct == 1) |>
  group_by(hct) |>
  summarise(geometry = st_union(geometry), .groups = "drop")

hct_transit_lyr <- st_difference(hct_transit_lyr, water)
st_write(hct_transit_lyr, dsn = fgdb_file, layer = paste0("high_capacity_transit_service_", gtfs_year, "_buffer_", ifelse(buffer_dist==0.25, "qtr_mile", "hlf_mile")), append = FALSE)
