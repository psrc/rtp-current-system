# Libraries -----------------------------------------------------------------
library(tidyverse)
library(data.table)
library(openxlsx)
library(tidytransit)
library(sf)
library(rhdf5)
library(psrcelmer)
library(units)

source("functions.R")

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
transit_dir <- file.path(data_dir, "transit")
model_dir <- file.path(data_dir, "model", "base-year")
spatial_inputs_dir <- file.path(data_dir,"spatial-layers","input-layers")
spatial_outputs_dir <- file.path(data_dir,"spatial-layers","output-layers")
congestion_dir <- file.path(data_dir,"npmrds")

options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

annualization <- 320
model_base_year <- 2023

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

severe_congestion_threshold <- 0.25
heavy_congestion_threshold <- 0.50
moderate_congestion_threshold <- 0.75

congestion_month <- 1
congestion_year <- 2025

# ElmerGeo layer names
non_sr_lyr <- "FUNC_CLASS_NON_SR"
sr_lyr <- "FUNC_CLASS_SR"
region_lyr <- "PSRC_REGIONAL_OUTLINE"
local_lyr <- "REG_STCL"
psrc_fips <- c("53033", "53035", "53053", "53061")

# Input File Paths -------------------------------------------------------------
gtfs_file <- file.path(transit_dir, "gtfs", tolower(gtfs_service), paste0(gtfs_year,".zip"))
hct_file <- file.path(transit_dir, "hct_ids.csv")
parcel_file <- file.path(model_dir, "parcels_urbansim.txt")
trips_file <- file.path(model_dir, "_trip.tsv")
hh_file <- file.path(model_dir, "hh_and_persons.h5")
its_file <- file.path(spatial_inputs_dir, "ITS_Signals_2024_Final.shp")
hrn_file <- file.path(spatial_inputs_dir, "filtered_hrn.gpkg")

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
  st_transform(spn) |>
  st_buffer(dist = buffer_dist * 5280)

st_write(stops_lyr, dsn = file.path(spatial_outputs_dir, "transit_stops.shp"), append = FALSE)

# Route Layer
routes_layer <- shapes_as_sf(gtfs$shapes) 
routes_layer <- left_join(routes_layer, trips, by=c("shape_id")) |> 
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

st_write(routes_layer, dsn = file.path(spatial_outputs_dir, "transit_routes.shp"), append = FALSE)

rm(routes, stops, stoproutes, stoptimes, trips, trips_by_hour_by_route, gtfs, hct)

# Parcels -----------------------------------------------------------------
p <- as_tibble(fread(parcel_file)) |>
  select(parcel_id = "parcelid", jobs = "emptot_p", hh = "hh_p",x = "xcoord_p", y = "ycoord_p") |>
  st_as_sf(coords = c("x", "y"), crs=spn)

# Parcels near Transit by Frequency & Span typologies
print(str_glue("Intersecting parcels with areas with a minimum level of transit service"))
m = st_intersection(p |> select("parcel_id"), stops_lyr |> filter(min_routes == 1) |> select("min_routes")) |>
  st_drop_geometry() |>
  distinct()

print(str_glue("Intersecting parcels with areas with all day transit service"))
a = st_intersection(p |> select("parcel_id"), stops_lyr |> filter(all_day == 1) |> select("all_day")) |>
  st_drop_geometry() |>
  distinct()

print(str_glue("Intersecting parcels with areas with frequent transit service"))
f = st_intersection(p |> select("parcel_id"), stops_lyr |> filter(frequent == 1) |> select("frequent")) |>
  st_drop_geometry() |>
  distinct()

print(str_glue("Intersecting parcels with areas with high-capacity transit service"))
h = st_intersection(p |> select("parcel_id"), stops_lyr |> filter(hct == 1) |> select("hct")) |>
  st_drop_geometry() |>
  distinct()

print(str_glue("Joining the transit service levels with the parcel records"))
p <- p |> st_drop_geometry()
p <- left_join(p, m, by="parcel_id") |> mutate(min_routes = replace_na(min_routes, 0))
p <- left_join(p, a, by="parcel_id") |> mutate(all_day = replace_na(all_day, 0))
p <- left_join(p, f, by="parcel_id") |> mutate(frequent = replace_na(frequent, 0))
p <- left_join(p, h, by="parcel_id") |> mutate(hct = replace_na(hct, 0))
p <- p |> mutate(year = model_base_year)

rm(a,f,h,m)

# Trips -------------------------------------------------------------------
t <- as_tibble(fread(trips_file)) |>
  filter(mode %in% c(3, 4, 5) & dorp==1) |>
  select("hhno", "travtime", "sov_ff_time") |>
  mutate(delay = travtime - (sov_ff_time/100)) |>
  mutate(delay = case_when(
    delay <0 ~ 0,
    delay >=0 ~ delay)) |>
  group_by(hhno) |>
  summarise(delay = sum(delay)) |>
  as_tibble()

# Delay per Household --------------------------------------------------------------
h <- as_tibble(h5read(hh_file, "Household")) |> select("hhno", parcel_id = "hhparcel")
h <- left_join(h, p |> select(-"jobs", -"hh"), by="parcel_id")
h <- left_join(h, t, by="hhno") |> mutate(delay = replace_na(delay, 0))

print(str_glue("Delay per Household - Minimum Transit Frequency & Span"))
d1 <- h |>
  select("min_routes", "delay") |>
  mutate(num_hhs = 1) |>
  group_by(min_routes) |>
  summarise(Households = sum(num_hhs), Delay = sum(delay)/60) |>
  as_tibble() |>
  mutate(`Delay per HH` = round((Delay*annualization) / Households,0)) |>
  mutate(min_routes = as.character(min_routes)) |>
  mutate(min_routes = case_when(
    min_routes == 0 ~ "not near Minimum Transit",
    min_routes == 1 ~ "near Minimum Transit")) |>
  rename(`Span & Frequency` = "min_routes") 

print(str_glue("Delay per Household - All Day Frequency & Span"))
d2 <- h |>
  select("all_day", "delay") |>
  mutate(num_hhs = 1) |>
  group_by(all_day) |>
  summarise(Households = sum(num_hhs), Delay = sum(delay)/60) |>
  as_tibble() |>
  mutate(`Delay per HH` = round((Delay*annualization) / Households,0)) |>
  mutate(all_day = as.character(all_day)) |>
  mutate(all_day = case_when(
    all_day == 0 ~ "not near All Day Transit",
    all_day == 1 ~ "near All Day Transit")) |>
  rename(`Span & Frequency` = "all_day") 

print(str_glue("Delay per Household - Frequent Frequency & Span"))
d3 <- h |>
  select("frequent", "delay") |>
  mutate(num_hhs = 1) |>
  group_by(frequent) |>
  summarise(Households = sum(num_hhs), Delay = sum(delay)/60) |>
  as_tibble() |>
  mutate(`Delay per HH` = round((Delay*annualization) / Households,0)) |>
  mutate(frequent = as.character(frequent)) |>
  mutate(frequent = case_when(
    frequent == 0 ~ "not near Frequent Transit",
    frequent == 1 ~ "near Frequent Transit")) |>
  rename(`Span & Frequency` = "frequent") 

print(str_glue("Delay per Household - High-Capacity Frequency & Span"))
d4 <- h |>
  select("hct", "delay") |>
  mutate(num_hhs = 1) |>
  group_by(hct) |>
  summarise(Households = sum(num_hhs), Delay = sum(delay)/60) |>
  as_tibble() |>
  mutate(`Delay per HH` = round((Delay*annualization) / Households, 0)) |>
  mutate(hct = as.character(hct)) |>
  mutate(hct = case_when(
    hct == 0 ~ "not near High-Capacity Transit",
    hct == 1 ~ "near High-Capacity Transit")) |>
  rename(`Span & Frequency` = "hct") 

print(str_glue("Delay per Household - Region"))
r <- h |>
  select("delay") |>
  mutate(region = "PSRC Region", num_hhs = 1) |>
  group_by(region) |>
  summarise(Households = sum(num_hhs), Delay = sum(delay)/60) |>
  as_tibble() |>
  mutate(`Delay per HH` = round((Delay*annualization) / Households,0)) |>
  rename(`Span & Frequency` = "region") 

delay_per_hh <- bind_rows(d1, d2, d3, d4, r)
rm(d1, d2, d3, d4, r)

# Roadways by Functional Classification -----------------------------------
r1 <- st_read_elmergeo(region_lyr, project_to_wgs84 = FALSE) |> select(county = "id")

r2 <- st_read_elmergeo(non_sr_lyr, project_to_wgs84 = FALSE) |>
  filter(county_fips %in% psrc_fips) |>
  select(id = "route_ident", length = "shape_leng", fc = "federalf_1")

r2 <- r2 |>
  mutate(centerline = as.numeric(set_units(st_length(r2), "mi")))

r3 <- st_read_elmergeo(sr_lyr, project_to_wgs84 = FALSE) |>
  select(id = "routeident", length = "shape_leng", fc = "federalf_1")

r3 <- st_intersection(r3, r1) 

r3 <- r3 |>
  mutate(centerline = as.numeric(set_units(st_length(r3), "mi"))) |>
  select(-"county")

r4 <- st_read_elmergeo(local_lyr, project_to_wgs84 = FALSE)

r5 <- r4 |>
  mutate(centerline = as.numeric(set_units(st_length(r4), "mi"))) |>
  mutate(length = as.numeric(set_units(st_length(r4), "ft"))) |>
  filter(facilityty <= 9) |>
  select(id = "psrcedgeid", "length", fc = "facilityty", "centerline") |>
  st_drop_geometry() |>
  group_by(fc) |>
  summarise(centerline_miles = sum(centerline)) |>
  as_tibble() |>
  filter(fc == 8) |>
  mutate(fc = as.character(fc)) |>
  mutate(fc = "Local Roadways - 2022 RTP")

functional_classified_roadways <- bind_rows(r2, r3)

centerline_mi <- bind_rows(functional_classified_roadways) |>
  st_drop_geometry() |>
  group_by(fc) |>
  summarize(centerline_miles = sum(centerline)) |>
  as_tibble()

region <- centerline_mi |>
  mutate(fc = "Region") |>
  group_by(fc) |>
  summarize(centerline_miles = sum(centerline_miles)) |>
  as_tibble()

centerline_mi <- bind_rows(centerline_mi, region, r5)
rm(r1, r2, r3, r4, r5, region)

# ITS Overlays ------------------------------------------------------------
its <- read_sf(its_file) |> st_transform(spn)
its_buffer <- read_sf(its_file) |> st_transform(spn) |> st_buffer(dist = 50)
frequent_routes <- routes_layer |> filter(frequent == 1) |> select("shape_id", "route") |> st_transform(spn)

i <- st_intersection(its_buffer, frequent_routes) |>
  select("OBJECTID", street = "majorst_1", "tsp") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(frequent = "Yes")

its_tsp <- left_join(its, i |> select("OBJECTID", "frequent"), by = "OBJECTID") |>
  filter(frequent == "Yes") |>
  select("OBJECTID", "frequent", "tsp") |>
  mutate(tsp = str_replace_all(tsp, "Null", "No"))

st_write(its_tsp, dsn = file.path(spatial_outputs_dir,"frequent_transit_tsp.shp"), append = FALSE)

tsp_summary <- its_tsp |>
  st_drop_geometry() |>
  mutate(count=1) |>
  group_by(tsp) |>
  summarise(count = sum(count)) |>
  as_tibble()

# Safety Overlays ------------------------------------------------------------
safety <- read_sf(hrn_file) |> st_transform(spn)

i <- st_intersection(its_buffer, safety) |>
  select("OBJECTID", street = "majorst_1", "ped_signal") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(pedestrian = "Yes")

its_ped <- left_join(its, i |> select("OBJECTID", "pedestrian"), by = "OBJECTID") |>
  filter(pedestrian == "Yes") |>
  select("OBJECTID", "pedestrian", "ped_signal") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No"))

st_write(its_ped, dsn = file.path(spatial_outputs_dir, "pedestrian_signals.shp"), append = FALSE)

ped_summary <- its_ped |>
  st_drop_geometry() |>
  mutate(count=1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble()

# Congestion Overlays ------------------------------------------------------------
congested_lanes_miles <- process_npmrds_data(file_path = congestion_dir) |>
  filter(geography == "Region" & grouping %in% c("Heavy", "Severe") & variable == "PM Peak Period") |>
  select("date", "variable", "grouping", "share") |>
  pivot_wider(names_from = grouping, values_from = share) |>
  as_tibble() |>
  select("date", time_period = "variable", heavy_congestion = "Heavy", severe_congestion = "Severe") |>
  mutate(metric = "% of NHS lane-miles congested by time of day")

congestion <- map_npmrds_data(file_path = congestion_dir, coord_sys = spn)

congestion_lyr <- congestion |> 
  filter(pm_peak <= 0.25 & month(date) == congestion_month & year(date) == congestion_year) |>
  select("Tmc", "pm_peak")

st_write(congestion_lyr, dsn = file.path(spatial_outputs_dir, "congestion_roadways.shp"), append = FALSE)

i <- st_intersection(its_buffer, congestion_lyr) |>
  select("OBJECTID", street = "majorst_1", "ts_asc", "ts_coordin") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(congestion = "Yes")

its_cong <- left_join(its, i |> select("OBJECTID", "congestion"), by = "OBJECTID") |>
  filter(congestion == "Yes") |>
  select("OBJECTID", "congestion", "ts_asc", "ts_coordin") |>
  mutate(ts_asc = str_replace_all(ts_asc, "Null", "No")) |>
  mutate(ts_coordin = str_replace_all(ts_coordin, "Null", "No")) |>
  mutate(signals = case_when (
    ts_asc == "Yes" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "Yes" & ts_coordin == "No" ~ "Yes",
    ts_asc == "No" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "No" & ts_coordin == "No" ~ "No"))

st_write(its_cong, dsn = file.path(spatial_outputs_dir, "congestion_signals.shp"), append = FALSE)

cong_summary <- its_cong |>
  st_drop_geometry() |>
  mutate(count=1) |>
  group_by(signals) |>
  summarise(count = sum(count)) |>
  as_tibble()

# Final Data Cleanup ------------------------------------------------------


