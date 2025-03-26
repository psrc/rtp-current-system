# Libraries -----------------------------------------------------------------
library(tidyverse)
library(sf)
library(psrcelmer)
library(rhdf5)
library(data.table)

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Transportation/RTP_2026")
model_dir_2030 <- file.path(data_dir, "model", "2030")
model_dir_2050 <- file.path(data_dir, "model", "2050")

transit_dir <- file.path(data_dir, "transit")
options(dplyr.summarise.inform = FALSE)

population_csv_file <- file.path(gis_dir,"finance","finance_tool_inputs_population.csv")

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

existing_years <- c(2024)

ptba_lyr <- "PTBA"
st_fgdb_file <- file.path(gis_dir,"transit", "sound_transit_district.gdb")

parcel_file_2030 <- file.path(model_dir_2030, "parcels_urbansim.txt")
hh_file_2030 <- file.path(model_dir_2030, "hh_and_persons.h5")

parcel_file_2050 <- file.path(model_dir_2050, "parcels_urbansim.txt")
hh_file_2050 <- file.path(model_dir_2050, "hh_and_persons.h5")


# Current Population Data -------------------------------------------------
parcel_data <- NULL
for (y in existing_years) {
  
  # Parcel population
  print(str_glue("Loading {y} OFM based parcelized estimates of total population"))
  if (y >= 2020) {ofm_vintage <- y} else {ofm_vintage <- 2020}
  q <- paste0("SELECT parcel_dim_id, estimate_year, total_pop from ofm.parcelized_saep_facts WHERE ofm_vintage = ", ofm_vintage, " AND estimate_year = ", y, "")
  p <- get_query(sql = q)
  
  # Parcel Dimensions
  if (y >=2018) {parcel_yr <- 2018} else {parcel_yr <- 2014}
  print(str_glue("Loading {parcel_yr} parcel dimensions from Elmer"))
  q <- paste0("SELECT parcel_dim_id, parcel_id, county_name, x_coord_state_plane, y_coord_state_plane from small_areas.parcel_dim WHERE base_year = ", parcel_yr, " ")
  d <- get_query(sql = q) 
  
  # Add 2010 and 2020 Blockgroup IDs to Parcels
  p <- left_join(p, d, by="parcel_dim_id")
  
  if (is.null(parcel_data)) {parcel_data <- p} else {parcel_data <- bind_rows(parcel_data, p)}
  rm(p, q, d)
}

parcel_data <- parcel_data |>
  select("parcel_id", x = "x_coord_state_plane", y = "y_coord_state_plane", county = "county_name", population = "total_pop")

print(str_glue("Creating a parcel layer from the parcel data"))
parcel_lyr <- st_as_sf(parcel_data, coords = c("x", "y"), crs = spn) |> select("parcel_id")

print(str_glue("Loading {ptba_lyr} from ElmerGeo"))
ptba <- st_read_elmergeo(ptba_lyr, project_to_wgs84 = FALSE) |> 
  select(ptba = "ptba_name") |>
  filter(ptba %in% c("Snohomish PTBA", "King PTBA", "Kitsap PTBA", "Everett PTBA", "Pierce PTBA")) |>
  st_transform(spn)

print(str_glue("Adding PTBA boundary to parcel inputs"))
p <- st_intersection(parcel_lyr, ptba) |>
  st_drop_geometry() |>
  select("parcel_id", "ptba")

parcel_data <- left_join(parcel_data, p, by="parcel_id")
parcel_data <- parcel_data |>
  mutate(ptba = replace_na(ptba, "outside a PTBA boundary"))

print(str_glue("Adding Sound Transit boundary to parcel inputs"))
st_boundary <- st_read(dsn = st_fgdb_file, layer = "sound_transit_district") |> 
  st_transform(spn) |>
  mutate(district_name = str_to_title(district_name))

p <- st_intersection(parcel_lyr, st_boundary) |>
  st_drop_geometry() |>
  select("parcel_id", "district_name")

parcel_data <- left_join(parcel_data, p, by="parcel_id")
parcel_data <- parcel_data |>
  mutate(district_name = replace_na(district_name, "outside Sound Transit Distric")) |>
  rename(population_2024 = "population")

# Population Data: 2030 --------------------------------------------------
print(str_glue("Loading the parcels file for 2030"))
p <- as_tibble(fread(parcel_file_2030)) |>
  select(parcel_id = "PARCELID")

print(str_glue("Get people per parcel"))
h5p <- as_tibble(h5read(hh_file_2030, "Person")) |> 
  select("hhno") |>
  mutate(population_2030 = 1)

h5h <- as_tibble(h5read(hh_file_2030, "Household")) |> 
  select("hhno", parcel_id = "hhparcel")

h5p <- left_join(h5p, h5h, by="hhno") |>
  select(-"hhno") 

h5p <- h5p |>
  group_by(parcel_id) |>
  summarise(population_2030 = sum(population_2030)) |>
  as_tibble()

p <- left_join(p, h5p, by="parcel_id") |>
  mutate(population_2030 = replace_na(population_2030, 0))

parcel_data <- left_join(parcel_data, p, by="parcel_id") 

parcel_data <- parcel_data |>
  mutate(population_2030 = replace_na(population_2030, 0))

# Population Data: 2050 --------------------------------------------------
print(str_glue("Loading the parcels file for 2050"))
p <- as_tibble(fread(parcel_file_2050)) |>
  select(parcel_id = "PARCELID")

print(str_glue("Get people per parcel"))
h5p <- as_tibble(h5read(hh_file_2050, "Person")) |> 
  select("hhno") |>
  mutate(population_2050 = 1)

h5h <- as_tibble(h5read(hh_file_2050, "Household")) |> 
  select("hhno", parcel_id = "hhparcel")

h5p <- left_join(h5p, h5h, by="hhno") |>
  select(-"hhno") 

h5p <- h5p |>
  group_by(parcel_id) |>
  summarise(population_2050 = sum(population_2050)) |>
  as_tibble()

p <- left_join(p, h5p, by="parcel_id") |>
  mutate(population_2050 = replace_na(population_2050, 0))

parcel_data <- left_join(parcel_data, p, by="parcel_id") 

parcel_data <- parcel_data |>
  mutate(population_2050 = replace_na(population_2050, 0))

# Summary for Financial Strategy ------------------------------------------

county_summary <- parcel_data |>
  group_by(county) |>
  summarise(population_2024 = sum(population_2024), 
            population_2030 = sum(population_2030), 
            population_2050 = sum(population_2050)) |>
  as_tibble() |>
  pivot_longer(!county, names_to = "year", values_to = "population") |>
  mutate(population = as.integer(round(population,-3)), year = as.integer(str_remove_all(year, "population_"))) |>
  pivot_wider(names_from = county, values_from = population) |>
  mutate(Region = King + Kitsap + Pierce + Snohomish)

ptba_summary <- parcel_data |>
  group_by(ptba) |>
  summarise(population_2024 = sum(population_2024), 
            population_2030 = sum(population_2030), 
            population_2050 = sum(population_2050)) |>
  as_tibble() |>
  filter(ptba != "outside a PTBA boundary") |>
  pivot_longer(!ptba, names_to = "year", values_to = "population") |>
  mutate(population = as.integer(round(population,-3)), year = as.integer(str_remove_all(year, "population_"))) |>
  pivot_wider(names_from = ptba, values_from = population)

st_summary <- parcel_data |>
  group_by(district_name, county) |>
  summarise(population_2024 = sum(population_2024), 
            population_2030 = sum(population_2030), 
            population_2050 = sum(population_2050)) |>
  as_tibble() |>
  filter(county != "Kitsap") |>
  rename(st_district = "county") |>
  filter(district_name != "outside Sound Transit Distric") |>
  select(-"district_name") |>
  mutate(st_district = paste0("ST District ", st_district)) |>
  pivot_longer(!st_district, names_to = "year", values_to = "population") |>
  mutate(population = as.integer(round(population,-3)), year = as.integer(str_remove_all(year, "population_"))) |>
  pivot_wider(names_from = st_district, values_from = population)

population_summary <- left_join(county_summary, ptba_summary, by="year")
population_summary <- left_join(population_summary, st_summary, by="year")

write_csv(population_summary, population_csv_file)
