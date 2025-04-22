# Libraries -----------------------------------------------------------------
library(tidyverse)
library(sf)
library(units)
library(psrcelmer)

un <- Sys.getenv("USERNAME")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Transportation/RTP_2026")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

gtfs_year <- 2024
gtfs_service <- "fall"
if (tolower(gtfs_service)=="spring") {gtfs_month = "05"} else (gtfs_month = "10")

buffer_dist <- 0.25

local_transit_density <- 7
all_day_transit_density <- 15
frequent_transit_density <- 25
hct_transit_density <- 40

# Functions ---------------------------------------------------------------
transit_overlay_calc <- function(density_lvl, transit_type, hex_file, buffer_file, efa) {
  
  if(transit_type == "Local Transit") {transit_attr <- "min_routes"}
  if(transit_type == "All Day Transit") {transit_attr <- "all_day"}
  if(transit_type == "Frequent Transit") {transit_attr <- "frequent"}
  if(transit_type == "High Capacity Transit") {transit_attr <- "hct"}
  
  print(str_glue("Hex grids with a density greater than or equal to {density_lvl} activity units per acre"))
  if (length(hex_file |> filter(density >= density_lvl) |> select("id") |> pull()) >0) {
    
    s <- hex_file |> 
      filter(density >= density_lvl) |>
      select("id", "population", "jobs", "activity_units", "density") |>
      st_drop_geometry() |>
      distinct() |>
      mutate(density_threshold = transit_type) |>
      group_by(density_threshold) |>
      summarize(population = sum(population), jobs = sum(jobs), activity_units = sum(activity_units)) |>
      as_tibble() |>
      mutate(Variable = paste0("Total people and jobs in areas with a density that supports ", str_to_lower(transit_type))) |>
      mutate(`Equity Focus Area` = efa)
    
    total_people_in_transit_hex <- s |> select("population") |> pull()
    total_jobs_in_transit_hex <- s |> select("jobs") |> pull()
    total_activity_units_in_transit_hex <- s |> select("activity_units") |> pull()
    
  } else {
    
    s <- as_tibble(data.frame(density_threshold = transit_type, population=0, jobs=0, activity_units=0, 
                    Variable = paste0("Total people and jobs in areas with a density that supports ", str_to_lower(transit_type)), 
                    `Equity Focus Area` = efa)) |>
      rename(`Equity Focus Area` = "Equity.Focus.Area")
    total_people_in_transit_hex <- s |> select("population") |> pull()
    total_jobs_in_transit_hex <- s |> select("jobs") |> pull()
    total_activity_units_in_transit_hex <- s |> select("activity_units") |> pull()
  }
  
  print(str_glue("Hex grids with a density less than {density_lvl} activity units per acre"))
  ns <- hex_file |> 
    filter(density < density_lvl) |>
    select("id", "population", "jobs", "activity_units", "density") |>
    st_drop_geometry() |>
    distinct() |>
    mutate(density_threshold = transit_type) |>
    group_by(density_threshold) |>
    summarize(population = sum(population), jobs = sum(jobs), activity_units = sum(activity_units)) |>
    as_tibble() |>
    mutate(Variable = paste0("Total people and jobs in areas with a density that does not support ", str_to_lower(transit_type))) |>
    mutate(`Equity Focus Area` = efa)
  
  total_people_not_in_transit_hex <- ns |> select("population") |> pull()
  total_jobs_not_in_transit_hex <- ns |> select("jobs") |> pull()
  total_activity_units_not_in_transit_hex <- ns |> select("activity_units") |> pull()
  
  total_people <- total_people_in_transit_hex + total_people_not_in_transit_hex
  total_jobs <- total_jobs_in_transit_hex + total_jobs_not_in_transit_hex
  total_activity_units <- total_activity_units_in_transit_hex + total_activity_units_not_in_transit_hex
  
  print(str_glue("Adding percentages to hex calculations for supportive Hexs"))
  s <- s |>
    mutate(`% Population` = population / total_people) |>
    mutate(`% Jobs` = jobs / total_jobs) |>
    mutate(`% Activity Units` = activity_units / total_activity_units) |>
    select(`Transit Type` = "density_threshold", 
           Population = "population", "% Population",
           Jobs = "jobs", "% Jobs",
           `Activity Units` = "activity_units", "% Activity Units",
           "Variable", "Equity Focus Area")
  
  print(str_glue("Adding percentages to hex calculations for non-supportive Hexs"))
  ns <- ns |>
    mutate(`% Population` = population / total_people) |>
    mutate(`% Jobs` = jobs / total_jobs) |>
    mutate(`% Activity Units` = activity_units / total_activity_units) |>
    select(`Transit Type` = "density_threshold", 
           Population = "population", "% Population",
           Jobs = "jobs", "% Jobs",
           `Activity Units` = "activity_units", "% Activity Units",
           "Variable", "Equity Focus Area")
  
  print(str_glue("Intersecting {transit_type} buffers with hex grids with a density greater than or equal to {density_lvl}"))
  if (length(suppressWarnings(st_intersection(hex_file |> filter(density >= density_lvl), buffer_file |> filter(.data[[transit_attr]] == 1))) |> select("id") |> pull()) >0) {
  
      i <- suppressWarnings(st_intersection(hex_file |> filter(density >= density_lvl), buffer_file |> filter(.data[[transit_attr]] == 1))) |>
        select("id", "population", "jobs", "activity_units", "density") |>
        st_drop_geometry() |>
        distinct() |>
        mutate(density_threshold = transit_type) |>
        group_by(density_threshold) |>
        summarize(population = sum(population), jobs = sum(jobs), activity_units = sum(activity_units)) |>
        as_tibble() |>
        mutate(Variable = paste0("Total people and jobs in areas with a density that supports ", str_to_lower(transit_type), " that are near ", str_to_lower(transit_type))) |>
        mutate(`Equity Focus Area` = efa)
  
      total_people_near_transit <- i |> select("population") |> pull()
      total_jobs_near_transit <- i |> select("jobs") |> pull()
      total_activity_units_near_transit <- i |> select("activity_units") |> pull()
    
  } else {
    
    i <- as_tibble(data.frame(density_threshold = transit_type, population=0, jobs=0, activity_units=0, 
                              Variable = paste0("Total people and jobs in areas with a density that supports ", str_to_lower(transit_type), " that are near ", str_to_lower(transit_type)), 
                              `Equity Focus Area` = efa)) |>
      rename(`Equity Focus Area` = "Equity.Focus.Area")
    
    total_people_near_transit <- i |> select("population") |> pull()
    total_jobs_near_transit <- i |> select("jobs") |> pull()
    total_activity_units_near_transit <- i |> select("activity_units") |> pull()
    
  }
  
  print(str_glue("Adding percentages to hex calculations for supportive Hexs with service"))
  i <- i |>
    mutate(`% Population` = population / total_people_in_transit_hex) |>
    mutate(`% Jobs` = jobs / total_jobs_in_transit_hex) |>
    mutate(`% Activity Units` = activity_units / total_activity_units_in_transit_hex) |>
    select(`Transit Type` = "density_threshold", 
           Population = "population", "% Population",
           Jobs = "jobs", "% Jobs",
           `Activity Units` = "activity_units", "% Activity Units",
           "Variable", "Equity Focus Area")
  
  print(str_glue("Adding percentages to hex calculations for supportive Hexs without service"))
  ni <- i |>
    mutate(Population = total_people_in_transit_hex - total_people_near_transit) |>
    mutate(Jobs= total_jobs_in_transit_hex - total_jobs_near_transit) |>
    mutate(`Activity Units` = total_activity_units_in_transit_hex - total_activity_units_near_transit) |>
    mutate(`% Population` = Population / total_people_in_transit_hex) |>
    mutate(`% Jobs` = Jobs / total_jobs_in_transit_hex) |>
    mutate(`% Activity Units` = `Activity Units` / total_activity_units_in_transit_hex) |>
    mutate(Variable = paste0("Total people and jobs in areas with a density that supports ", str_to_lower(transit_type), " that are not near ", str_to_lower(transit_type))) |>
    mutate(`Equity Focus Area` = efa) |>
    select("Transit Type", "Population", "% Population", "Jobs", "% Jobs", "Activity Units", "% Activity Units", "Variable", "Equity Focus Area")
  
  summary_data <- bind_rows(s, ns, i, ni) |>
    mutate(`% Population` = replace_na(`% Population`, 0)) |>
    mutate(`% Jobs` = replace_na(`% Jobs`, 0)) |>
    mutate(`% Activity Units` = replace_na(`% Activity Units`, 0))
  
  return(summary_data)
  
}

# Input File Paths -------------------------------------------------------------
efa_file <- file.path(gis_dir, "equity_focus_areas", "efa_3groupings_1sd", "equity_focus_areas_2023.csv")
transit_fgdb_file <- file.path(gis_dir,"transit","Transit_Network_2024.gdb")
safety_fgdb_file <- file.path(gis_dir,"safety","high_injury_networks_2024.gdb")
density_fgdb_file <- file.path(gis_dir,"activity_units","Activity_Units_2026_RTP.gdb")

tract_lyr <- "TRACT2020"
county_lyr <- "COUNTY_BACKGROUND"
rgc_lyr <- "URBAN_CENTERS"
mic_lyr <- "MICEN"
regeo_lyr <- "REGIONAL_GEOGRAPHIES"

transit_csv_file <- file.path(gis_dir,"transit","rtp_current_system_transit_typology_summary.csv")

# Layers for Overlays ------------------------------------------------------------
print(str_glue("Loading Transit Route layer from {transit_fgdb_file}"))
transit_buffers <- st_read(dsn = transit_fgdb_file, layer = paste0("transit_stops_", gtfs_service, "_", gtfs_year)) |> 
  st_transform(spn) |>
  st_buffer(dist = buffer_dist*5280)

print(str_glue("Loading the EFA file from {efa_file} and joining with the 2020 Census Tracts file"))
efa_tbl <- read_csv(efa_file, show_col_types = FALSE) |>
  select("GEOID20", "efa_poc", "efa_pov200", "efa_lep", "efa_youth", "efa_older", "efa_dis")

efa_tracts <- st_read_elmergeo(tract_lyr, project_to_wgs84 = FALSE) |> 
  select(GEOID20 = "geoid_nm")

efa_tracts <- left_join(efa_tracts, efa_tbl, by="GEOID20") |>
  st_transform(spn)

rm(efa_tbl)

print(str_glue("Loading the Walk & Bike subset of the High Injury Network from {safety_fgdb_file}"))
high_injury_network <- st_read(dsn = safety_fgdb_file, layer = "high_injury_network_nonmotorized_subset_2024") |> 
  st_transform(spn)

print(str_glue("Loading the Activity Units density data from {density_fgdb_file}"))
activity_units <- st_read(dsn = density_fgdb_file, layer = "peope_and_jobs_2024") |> 
  st_transform(spn) |>
  select(id = "GRID_ID", population = "sum_pop_20", jobs = "sum_jobs_2", activity_units = "sum_au_202", density = "au_acre")

print(str_glue("Loading the County layer from {county_lyr}"))
county <- st_read_elmergeo(county_lyr, project_to_wgs84 = FALSE) |> 
  filter(psrc == 1) |>
  select(county = "county_nm")

print(str_glue("Loading the Regional Geography layer from {regeo_lyr}"))
rgeo <- st_read_elmergeo(regeo_lyr, project_to_wgs84 = FALSE) |> 
  select(regeo = "class_desc")

print(str_glue("Loading the Regional Growth Center layer from {rgc_lyr}"))
rgc <- st_read_elmergeo(rgc_lyr, project_to_wgs84 = FALSE) |> 
  select(rgc = "category")

print(str_glue("Loading the Manufacturing & Industrial Center layer from {mic_lyr}"))
mic <- st_read_elmergeo(mic_lyr, project_to_wgs84 = FALSE) |> 
  select(mic = "category")

# Transit Overlays: Region Population ---------------------------------------------------
local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units, buffer_file = transit_buffers, efa = "Region Population")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units, buffer_file = transit_buffers, efa = "Region Population")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units, buffer_file = transit_buffers, efa = "Region Population")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units, buffer_file = transit_buffers, efa = "Region Population")
transit_overlay_data <- bind_rows(local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People of Color ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_poc >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People of Color")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People of Color")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People of Color")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People of Color")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People with Lower Incomes ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_pov200 >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Lower Incomes")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Lower Incomes")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Lower Incomes")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Lower Incomes")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People with Limited English ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_lep >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Limited English")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Limited English")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Limited English")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with Limited English")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People with a Disability ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_dis >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with a Disability")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with a Disability")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with a Disability")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People with a Disability")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People under 18 ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_youth >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People under 18")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People under 18")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People under 18")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People under 18")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: People over 65 ---------------------------------------
efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, efa_tracts |> filter(efa_older >=1))) |>
  st_drop_geometry() |>
  select("id") |>
  distinct() |>
  pull()

local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People over 65")
all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People over 65")
frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People over 65")
high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = "People over 65")
transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)

# Transit Overlays: County ---------------------------------------
for (county_name in unique(county$county)) {
  
  efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, county |> filter(county == county_name))) |>
    st_drop_geometry() |>
    select("id") |>
    distinct() |>
    pull()
  
  local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(county_name, " County"))
  all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(county_name, " County"))
  frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(county_name, " County"))
  high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(county_name, " County"))
  transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  
}

# Transit Overlays: Regional Geography ---------------------------------------
for (regeo_name in unique(rgeo$regeo)) {
  
  efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, rgeo |> filter(regeo == regeo_name))) |>
    st_drop_geometry() |>
    select("id") |>
    distinct() |>
    pull()
  
  local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(regeo_name))
  all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(regeo_name))
  frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(regeo_name))
  high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(regeo_name))
  transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  
}  

# Transit Overlays: Regional Growth Centers ---------------------------------------
for (rgc_name in unique(rgc$rgc)) {
  
  efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, rgc |> filter(rgc == rgc_name))) |>
    st_drop_geometry() |>
    select("id") |>
    distinct() |>
    pull()
  
  local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(rgc_name, " Regional Growth Centers"))
  all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(rgc_name, " Regional Growth Centers"))
  frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(rgc_name, " Regional Growth Centers"))
  high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(rgc_name, " Regional Growth Centers"))
  transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  
}  

# Transit Overlays: Manufacturing & Industrial Centers ---------------------------------------
for (mic_name in unique(mic$mic)) {
  
  efa_overlay_hexes <- suppressWarnings(st_intersection(activity_units, mic |> filter(mic == mic_name))) |>
    st_drop_geometry() |>
    select("id") |>
    distinct() |>
    pull()
  
  local_transit <- transit_overlay_calc(density_lvl = local_transit_density, transit_type = "Local Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(mic_name, " Manufacturing & Industrial Centers"))
  all_day_transit <- transit_overlay_calc(density_lvl = all_day_transit_density, transit_type = "All Day Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(mic_name, " Manufacturing & Industrial Centers"))
  frequent_transit <- transit_overlay_calc(density_lvl = frequent_transit_density, transit_type = "Frequent Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(mic_name, " Manufacturing & Industrial Centers"))
  high_capacity_transit <- transit_overlay_calc(density_lvl = hct_transit_density, transit_type = "High Capacity Transit", hex_file = activity_units |> filter(id %in% efa_overlay_hexes), buffer_file = transit_buffers, efa = paste0(mic_name, " Manufacturing & Industrial Centers"))
  transit_overlay_data <- bind_rows(transit_overlay_data, local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  rm(local_transit, all_day_transit, frequent_transit, high_capacity_transit)
  
}  

# Final Service Summary ---------------------------------------------------
print(str_glue("Writing the Transit summary tables to {transit_csv_file}"))
write_csv(transit_overlay_data, transit_csv_file)
