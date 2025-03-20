# Libraries -----------------------------------------------------------------
library(tidyverse)
library(sf)
library(units)
library(psrcelmer)

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Transportation/RTP_2026")
spatial_inputs_dir <- file.path(data_dir,"spatial-layers","input-layers")
spatial_outputs_dir <- file.path(data_dir,"spatial-layers","output-layers")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

gtfs_year <- 2024
gtfs_service <- "fall"
if (tolower(gtfs_service)=="spring") {gtfs_month = "05"} else (gtfs_month = "10")

# Input File Paths -------------------------------------------------------------
its_file <- file.path(spatial_inputs_dir, "ITS_Signals_2024_Final.shp")
hrn_file <- file.path(spatial_inputs_dir, "filtered_hrn.gpkg")
efa_file <- file.path(gis_dir, "equity_focus_areas", "efa_3groupings_1sd", "equity_focus_areas_2023.csv")
fgts_file <- file.path(gis_dir, "freight", "FGTSWA.gdb")

fgdb_file <- file.path(spatial_outputs_dir,"rtp_current_system.gdb")
congestion_fgdb_file <- file.path(gis_dir,"congestion","rtp_current_system_congestion.gdb")
its_fgdb_file <- file.path(gis_dir,"its_overlays","rtp_current_system_its_overlays.gdb")

tract_lyr <- "TRACT2020"

# Layers for Overlays ------------------------------------------------------------
print(str_glue("Reading ITS signal data from {its_file}"))
its <- read_sf(its_file) |> 
  st_transform(spn)

print(str_glue("Reading ITS signal data from {its_file} and creating 50' buffer for spatial intersections"))
its_buffer <- read_sf(its_file) |> 
  st_transform(spn) |> 
  st_buffer(dist = 50) |>
  select("OBJECTID")

print(str_glue("Loading Transit Route layer from {fgdb_file} and trimming to frequent transit"))
frequent_transit <- st_read(dsn = fgdb_file, layer = paste0("transit_routes_", gtfs_service, "_", gtfs_year)) |> 
  st_transform(spn) |>
  filter(frequent == 1) |> 
  select("shape_id", "route")

print(str_glue("Loading the Walk & Bike subset of the High Injury Network from {hrn_file}"))
high_injury_network <- read_sf(hrn_file) |> 
  st_transform(spn)

print(str_glue("Loading the roadway congestion layer from {congestion_fgdb_file} and trimming to PM Heavy & Severe COngestion"))
roadway_congestion <- st_read(dsn = congestion_fgdb_file, layer = "roadway_congestion_2025") |> 
  st_transform(spn) |>
  filter(pm_congestion %in% c("Heavy", "Severe")) |> 
  select("tmc", "pm_congestion")

print(str_glue("Loading the EFA file from {efa_file} and joining with the 2020 Census Tracts file"))
efa_tbl <- read_csv(efa_file, show_col_types = FALSE) |>
  select("GEOID20", "efa_poc", "efa_pov200", "efa_lep", "efa_youth", "efa_older", "efa_dis")

efa_tracts <- st_read_elmergeo(tract_lyr, project_to_wgs84 = FALSE) |> 
  select(GEOID20 = "geoid_nm")

efa_tracts <- left_join(efa_tracts, efa_tbl, by="GEOID20") |>
  st_transform(spn)

rm(efa_tbl)

print(str_glue("Loading the FGTS layer from {congestion_fgdb_file} and trimming to PSRC Region"))
fgts <- st_read(dsn = fgts_file, layer = "FGTSWA") |> 
  filter(CountyName %in% c("King", "Kitsap", "Pierce", "Snohomish")) |>
  st_transform(spn) |>
  select("RouteIdentifier","FGTSClass")

# Spatial Intersections ---------------------------------------------------
print(str_glue("Intersecting the ITS layer with Frequent Transit Routes"))
i <- st_intersection(its_buffer, frequent_transit) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`frequent_transit` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`frequent_transit` = replace_na(`frequent_transit`, "No"))

print(str_glue("Intersecting the ITS layer with the Walk & Bike subset of the High Injury Network"))
i <- st_intersection(its_buffer, high_injury_network) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`high_injury_network` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`high_injury_network` = replace_na(`high_injury_network`, "No"))

print(str_glue("Intersecting the ITS layer with the PM Peak Heavy and Severe Congestion"))
i <- st_intersection(its_buffer, roadway_congestion) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`pm_congestion` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`pm_congestion` = replace_na(`pm_congestion`, "No"))

print(str_glue("Intersecting the ITS layer with the FGTS network"))
i <- st_intersection(its_buffer, fgts) |>
  filter(FGTSClass %in% c("T-1", "T-2")) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`fgts_t1_t2` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`fgts_t1_t2` = replace_na(`fgts_t1_t2`, "No"))

print(str_glue("Intersecting the ITS layer with People of Color Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_poc >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_poc` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_poc` = replace_na(`efa_poc`, "No"))

print(str_glue("Intersecting the ITS layer with People with Limited Income Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_pov200 >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_pov` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_pov` = replace_na(`efa_pov`, "No"))

print(str_glue("Intersecting the ITS layer with People with Limited English Proficiency Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_lep >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_lep` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_lep` = replace_na(`efa_lep`, "No"))

print(str_glue("Intersecting the ITS layer with People with a Disability Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_dis >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_dis` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_dis` = replace_na(`efa_dis`, "No"))

print(str_glue("Intersecting the ITS layer with Youth Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_youth >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_yth` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_yth` = replace_na(`efa_yth`, "No"))

print(str_glue("Intersecting the ITS layer with Older Adults Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_older >0)) |>
  select("OBJECTID") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_old` = "Yes")

its <- left_join(its, i, by  ="OBJECTID") |>
  mutate(`efa_old` = replace_na(`efa_old`, "No"))

# Final Data Cleanup ------------------------------------------------------
st_write(its, dsn = its_fgdb_file, layer = "its_overlays_2024", append = FALSE)
