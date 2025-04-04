# Libraries -----------------------------------------------------------------
library(tidyverse)
library(sf)
library(units)
library(psrcelmer)

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/RTP Data & Analysis - Data")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Transportation/RTP_2026")
options(dplyr.summarise.inform = FALSE)

# Inputs ------------------------------------------------------------------
wgs84 <- 4326
spn <- 2285

buffer_dist <- 50
min_overlap <- 0

gtfs_year <- 2024
gtfs_service <- "fall"
if (tolower(gtfs_service)=="spring") {gtfs_month = "05"} else (gtfs_month = "10")

frequent_description <- "2024 Frequent Transit Network"
hin_description = "Regional High Injury Network"
hvy_description = "PM Heavy or Severe Congestion"
sev_description = "PM Severe Congestion"
poc_description = "People of Color Emphasis Area"
pov_description = "People with Limited Income Emphasis Area"
lep_description = "People with Limited English Emphasis Area"
dis_description = "People with a Disability Emphasis Area"
yth_description = "People uner 18 Emphasis Area"
old_description = "People over 65 Emphasis Area"
fgts_hin_description = "High Injury Network overlay with the FGTS Network"

# Input File Paths -------------------------------------------------------------
efa_file <- file.path(gis_dir, "equity_focus_areas", "efa_3groupings_1sd", "equity_focus_areas_2023.csv")
fgts_file <- file.path(gis_dir, "freight", "FGTSWA.gdb")
congestion_fgdb_file <- file.path(gis_dir,"congestion","rtp_current_system_congestion.gdb")
safety_fgdb_file <- file.path(gis_dir,"safety","high_injury_networks_2024.gdb")
transit_fgdb_file <- file.path(gis_dir,"transit","Transit_Network_2024.gdb")
freight_csv_file <- file.path(gis_dir,"freight","rtp_current_system_freight_overlays.csv")

tract_lyr <- "TRACT2020"

freight_layers_file <- file.path(gis_dir,"freight", "interim-overlay-layers")

# Functions ---------------------------------------------------------------
spatial_intersections <- function(lyr, lyr_name, buffer, buffer_name, buffer_description, fgb_file = freight_layers_file) {
  
  print(str_glue("Intersecting the {lyr_name} network with the {buffer_name} network"))
  i <- suppressWarnings(st_intersection(lyr, buffer))
  
  print(str_glue("Calculating the length of the {lyr_name} network that intersects with the {buffer_name} network"))
  i <- i |>
    mutate(len = as.numeric(set_units(st_length(i), "mi"))) |>
    filter(len >= (min_overlap/5280))
  
  i <- i |>
    select("index", "road", "cat", "len") |>
    mutate(!!buffer_name := buffer_description) |>
    arrange(index, desc(len)) |>
    distinct(index, .keep_all = TRUE)
  
  print(str_glue("Converting the {lyr_name} and {buffer_name} intersected network to a multilinestring"))
  i <- suppressWarnings(st_cast(i, "MULTILINESTRING"))
  
  print(str_glue("Writing the {lyr_name} and {buffer_name} intersected network to the {fgb_file} folder"))
  st_write(i, dsn = file.path(fgb_file, paste0(lyr_name, "_", buffer_name, ".shp")), append = FALSE)
  
  var_name <- paste0("Miles on ", str_to_title(str_replace_all(buffer_name, "_", " ")))
  
  t <- i |>
    st_drop_geometry() |>
    group_by(cat) |>
    summarise(!!var_name := round(sum(len),0)) |>
    as_tibble()
  
  r <- t |>
    mutate(cat = "Totals") |>
    group_by(cat) |>
    summarise(!!var_name := round(sum(.data[[var_name]]),0)) |>
    as_tibble()
  
  t <- bind_rows(t,r)
  
  return(t)
  
}

# Layers for Freight Overlays ------------------------------------------------------------
print(str_glue("Loading the FGTS layer from {congestion_fgdb_file} and trimming to PSRC Region"))
fgts <- st_read(dsn = fgts_file, layer = "FGTSWA") |> 
  filter(CountyName %in% c("King", "Kitsap", "Pierce", "Snohomish")) |>
  st_transform(spn) |>
  select(road = "RouteIdentifier", cat = "FGTSClass") |>
  rowid_to_column("index")

fgts <- fgts |>
  mutate(len = as.numeric(set_units(st_length(fgts), "mi")))

print(str_glue("Loading Transit Route layer from {transit_fgdb_file} and trimming to frequent transit"))
frequent_transit_buffer <- st_read(dsn = transit_fgdb_file, layer = paste0("transit_routes_", gtfs_service, "_", gtfs_year)) |> 
  st_transform(spn) |>
  filter(frequent == 1 & type %in% c("Bus", "BRT", "ST Express")) |> 
  select("frequent")

print(str_glue("Creating frequent transit route dissolved layer with a buffer distance of {buffer_dist} feet"))
frequent_transit_buffer <- frequent_transit_buffer |>
  st_buffer(dist = buffer_dist) |>
  group_by(frequent) |>
  summarise(SHAPE = st_union(SHAPE), .groups = "drop")  

print(str_glue("Loading the High Injury Network from {safety_fgdb_file}"))
high_injury_network_buffer <- st_read(dsn = safety_fgdb_file, layer = "high_injury_network_2024") |> 
  st_transform(spn) |>
  mutate(hin = 1) |>
  select("hin") |>
  st_buffer(dist = buffer_dist)

print(str_glue("Loading the roadway congestion layer from {congestion_fgdb_file} and trimming to PM Heavy & Severe COngestion"))
roadway_congestion_buffer <- st_read(dsn = congestion_fgdb_file, layer = "roadway_congestion_2025") |> 
  st_transform(spn) |>
  filter(pm_congestion %in% c("Heavy", "Severe")) |> 
  select("tmc", "pm_congestion") |>
  st_buffer(dist = buffer_dist)

print(str_glue("Loading the EFA file from {efa_file} and joining with the 2020 Census Tracts file"))
efa_tbl <- read_csv(efa_file, show_col_types = FALSE) |>
  select("GEOID20", "efa_poc", "efa_pov200", "efa_lep", "efa_youth", "efa_older", "efa_dis")

efa_tracts <- st_read_elmergeo(tract_lyr, project_to_wgs84 = FALSE) |> 
  select(GEOID20 = "geoid_nm")

efa_tracts <- left_join(efa_tracts, efa_tbl, by="GEOID20") |>
  st_transform(spn)

rm(efa_tbl)

fgts_summary <- fgts |>
  st_drop_geometry() |>
  group_by(cat) |>
  summarise(`Total Centerline Miles` = round(sum(len),0)) |>
  as_tibble() 

t <- fgts_summary |>
  mutate(cat = "Totals") |>
  group_by(cat) |>
  summarise(`Total Centerline Miles` = round(sum(`Total Centerline Miles`),0)) |>
  as_tibble()

fgts_summary <- bind_rows(fgts_summary, t)
rm(t)

# Layers for High Injury Network Overlays ---------------------------------
print(str_glue("Loading the High Injury Network from {safety_fgdb_file} for spatial analysis"))
high_injury_network <- st_read(dsn = safety_fgdb_file, layer = "high_injury_network_2024") |> 
  st_transform(spn) |>
  select(road = "street") |>
  mutate(cat = "Regional High Injury Network") |>
  rowid_to_column("index")

high_injury_network <- high_injury_network |>
  mutate(len = as.numeric(set_units(st_length(high_injury_network), "mi")))

fgts_buffer <- fgts |>
  st_buffer(dist = buffer_dist)

hin_summary <- high_injury_network |>
  st_drop_geometry() |>
  group_by(cat) |>
  summarise(`Total Centerline Miles` = round(sum(len),0)) |>
  as_tibble()

# FGTS & Frequent Transit ---------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = frequent_transit_buffer, lyr_name = "fgts", buffer_name = "transit", buffer_description = frequent_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Transit` = round(`Miles on Transit` / `Total Centerline Miles`,3))
rm(l)

# FGTS & High Injury Network -----------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = high_injury_network_buffer, lyr_name = "fgts", buffer_name = "hin", buffer_description = hin_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Hin` = round(`Miles on Hin` / `Total Centerline Miles`,3))
rm(l)

# FGTS & Heavy & Severe Congestion -------------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = roadway_congestion_buffer |> filter(pm_congestion %in% c("Heavy","Severe")), lyr_name = "fgts", buffer_name = "hvy_cong", buffer_description = hvy_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Hvy Cong` = round(`Miles on Hvy Cong` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & Severe Congestion -------------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = roadway_congestion_buffer |> filter(pm_congestion %in% c("Severe")), lyr_name = "fgts", buffer_name = "sev_cong", buffer_description = sev_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Sev Cong` = round(`Miles on Sev Cong` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People of Color ---------------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_poc >0), lyr_name = "fgts", buffer_name = "poc", buffer_description = poc_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Poc` = round(`Miles on Poc` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People with Limited Incomes ---------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_pov200 >0), lyr_name = "fgts", buffer_name = "pov", buffer_description = pov_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Pov` = round(`Miles on Pov` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People with Limited English ---------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_lep >0), lyr_name = "fgts", buffer_name = "lep", buffer_description = lep_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Lep` = round(`Miles on Lep` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People with a Disability ------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_dis >0), lyr_name = "fgts", buffer_name = "dis", buffer_description = dis_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Dis` = round(`Miles on Dis` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People under 18 ---------------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_youth >0), lyr_name = "fgts", buffer_name = "yth", buffer_description = yth_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Yth` = round(`Miles on Yth` / `Total Centerline Miles`, 3))
rm(l)

# FGTS & People over 65 ----------------------------------------------------------
l <- spatial_intersections(lyr = fgts, buffer = efa_tracts |> filter(efa_older >0), lyr_name = "fgts", buffer_name = "old", buffer_description = old_description)
fgts_summary <- left_join(fgts_summary, l, by="cat") |>
  mutate(`% on Old` = round(`Miles on Old` / `Total Centerline Miles`, 3))
rm(l)

# FGTS overlay table to a csv -------------------------------------------
print(str_glue("Writing the Freight summary tables to {freight_csv_file}"))
write_csv(fgts_summary, freight_csv_file)

# High Injury Network & FGTS -----------------------------------------------------
print(str_glue("Intersecting the High Injury Network with the FGTS network"))
i <- suppressWarnings(st_intersection(high_injury_network, fgts_buffer))

i <- i |> select("index", "road", cat = "cat.1")
i <- i |>  mutate(len = as.numeric(set_units(st_length(i), "mi"))) 
i <- i |> 
  mutate(id = paste0(index,"_",cat)) |>
  arrange(id, desc(len)) |> 
  distinct(id, .keep_all = TRUE)

i <- suppressWarnings(st_cast(i, "MULTILINESTRING"))
st_write(i, dsn = file.path(freight_layers_file, paste0("hin_fgts.shp")), append = FALSE)

var_name <- paste0("Miles on ", str_to_title(str_replace_all(buffer_name, "_", " ")))

t <- i |>
  st_drop_geometry() |>
  group_by(cat) |>
  summarise(`Miles of FGTS` = round(sum(len),0)) |>
  as_tibble()

r <- t |>
  mutate(cat = "Totals") |>
  group_by(cat) |>
  summarise(!!var_name := round(sum(.data[[var_name]]),0)) |>
  as_tibble()

t <- bind_rows(t,r)




l <- spatial_intersections(lyr = high_injury_network, buffer = fgts_buffer, lyr_name = "hin", buffer_name = "fgts", fgb_file = freight_layers_file, buffer_description = "FGTS Network")
hin_summary <- left_join(hin_summary, l |> filter(cat != "Totals"), by="cat") |>
  mutate(`% on Fgts` = `Miles on Fgts` / round(`Total Centerline Miles`, 3))
rm(l)
