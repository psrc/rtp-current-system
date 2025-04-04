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

type_order <- c("Local", "State", "Total")
condition_order <- c("Poor", "Fair", "Good", "Total")
overlay_order <- c("Yes", "No")

# Input File Paths -------------------------------------------------------------
efa_file <- file.path(gis_dir, "equity_focus_areas", "efa_3groupings_1sd", "equity_focus_areas_2023.csv")
fgts_file <- file.path(gis_dir, "freight", "FGTSWA.gdb")
transit_file <- file.path(gis_dir,"transit","Transit_Network_2024.gdb")
congestion_fgdb_file <- file.path(gis_dir,"congestion","rtp_current_system_congestion.gdb")
safety_fgdb_file <- file.path(gis_dir,"safety","high_injury_networks_2024.gdb")
local_bridge_file <- file.path(gis_dir, "bridge", "local_bridges_2023.csv")
state_bridge_file <- file.path(gis_dir, "bridge", "state_bridges_2023.csv")
bridge_csv_file <- file.path(gis_dir,"bridge","rtp_current_system_bridge_overlays.csv")

tract_lyr <- "TRACT2020"
local_bridge_lyr <- "BRIDGES_LOCAL_2020"
state_bridge_lyr <- "BRIDGES_STATE_2020"

bridge_fdb <- file.path(gis_dir, "bridge", "bridge_conditons.gdb")

# Layers for Overlays ------------------------------------------------------------

print(str_glue("Loading Transit Route layer from {transit_file} and trimming to frequent transit"))
frequent_transit <- st_read(dsn = transit_file, layer = paste0("transit_routes_", gtfs_service, "_", gtfs_year)) |> 
  st_transform(spn) |>
  filter(frequent == 1) |> 
  select("shape_id", "route")

print(str_glue("Loading the High Injury Network from {safety_fgdb_file}"))
high_injury_network <- st_read(dsn = safety_fgdb_file, layer = "high_injury_network_2024") |> 
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

print(str_glue("Loading the local bridge layer from {local_bridge_lyr} in ElmerGeo"))
local_bridges <- st_read_elmergeo(local_bridge_lyr, project_to_wgs84 = FALSE) |> 
  select(bridge_id = "structure_", "bridge_no", bridge_name = "bridge_nam", condition_2020 = "map_21_goo") |>
  st_transform(spn) |>
  mutate(type = "Local")

local_bridges_tbl <- read_csv(local_bridge_file, show_col_types = FALSE) |>
  mutate(bridge_id = str_pad(bridge_id, width = 8, side = c("left"),  pad=0))
local_bridges <- left_join(local_bridges, local_bridges_tbl, by="bridge_id") |>
  mutate(bridge_condition = case_when(
    is.na(condition_2023) ~ condition_2020,
    !(is.na(condition_2023)) ~ condition_2023))

print(str_glue("Loading the state bridge layer from {state_bridge_lyr} in ElmerGeo"))
state_bridges <- st_read_elmergeo(state_bridge_lyr, project_to_wgs84 = FALSE) |> 
  select(bridge_id = "key_struct", "bridge_no", bridge_name = "bridge_nam", condition_2020 = "map21_gfp") |>
  st_transform(spn) |>
  mutate(type = "State") |>
  filter(bridge_id != "1")

state_bridges_tbl <- read_csv(state_bridge_file, show_col_types = FALSE)
state_bridges <- left_join(state_bridges, state_bridges_tbl, by="bridge_id") |>
  mutate(bridge_condition = case_when(
    is.na(condition_2023) ~ condition_2020,
    !(is.na(condition_2023)) ~ condition_2023))

print(str_glue("Combining Local and State bridges into one layer"))
bridges <- bind_rows(local_bridges, state_bridges) |>
  select("bridge_id", "bridge_no", "bridge_name", "type", "condition_2020", "condition_2023", "bridge_condition")

print(str_glue("Creating 50' buffer around bridge points for spatial intersections"))
bridge_buffer <- bridges |>
  st_buffer(dist = 50) |>
  select("bridge_id")

rm(local_bridges, local_bridges_tbl, state_bridges, state_bridges_tbl)

# Spatial Intersections ---------------------------------------------------
print(str_glue("Intersecting the Bridge layer with Frequent Transit Routes"))
i <- suppressWarnings(st_intersection(bridge_buffer, frequent_transit)) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`frequent_transit` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`frequent_transit` = replace_na(`frequent_transit`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with the High Injury Network"))
i <- suppressWarnings(st_intersection(bridge_buffer, high_injury_network)) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`high_injury_network` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`high_injury_network` = replace_na(`high_injury_network`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with the PM Peak Severe Congestion"))
i <- suppressWarnings(st_intersection(bridge_buffer, roadway_congestion |> filter(pm_congestion == "Severe"))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`pm_severe_congestion` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`pm_severe_congestion` = replace_na(`pm_severe_congestion`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with the PM Peak Heavy & Severe Congestion"))
i <- suppressWarnings(st_intersection(bridge_buffer, roadway_congestion)) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`pm_heavy_severe_congestion` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`pm_heavy_severe_congestion` = replace_na(`pm_heavy_severe_congestion`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with the FGTS network"))
i <- suppressWarnings(st_intersection(bridge_buffer, fgts)) |>
  select("bridge_id", fgts = "FGTSClass") |>
  st_drop_geometry() |>
  distinct() |>
  arrange(bridge_id, fgts) |>
  distinct(bridge_id, .keep_all = TRUE)

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`fgts` = replace_na(`fgts`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with People of Color Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_poc >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_poc` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_poc` = replace_na(`efa_poc`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with People with Limited Income Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_pov200 >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_pov` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_pov` = replace_na(`efa_pov`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with People with Limited English Proficiency Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_lep >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_lep` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_lep` = replace_na(`efa_lep`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with People with a Disability Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_dis >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_dis` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_dis` = replace_na(`efa_dis`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with Youth Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_youth >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_yth` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_yth` = replace_na(`efa_yth`, "No"))
rm(i)

print(str_glue("Intersecting the Bridge layer with Older Adults Equity Focus Areas"))
i <- suppressWarnings(st_intersection(bridge_buffer, efa_tracts |> filter(efa_older >0))) |>
  select("bridge_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_old` = "Yes")

bridges <- left_join(bridges, i, by  ="bridge_id") |>
  mutate(`efa_old` = replace_na(`efa_old`, "No"))
rm(i)

bridges <- suppressWarnings(st_cast(bridges, "POINT"))

# Write Bridge overlays to the filegdb ------------------------------------------------------
print(str_glue("Writing the Bridge overlay layer to {bridge_fdb}"))
st_write(bridges, dsn = bridge_fdb, layer = "all_bridge_conditions_2023", append = FALSE)

# All Bridge Summary Table ----------------------------------------------------------
print(str_glue("Creating Bridge summary tables"))
bridge_tbl <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(type, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- bridge_tbl |>
  mutate(type = "Total") |>
  group_by(type, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

bridge_tbl <- bind_rows(bridge_tbl, r) 
rm(r)

r <- bridge_tbl |>
  mutate(bridge_condition = "Total") |>
  group_by(type, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

bridge_tbl <- bind_rows(bridge_tbl, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  arrange(type, bridge_condition)
rm(r)

bridge_tbl <- bridge_tbl |>
  pivot_wider(names_from = bridge_condition, values_from = Bridges) |>
  mutate(Metric = "All PSRC Bridges") |>
  rename(Type = "type")

# Frequent Transit Bridge Summary Table ----------------------------------------------------------
transit <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "frequent_transit") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- transit |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

transit <- bind_rows(transit, r) 
rm(r)

r <- transit |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

transit <- bind_rows(transit, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

transit <- transit |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "Frequent Transit Routes") |>
  rename(Type = "type")

# HIN Bridge Summary Table ----------------------------------------------------------
hin <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "high_injury_network") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- hin |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

hin <- bind_rows(hin, r) 
rm(r)

r <- hin |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

hin <- bind_rows(hin, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

hin <- hin |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "High Injury Network") |>
  rename(Type = "type")

# FGTS Bridge Summary Table ----------------------------------------------------------
freight <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  mutate(fgts = case_when(
    fgts %in% c("T-1", "T-2") ~ "Yes",
    fgts %in% c("T-3", "T-4", "T-5", "No") ~ "No")) |>
  rename(overlay = "fgts") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- freight |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

freight <- bind_rows(freight, r) 
rm(r)

r <- freight |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

freight <- bind_rows(freight, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

freight <- freight |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "FGTS T1 & T2") |>
  rename(Type = "type")

# Severe Congestion Bridge Summary Table ----------------------------------------------------------
severe <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "pm_severe_congestion") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- severe |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

severe <- bind_rows(severe, r) 
rm(r)

r <- severe |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

severe <- bind_rows(severe, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

severe <- severe |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "PM Peak Severe Congestion") |>
  rename(Type = "type")

# Heavy and Severe Congestion Bridge Summary Table ----------------------------------------------------------
heavy <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "pm_heavy_severe_congestion") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- heavy |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

heavy <- bind_rows(heavy, r) 
rm(r)

r <- heavy |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

heavy <- bind_rows(heavy, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

heavy <- heavy |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "PM Peak Heavy & Severe Congestion") |>
  rename(Type = "type")

# People of Color Bridge Summary Table ----------------------------------------------------------
poc <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_poc") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- poc |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

poc <- bind_rows(poc, r) 
rm(r)

r <- poc |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

poc <- bind_rows(poc, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

poc <- poc |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People of Color") |>
  rename(Type = "type")

# People with Limited Incomes Bridge Summary Table ----------------------------------------------------------
pov <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_pov") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- pov |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

pov <- bind_rows(pov, r) 
rm(r)

r <- pov |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

pov <- bind_rows(pov, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

pov <- pov |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People with Limited Incomes") |>
  rename(Type = "type")

# People with Limited English Bridge Summary Table ----------------------------------------------------------
lep <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_lep") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- lep |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

lep <- bind_rows(lep, r) 
rm(r)

r <- lep |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

lep <- bind_rows(lep, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

lep <- lep |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People with Limited English") |>
  rename(Type = "type")

# People with a Disability Bridge Summary Table ----------------------------------------------------------
dis <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_dis") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- dis |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

dis <- bind_rows(dis, r) 
rm(r)

r <- dis |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

dis <- bind_rows(dis, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

dis <- dis |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People with a Disability") |>
  rename(Type = "type")

# People under 18 Bridge Summary Table ----------------------------------------------------------
yth <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_yth") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- yth |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

yth <- bind_rows(yth, r) 
rm(r)

r <- yth |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

yth <- bind_rows(yth, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

yth <- yth |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People under 18") |>
  rename(Type = "type")

# People over 65 Bridge Summary Table ----------------------------------------------------------
old <- bridges |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  rename(overlay = "efa_old") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(count)) |>
  as_tibble()

r <- old |>
  mutate(type = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

old <- bind_rows(old, r) 
rm(r)

r <- old |>
  mutate(bridge_condition = "Total") |>
  group_by(type, overlay, bridge_condition) |>
  summarise(`Bridges` = sum(`Bridges`)) |>
  as_tibble()

old <- bind_rows(old, r) |>
  mutate(type = factor(type, levels = type_order)) |>
  mutate(bridge_condition = factor(bridge_condition, levels = condition_order)) |>
  mutate(overlay = factor(overlay, levels = overlay_order)) |>
  arrange(type, overlay, bridge_condition)
rm(r)

old <- old |>
  pivot_wider(names_from = c(overlay, bridge_condition), values_from = Bridges, names_sep = ": ") |>
  mutate(Metric = "People over 65") |>
  rename(Type = "type")

# Write overlay table to a csv -------------------------------------------
print(str_glue("Writing the ITS summary tables to {bridge_csv_file}"))
overlay_tbl <- bind_rows(transit, hin, freight, heavy, severe,
                         poc, pov, lep, dis, yth, old)

write_csv(overlay_tbl, bridge_csv_file)
