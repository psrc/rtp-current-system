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

gtfs_year <- 2024
gtfs_service <- "fall"
if (tolower(gtfs_service)=="spring") {gtfs_month = "05"} else (gtfs_month = "10")

# Input File Paths -------------------------------------------------------------
efa_file <- file.path(gis_dir, "equity_focus_areas", "efa_3groupings_1sd", "equity_focus_areas_2023.csv")
fgts_file <- file.path(gis_dir, "freight", "FGTSWA.gdb")
fgdb_file <- file.path(gis_dir,"transit","Transit_Network_2024.gdb")
congestion_fgdb_file <- file.path(gis_dir,"congestion","rtp_current_system_congestion.gdb")
its_input_fgdb_file <- file.path(gis_dir,"its","ITS_Signals_2024_Final.gdb")
its_fgdb_file <- file.path(gis_dir,"its","rtp_current_system_its_overlays.gdb")
its_csv_file <- file.path(gis_dir,"its","rtp_current_system_its_overlays.csv")
safety_fgdb_file <- file.path(gis_dir,"safety","high_injury_networks_2024.gdb")

tract_lyr <- "TRACT2020"

# Layers for Overlays ------------------------------------------------------------
print(str_glue("Reading ITS signal data from {its_input_fgdb_file}"))
its <- st_read(dsn = its_input_fgdb_file, layer = "its_signals_final_2024") |> 
  st_transform(spn) 

print(str_glue("Reading ITS signal data from {its_input_fgdb_file} and creating 50' buffer for spatial intersections"))
its_buffer <- st_read(dsn = its_input_fgdb_file, layer = "its_signals_final_2024") |> 
  st_transform(spn) |> 
  st_buffer(dist = 50) |>
  select("signal_id")

print(str_glue("Loading Transit Route layer from {fgdb_file} and trimming to frequent transit"))
frequent_transit <- st_read(dsn = fgdb_file, layer = paste0("transit_routes_", gtfs_service, "_", gtfs_year)) |> 
  st_transform(spn) |>
  filter(frequent == 1) |> 
  select("shape_id", "route")

print(str_glue("Loading the Walk & Bike subset of the High Injury Network from {safety_fgdb_file}"))
high_injury_network <- st_read(dsn = safety_fgdb_file, layer = "high_injury_network_nonmotorized_subset_2024") |> 
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
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`frequent_transit` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`frequent_transit` = replace_na(`frequent_transit`, "No"))

print(str_glue("Intersecting the ITS layer with the Walk & Bike subset of the High Injury Network"))
i <- st_intersection(its_buffer, high_injury_network) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`high_injury_network` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`high_injury_network` = replace_na(`high_injury_network`, "No"))

print(str_glue("Intersecting the ITS layer with the PM Peak Severe Congestion"))
i <- st_intersection(its_buffer, roadway_congestion |> filter(pm_congestion == "Severe")) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`pm_severe_congestion` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`pm_severe_congestion` = replace_na(`pm_severe_congestion`, "No"))

print(str_glue("Intersecting the ITS layer with the PM Peak Heavy & Severe Congestion"))
i <- st_intersection(its_buffer, roadway_congestion) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`pm_heavy_severe_congestion` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`pm_heavy_severe_congestion` = replace_na(`pm_heavy_severe_congestion`, "No"))

print(str_glue("Intersecting the ITS layer with the FGTS network"))
i <- st_intersection(its_buffer, fgts) |>
  filter(FGTSClass %in% c("T-1", "T-2")) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`fgts_t1_t2` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`fgts_t1_t2` = replace_na(`fgts_t1_t2`, "No"))

print(str_glue("Intersecting the ITS layer with People of Color Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_poc >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_poc` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_poc` = replace_na(`efa_poc`, "No"))

print(str_glue("Intersecting the ITS layer with People with Limited Income Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_pov200 >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_pov` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_pov` = replace_na(`efa_pov`, "No"))

print(str_glue("Intersecting the ITS layer with People with Limited English Proficiency Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_lep >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_lep` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_lep` = replace_na(`efa_lep`, "No"))

print(str_glue("Intersecting the ITS layer with People with a Disability Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_dis >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_dis` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_dis` = replace_na(`efa_dis`, "No"))

print(str_glue("Intersecting the ITS layer with Youth Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_youth >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_yth` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_yth` = replace_na(`efa_yth`, "No"))

print(str_glue("Intersecting the ITS layer with Older Adults Equity Focus Areas"))
i <- st_intersection(its_buffer, efa_tracts |> filter(efa_older >0)) |>
  select("signal_id") |>
  st_drop_geometry() |>
  distinct() |>
  mutate(`efa_old` = "Yes")

its <- left_join(its, i, by  ="signal_id") |>
  mutate(`efa_old` = replace_na(`efa_old`, "No"))

# Overlay Layers ----------------------------------------------------------
print(str_glue("Creating ITS overlay layers and summary tables"))
transit_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "tsp", "frequent_transit") |>
  mutate(tsp = str_replace_all(tsp, "Null", "No")) |>
  filter(frequent_transit == "Yes")

transit_tbl <- transit_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(tsp) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on Frequent Transit Routes with Transit Signal Priority") |>
  pivot_wider(names_from = tsp, values_from = count) |>
  mutate(Total = Yes + No)

safety_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", "high_injury_network") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(high_injury_network == "Yes")

safety_tbl <- safety_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the High Injury Network non-motorized subset with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

severe_congestion_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ts_asc", "ts_coordin", "pm_severe_congestion") |>
  mutate(ts_asc = str_replace_all(ts_asc, "Null", "No")) |>
  mutate(ts_coordin = str_replace_all(ts_coordin, "Null", "No")) |>
  mutate(asc_or_coord = case_when (
    ts_asc == "Yes" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "Yes" & ts_coordin == "No" ~ "Yes",
    ts_asc == "No" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "No" & ts_coordin == "No" ~ "No")) |>
  filter(pm_severe_congestion == "Yes")

severe_congestion_tbl <- severe_congestion_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(asc_or_coord) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the NHS network that experience Severe congestion with Adaptive or Coordinated Signals") |>
  pivot_wider(names_from = asc_or_coord, values_from = count) |>
  mutate(Total = Yes + No)

heavy_severe_congestion_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ts_asc", "ts_coordin", "pm_heavy_severe_congestion") |>
  mutate(ts_asc = str_replace_all(ts_asc, "Null", "No")) |>
  mutate(ts_coordin = str_replace_all(ts_coordin, "Null", "No")) |>
  mutate(asc_or_coord = case_when (
    ts_asc == "Yes" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "Yes" & ts_coordin == "No" ~ "Yes",
    ts_asc == "No" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "No" & ts_coordin == "No" ~ "No")) |>
  filter(pm_heavy_severe_congestion == "Yes")

heavy_severe_congestion_tbl <- heavy_severe_congestion_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(asc_or_coord) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the NHS network that experience Heavy or Severe congestion with Adaptive or Coordinated Signals") |>
  pivot_wider(names_from = asc_or_coord, values_from = count) |>
  mutate(Total = Yes + No)

poc_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_of_color = "efa_poc") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_of_color == "Yes")

poc_tbl <- poc_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People of Color with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

pov_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_lower_incomes = "efa_pov") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_lower_incomes == "Yes")

pov_tbl <- pov_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People with Lower Incomes with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

lep_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_limited_english = "efa_lep") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_limited_english == "Yes")

lep_tbl <- lep_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People with Limited English Profiency with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

dis_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_with_a_disability = "efa_dis") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_with_a_disability == "Yes")

dis_tbl <- dis_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People with a Disability with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

yth_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_under_18 = "efa_yth") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_under_18 == "Yes")

yth_tbl <- yth_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People under 18 with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

old_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ped_signal", people_over_65 = "efa_old") |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  filter(people_over_65 == "Yes")

old_tbl <- old_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals in areas with a higher share of People 65+ with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

region_ped_tbl <- its |>
  st_drop_geometry() |>
  mutate(ped_signal = str_replace_all(ped_signal, "Null", "No")) |>
  mutate(count = 1) |>
  group_by(ped_signal) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the Regional NHS network with Accessible Pedestrian Signals") |>
  pivot_wider(names_from = ped_signal, values_from = count) |>
  mutate(Total = Yes + No)

severe_congestion_fgts_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ts_asc", "ts_coordin", "pm_severe_congestion", "fgts_t1_t2") |>
  mutate(ts_asc = str_replace_all(ts_asc, "Null", "No")) |>
  mutate(ts_coordin = str_replace_all(ts_coordin, "Null", "No")) |>
  mutate(asc_or_coord = case_when (
    ts_asc == "Yes" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "Yes" & ts_coordin == "No" ~ "Yes",
    ts_asc == "No" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "No" & ts_coordin == "No" ~ "No")) |>
  filter(pm_severe_congestion == "Yes" & fgts_t1_t2 == "Yes")

severe_congestion_fgts_tbl <- severe_congestion_fgts_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(asc_or_coord) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the FGTS T1 or T2 network that experience Severe congestion with Adaptive or Coordinated Signals") |>
  pivot_wider(names_from = asc_or_coord, values_from = count) |>
  mutate(Total = Yes + No)

heavy_severe_congestion_fgts_overlay <- its |>
  select("signal_id", "city_name", "cnty_name", "majorst_1", "ts_asc", "ts_coordin", "pm_heavy_severe_congestion", "fgts_t1_t2") |>
  mutate(ts_asc = str_replace_all(ts_asc, "Null", "No")) |>
  mutate(ts_coordin = str_replace_all(ts_coordin, "Null", "No")) |>
  mutate(asc_or_coord = case_when (
    ts_asc == "Yes" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "Yes" & ts_coordin == "No" ~ "Yes",
    ts_asc == "No" & ts_coordin == "Yes" ~ "Yes",
    ts_asc == "No" & ts_coordin == "No" ~ "No")) |>
  filter(pm_heavy_severe_congestion == "Yes" & fgts_t1_t2 == "Yes")

heavy_severe_congestion_fgts_tbl <- heavy_severe_congestion_fgts_overlay |>
  st_drop_geometry() |>
  mutate(count = 1) |>
  group_by(asc_or_coord) |>
  summarise(count = sum(count)) |>
  as_tibble() |>
  mutate(Metric = "Signals on the FGTS T1 or T2 network that experience Heavy or Severe congestion with Adaptive or Coordinated Signals") |>
  pivot_wider(names_from = asc_or_coord, values_from = count) |>
  mutate(Total = Yes + No)

# Write Overlay layers to the filegdb ------------------------------------------------------
print(str_glue("Writing the ITS overlay layers to {its_fgdb_file}"))
st_write(its, dsn = its_fgdb_file, layer = "all_signal_overlays", append = FALSE)
st_write(transit_overlay, dsn = its_fgdb_file, layer = "transit_overlay", append = FALSE)
st_write(safety_overlay, dsn = its_fgdb_file, layer = "safety_overlay", append = FALSE)
st_write(severe_congestion_overlay, dsn = its_fgdb_file, layer = "severe_congestion_overlay", append = FALSE)
st_write(heavy_severe_congestion_overlay, dsn = its_fgdb_file, layer = "heavy_severe_congestion_overlay", append = FALSE)
st_write(poc_overlay, dsn = its_fgdb_file, layer = "people_of_color_overlay", append = FALSE)
st_write(pov_overlay, dsn = its_fgdb_file, layer = "people_lower_incomes_overlay", append = FALSE)
st_write(lep_overlay, dsn = its_fgdb_file, layer = "people_limited_english_overlay", append = FALSE)
st_write(dis_overlay, dsn = its_fgdb_file, layer = "people_with_a_disability_overlay", append = FALSE)
st_write(yth_overlay, dsn = its_fgdb_file, layer = "people_under_18_overlay", append = FALSE)
st_write(old_overlay, dsn = its_fgdb_file, layer = "people_over_65_overlay", append = FALSE)
st_write(severe_congestion_fgts_overlay, dsn = its_fgdb_file, layer = "fgts_t1_t2_severe_congestion_overlay", append = FALSE)
st_write(heavy_severe_congestion_fgts_overlay, dsn = its_fgdb_file, layer = "fgts_t1_t2_heavy_severe_congestion_overlay", append = FALSE)

# Write overlay table to a csv -------------------------------------------
print(str_glue("Writing the ITS summary tables to {its_csv_file}"))
overlay_tbl <- bind_rows(transit_tbl, safety_tbl, 
                         severe_congestion_tbl, heavy_severe_congestion_tbl, 
                         poc_tbl, pov_tbl, lep_tbl, dis_tbl, yth_tbl, old_tbl, 
                         region_ped_tbl, 
                         severe_congestion_fgts_tbl,
                         heavy_severe_congestion_fgts_tbl) |>
  mutate(`Share of No` = round(No / Total, 2)) |>
  mutate(`Share of Yes` = round(Yes / Total, 2))

write_csv(overlay_tbl, its_csv_file)
