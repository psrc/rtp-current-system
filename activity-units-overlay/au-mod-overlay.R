# MOD Map and relevant layers: Puget Sound Regional Council\GIS - Sharing\Projects\Transportation\RTP_2026\mod (find the “in template” version of the map for the latest version)
# For the specialized vs. MOD overlay:
# “specialized transportation” service boundaries are saved here: Puget Sound Regional Council\GIS - Sharing\Projects\Transportation\RTP_2026\specialized transportation\2023_Specialized Transportation Coverage
# ADA paratransit: Puget Sound Regional Council\GIS - Sharing\Projects\Transportation\RTP_2026\specialized transportation\2023_2024_ADA Paratransit Coverage\2023_2024_ADA Coverage
# Population/employment density (a.k.a. activity units): Puget Sound Regional Council\GIS - Sharing\Projects\Transportation\RTP_2026\activity_units

library(tidyverse)
library(sf)
library(openxlsx)

un <- Sys.getenv("USERNAME")
gis_dir <- file.path("C:/Users",str_to_lower(un),"Puget Sound Regional Council/GIS - Sharing/Projects/Transportation/RTP_2026")
options(dplyr.summarise.inform = FALSE)

# Inputs ----

wgs84 <- 4326
spn <- 2285

## people_and_jobs_2024 ----

au_file <- file.path(gis_dir, "activity_units", "Activity_Units_2026_RTP.gdb")
au <- st_read(dsn = au_file, layer = "peope_and_jobs_2024")|> 
  st_transform(spn)

# transit supportive areas
# filter for >= 7 units/acre
au_min <- au |> 
  filter(au_acre >= 7)

## MOD ----
# Database	C:\Users\CLam\Puget Sound Regional Council\GIS - RTP_2026\mod\MODInventory.gdb, features do not read-in. Use J drive copy
# original db
# mod_inventory <- file.path(gis_dir, "mod", "MODInventory.gdb") 
# spec_files <- map(mod_lyrs, st_read(dsn = mod_inventory, layer = .x))

mod_inventory <- "J:\\Staff\\Christy\\GIS\\popemp-density\\popemp-density.gdb"
mod_lyrs <- st_layers(dsn = mod_inventory)

mt_all <- st_read(dsn = mod_inventory, layer = "Microtransit_northshore_nsno") |> 
  st_transform(spn)

au_mt_int <- st_join(au_min, st_make_valid(mt_all))

# duplicates found; overlapping boundaries
df <- au_mt_int |> 
  select(GRID_ID:transit, ends_with("acres"), MERGE_SRC, Shape) |> 
  distinct() |> 
  mutate(dup = ifelse(GRID_ID %in% c("CL-80", "CM-80") & MERGE_SRC == "Microtransit Services in PSRC Region\\King County Metro (Metro Flex Northshore)", 1, 0)) |> 
  filter(dup == 0) |> 
  mutate(name = str_extract(MERGE_SRC, "(?<=\\\\).*"))
  
# dups <- df %>%
#   group_by(GRID_ID) %>%
#   filter(n() > 1)

# summarise by specialized areas
df_summary <- df |> 
  st_drop_geometry() |> 
  group_by(name) |> 
  summarise(population_2024 = sum(sum_pop_20), jobs_2024 = sum(sum_jobs_2)) |> 
  mutate(name = ifelse(is.na(name), "Outside Specialized Transportation Areas", name)) |> 
  mutate(total_au_2024 = population_2024 + jobs_2024)

# write.xlsx(df_summary, "J:\\Staff\\Christy\\GIS\\popemp-density\\output\\specialized-mod-overlay.xlsx")


