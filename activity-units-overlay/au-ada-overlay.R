# This script overlays the ADA paratransit areas with Activity Units 2024

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

##  ADA Coverage ----

proj_dir <- "J:\\Staff\\Christy\\GIS\\popemp-ada"
ada_fgdb <- file.path(proj_dir, "2023_2024_ADA Coverage.gdb")
ada_lyrs <- st_layers(dsn = ada_fgdb)

# create list of reprojected layers
lyrs_reproj <- list()
for(i in 1:nrow(ada_lyrs)) {
  
  # identify layer name
  lyr_name <- ada_lyrs$name[[i]]
  
  lyr <- st_read(dsn = ada_fgdb, layer = lyr_name)|>
    mutate(coverage = lyr_name) |> 
    select(coverage)
  
  # check and set crs
  if(ada_lyrs$crs[[i]][[1]] != "NAD83 / Washington North (ftUS)") {
    lyr <- lyr |> 
      st_transform(spn)
  }
  
  lyrs_reproj[[lyr_name]] <- lyr
}

# create list of spatial joined layers and a list to identify duplicated records from join
lyrs_join <- list()
lyrs_dups <- list()
for(i in 1:length(lyrs_reproj)) {
  int <- st_join(au_min, lyrs_reproj[[i]]) |> 
    distinct()
  
  dups <- int  |> 
    group_by(GRID_ID) |> 
    filter(n() > 1) |> 
    select(GRID_ID, starts_with("sum"), coverage)
  
  lyrs_join[[i]] <- int
  lyrs_dups[[i]] <- dups
}

names(lyrs_join) <- names(lyrs_reproj)
names(lyrs_dups) <- names(lyrs_reproj)

# for each layer, summarise by coverage and sum pop, jobs, au
lyrs_sum <- list()
for(i in 1:length(lyrs_join)) {
  lyr_join <- lyrs_join[[i]] |> 
    group_by(coverage) |> 
    summarise(across(starts_with("sum"), sum)) |> 
    rename(population_2024 = sum_pop_20, jobs_2024 = sum_jobs_2, total_au_2024 = sum_au_202)
  
  lyr_join_df <- lyr_join |> 
    st_drop_geometry() |> 
    mutate(coverage = ifelse(is.na(coverage), paste("Not in", names(lyrs_join)[[i]]), coverage))
    
  
  lyrs_sum[[i]] <- lyr_join_df
}

ada_df_sep <- bind_rows(lyrs_sum)

# merge all 5 shapes into one & sp join to find how many are located in ADA services





# # write to fgdb
# output_gdb_path <- file.path(proj_dir,"popemp-ada.gdb")  # Path to the file geodatabase folder
# output_layer_name <- "my_test_data" # Name of the feature class within the geodatabase
# st_write(test_fil, dsn = output_gdb_path, layer = output_layer_name, driver = "OpenFileGDB", append=FALSE)
