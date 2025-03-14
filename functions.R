# Data Processing ---------------------------------------------------------
transit_stops_by_mode <- function(year, service_change, buffer_dist=0.25) {
  
  hct_file <- "data/hct_ids.csv"
  hct <- read_csv(hct_file, show_col_types = FALSE) 
  
  if (tolower(service_change)=="spring") {data_month = "05"} else (data_month = "10")
  
  options(dplyr.summarise.inform = FALSE)
  gtfs_file <- paste0("C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/gtfs/",tolower(service_change),"/",as.character(year),".zip")
  
  # Open Regional GTFS File and load into memory
  print(str_glue("Opening the {service_change} {year} GTFS archive."))
  gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))
  
  # Load Stops
  print(str_glue("Getting the {service_change} {year} stops into a tibble." ))
  stops <- as_tibble(gtfs$stops) |> 
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("stop_id", "stop_name", "stop_lat", "stop_lon")
  
  # Load Routes, add HCT modes and update names and agencies
  print(str_glue("Getting the {service_change} {year} routes into a tibble." ))
  routes <- as_tibble(gtfs$routes) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")
  
  print(str_glue("Adding High-Capacity Transit codes to the {service_change} {year} routes"))
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
  print(str_glue("Getting the {service_change} {year} trips into a tibble to add route ID to stop times." ))
  trips <- as_tibble(gtfs$trips) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("trip_id", "route_id")
  
  trips <- left_join(trips, routes, by=c("route_id"))
  
  # Clean Up Stop Times to get routes and mode by stops served
  print(str_glue("Getting the {service_change} {year} stop times into a tibble to add route information." ))
  stoptimes <- as_tibble(gtfs$stop_times) |>
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("trip_id", "stop_id")
  
  # Determine number of daily trips by stop
  print(str_glue("Getting the {service_change} {year} number of transit trips by stop a tibble." ))
  stoptrips <- as_tibble(gtfs$stop_times) |>
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("stop_id", "arrival_time") |>
    arrange(stop_id, arrival_time) |>
    mutate(hour = hour(arrival_time), daily_trips = 1) |>
    group_by(stop_id) |>
    summarise(daily_trips = sum(daily_trips), min_hr = min(hour), max_hr = max(hour)) |>
    as_tibble() |>
    mutate(hours_served = case_when(
      max_hr == min_hr ~ 1,
      max_hr > min_hr ~ (max_hr - min_hr))) |>
    mutate(trips_per_hr = daily_trips / hours_served)
  
  # Get Mode and agency from trips to stops
  print(str_glue("Getting unique stop list by modes for the {service_change} {year}." ))
  stops_by_mode <- left_join(stoptimes, trips, by=c("trip_id")) |>
    select("stop_id", "type_code", "type_name", "agency_name") |>
    distinct()
  
  print(str_glue("Adding daily trips by stop"))
  stops_by_mode <- left_join(stops_by_mode, stoptrips, by=c("stop_id"))
  
  stops_by_mode <- left_join(stops_by_mode, stops, by=c("stop_id")) |>
    mutate(date=mdy(paste0(data_month,"-01-",year)))
  
  # Convert it to a shapefile
  print(str_glue("Converting {service_change} {year} to a shapefile layer"))
  s <- stops_by_mode |> 
    st_as_sf(coords = c("stop_lon", "stop_lat"), crs=wgs84) |>
    st_transform(spn) |>
    st_buffer(dist = buffer_dist * 5280) |>
    select(id = "stop_id", mode = "type_name", agency = "agency_name", "min_hr", "max_hr", tot_hr = "hours_served", daily="daily_trips", hourly="trips_per_hr", name="stop_name", "date")
  
  print(str_glue("All Done."))
  
  return(s)
  
}

transit_routes_by_mode <- function(year, service_change) {
  
  hct_file <- "data/hct_ids.csv"
  hct <- read_csv(hct_file, show_col_types = FALSE) 
  
  if (tolower(service_change)=="spring") {data_month = "05"} else (data_month = "10")
  
  options(dplyr.summarise.inform = FALSE)
  gtfs_file <- paste0("C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/gtfs/",tolower(service_change),"/",as.character(year),".zip")
  
  # Open Regional GTFS File and load into memory
  print(str_glue("Opening the {service_change} {year} GTFS archive."))
  gtfs <- read_gtfs(path=gtfs_file, files = c("trips","stops","stop_times", "routes", "shapes"))
  
  # Load Stops
  print(str_glue("Getting the {service_change} {year} stops into a tibble." ))
  stops <- as_tibble(gtfs$stops) |> 
    mutate(stop_id = str_to_lower(stop_id)) |>
    select("stop_id", "stop_name", "stop_lat", "stop_lon")
  
  # Load Routes, add HCT modes and update names and agencies
  print(str_glue("Getting the {service_change} {year} routes into a tibble." ))
  routes <- as_tibble(gtfs$routes) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "agency_id","route_short_name", "route_long_name", "route_type")
  
  print(str_glue("Adding High-Capacity Transit codes to the {service_change} {year} routes"))
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
  print(str_glue("Getting the {service_change} {year} trips into a tibble to add route ID to stop times." ))
  trips <- as_tibble(gtfs$trips) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("trip_id", "route_id", "shape_id")
  
  trips <- left_join(trips, routes, by=c("route_id"))
  
  # Clean Up Stop Times to get routes and mode by stops served
  print(str_glue("Getting the {service_change} {year} stop times into a tibble to add route information." ))
  stoptimes <- as_tibble(gtfs$stop_times) |>
    mutate(stop_id = str_to_lower(stop_id)) |>
    filter(stop_sequence == 1) |>
    select("trip_id", "arrival_time")
  
  # Determine number of daily trips by route
  print(str_glue("Getting the {service_change} {year} number of transit trips by route a tibble." ))
  routetrips <- left_join(stoptimes, trips, by="trip_id") |>
    mutate(hour = hour(arrival_time), daily_trips = 1) |>
    group_by(route_id) |>
    summarise(daily_trips = sum(daily_trips), min_hr = min(hour), max_hr = max(hour)) |>
    as_tibble() |>
    mutate(hours_served = case_when(
      max_hr == min_hr ~ 1,
      max_hr > min_hr ~ (max_hr - min_hr))) |>
    mutate(trips_per_hr = daily_trips / hours_served)
  
  # Load Route Shapes to get Mode information on layers
  route_lyr <- shapes_as_sf(gtfs$shapes)
  
  # Trips are used to get route id onto shapes
  print(str_glue("Getting the {service_change} {year} trips into a tibble to add route ID to stop times." ))
  trips <- as_tibble(gtfs$trips) |> 
    mutate(route_id = str_to_lower(route_id)) |>
    select("route_id", "shape_id") |>
    distinct()
  
  route_lyr <- left_join(route_lyr, trips, by=c("shape_id"))
  
  # Get Mode and agency from routes to shapes
  print(str_glue("Getting route details onto shapes for the {service_change} {year}." ))
  route_lyr <- left_join(route_lyr, routetrips, by=c("route_id")) |> mutate(date=mdy(paste0(data_month,"-01-",year)))
  route_lyr <- left_join(route_lyr, routes, by=c("route_id")) |>
    select(id = "shape_id", "route_id", mode = "type_name", agency = "agency_name", "min_hr", "max_hr", tot_hr = "hours_served", daily="daily_trips", hourly="trips_per_hr", name="route_name", "date")
  
  print(str_glue("All Done."))
  
  return(route_lyr)
  
}

process_ntd_ridership_data <- function() {
  
  # Location of the most recently downloaded NTD file
  data_file <- "C:/Users/chelmann/Puget Sound Regional Council/2026-2050 RTP Trends - General/Transit/data/ntd-data.xlsx"
  
  # Silence the dplyr summarize message
  options(dplyr.summarise.inform = FALSE)
  
  # Passenger Trips, Revenue-Miles and Revenue-Hours tabs
  ntd_tabs <- c("UPT", "VRM", "VRH")
  
  # Figure out which Transit Agencies serve which MPO's
  agency_file <- "data/transit-agency.csv"
  print(str_glue("Figuring out which Transit agencies are in which Metro Area."))
  agencies <- read_csv(agency_file, show_col_types = FALSE) |>
    mutate(NTDID = str_pad(string=NTDID, width=5, pad="0", side=c("left"))) |>
    mutate(UACE = str_pad(string=UACE, width=5, pad="0", side=c("left")))
  
  ntd_ids <- agencies |> select("NTDID") |> distinct() |> pull()
  
  # Lookup for NTD Modes
  mode_file <- "data/ntd-modes.csv"
  ntd_modes <- read_csv(mode_file, show_col_types = FALSE)
  
  # Initial processing of NTD data
  processed <- NULL
  for (areas in ntd_tabs) {
    print(str_glue("Working on {areas} data processing and cleanup."))
    
    # Open file and filter data to only include operators for RTP analysis
    t <- as_tibble(read.xlsx(data_file, sheet = areas, skipEmptyRows = TRUE, startRow = 1, colNames = TRUE)) |>
      mutate(NTD.ID = str_pad(string=NTD.ID, width=5, pad="0", side=c("left"))) |>
      filter(NTD.ID %in% ntd_ids) |> 
      mutate(Mode = case_when(
        Mode == "DR" & TOS == "DO" ~ "DR-DO",
        Mode == "DR" & TOS == "PT" ~ "DR-PT",
        Mode == "DR" & TOS == "TN" ~ "DR-TN",
        Mode == "DR" & TOS == "TX" ~ "DR-TX",
        TRUE ~ Mode)) |> 
      select(-"Legacy.NTD.ID", -contains("Status"), -"Reporter.Type", -"UACE.CD", -"UZA.Name", -"TOS", -"3.Mode") |> 
      pivot_longer(cols = 4:last_col(), names_to = "date", values_to = "estimate", values_drop_na = TRUE) |> 
      mutate(date = my(date))
    
    # Add Detailed Mode Names & Aggregate  
    t <- left_join(t, ntd_modes, by=c("Mode")) |> 
      rename(variable="mode_name") |> 
      select(-"Mode") |>
      group_by(NTD.ID, Agency, date, variable) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble()
    
    # Add Metro Area Name
    n <- agencies |> select("NTDID", "MPO_AREA", "AGENCY_NAME")
    t <- left_join(t, n, by=c("NTD.ID"="NTDID"), relationship = "many-to-many") |>
      select(-"NTD.ID", -"Agency") |>
      rename(grouping="MPO_AREA", geography="AGENCY_NAME") |>
      as_tibble() |>
      mutate(metric=areas) |>
      mutate(metric = case_when(
        metric == "UPT" ~ "Boardings",
        metric == "VRM" ~ "Revenue-Miles",
        metric == "VRH" ~ "Revenue-Hours"))
    
    rm(n)
    
    # Full Year Data
    
    max_yr <- t |> select("date") |> distinct() |> pull() |> max() |> year()
    max_mo <- t |> select("date") |> distinct() |> pull() |> max() |> month()
    
    if (max_mo <12) {
      yr <- max_yr-1
    } else {
      yr <- max_yr
    }
    
    # Trim Data so it only includes full year data and combine
    print(str_glue("Trim {areas} data to only include full year data."))
    full_yr <- t |>
      filter(year(date)<=yr) |>
      mutate(year = year(date)) |>
      group_by(year, variable, grouping, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(date = ymd(paste0(year,"-12-01"))) |>
      select(-"year")
    
    # Metro Areas only need to compare at the total level
    print(str_glue("Summaring Metro Area {areas} full year data."))
    metro_total <-  full_yr |>
      group_by(date, grouping, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography=grouping, geography_type="Metro Areas", variable="All Transit Modes", grouping="Annual")
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Mode."))
    region_modes <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, variable, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="Annual")
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC Region {areas} full year data for All Transit."))
    region_total <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="Annual", variable="All Transit Modes")
    
    # PSRC Region by Operator and Mode
    print(str_glue("Summaring PSRC Region {areas} full year data by Transit Operator and Modes."))
    region_operator_modes <-  full_yr |>
      filter(grouping=="Seattle") |>
      mutate(geography_type="Transit Operator", grouping="Annual")
    
    # PSRC Region by Operator
    print(str_glue("Summaring PSRC Region {areas} full year data by Transit Operator for All Transit."))
    region_operator <-  full_yr |>
      filter(grouping=="Seattle") |>
      group_by(date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Transit Operator", grouping="Annual", variable="All Transit Modes")
    
    ifelse(is.null(processed), 
           processed <- bind_rows(list(region_operator,region_operator_modes,
                                       region_total,region_modes,
                                       metro_total)), 
           processed <- bind_rows(list(processed,
                                       region_operator,region_operator_modes,
                                       region_total,region_modes,
                                       metro_total)))
    
    rm(region_operator,region_operator_modes,
       region_total,region_modes,
       metro_total, full_yr)
    
    # Year to Date
    
    # Ensure that all data is consistent - find the maximum month for YTD calculations
    max_mo <- t |> select("date") |> distinct() |> pull() |> max() |> month()
    
    # Trim Data so it only includes ytd for maximum month and combine
    print(str_glue("Trim {areas} data to only months for year to date data through month {max_mo}."))
    ytd <- t |>
      filter(month(date)<=max_mo) |>
      mutate(year = year(date)) |>
      group_by(year, variable, grouping, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(date = ymd(paste0(year,"-",max_mo,"-01"))) |>
      select(-"year")
    
    # Metro Areas only need to compare at the total level
    print(str_glue("Summaring Metro Area {areas} year to date data through month {max_mo}."))
    metro_total <-  ytd |>
      group_by(date, grouping, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography=grouping, geography_type="Metro Areas", variable="All Transit Modes", grouping="YTD")
    
    # PSRC Region by Mode
    print(str_glue("Summaring PSRC region {areas} year to date data by mode through month {max_mo}."))
    region_modes <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, variable, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="YTD")
    
    # PSRC Region Total
    print(str_glue("Summaring PSRC region {areas} year to date data by All Transit through month {max_mo}."))
    region_total <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography="Region", geography_type="Region", grouping="YTD", variable="All Transit Modes")
    
    # PSRC Region by Operator and Mode
    print(str_glue("Summaring PSRC region {areas} year to date data by Operator and Mode through month {max_mo}."))
    region_operator_modes <-  ytd |>
      filter(grouping=="Seattle") |>
      mutate(geography_type="Transit Operator", grouping="YTD")
    
    # PSRC Region by Operator
    print(str_glue("Summaring PSRC region {areas} year to date data by Operator through month {max_mo}."))
    region_operator <-  ytd |>
      filter(grouping=="Seattle") |>
      group_by(date, geography, metric) |>
      summarise(estimate=sum(estimate)) |>
      as_tibble() |>
      mutate(geography_type="Transit Operator", grouping="YTD", variable="All Transit Modes")
    
    # Add YTD to Annual Data
    processed <- bind_rows(list(processed,
                                region_operator,region_operator_modes,
                                region_total,region_modes,
                                metro_total))
    
    rm(region_operator,region_operator_modes,
       region_total,region_modes,
       metro_total, ytd, t)
    
  }
  
  # Pivot NTD data wide and create new metric: boardings per revenue-hour
  print(str_glue("Calculating Boardings per Revenue Hour andd performing final cleanup."))
  processed_wide <- processed |> 
    pivot_wider(names_from = "metric",
                values_from = "estimate") |> 
    mutate(`Boardings-per-Hour` = ifelse(`Revenue-Hours` > 0,
                                         round(`Boardings` / `Revenue-Hours`, 2), NA))
  
  # Pivot NTD data back to long form for app use
  processed <- processed_wide |> 
    pivot_longer(cols = c("Boardings",
                          "Revenue-Miles",
                          "Revenue-Hours",
                          "Boardings-per-Hour"),
                 names_to = "metric",
                 values_to = "estimate")
  
  processed <- processed |> 
    mutate(year = as.character(year(date))) |>
    mutate(concept = "Transit Agency Service Data") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "concept", "estimate") |>
    drop_na() |>
    filter(estimate >0)
  
  print(str_glue("All done."))
  
  return(processed)
  
}

map_npmrds_data <- function(file_path, coord_sys) {
  
  print(str_glue("Opening travel time shapefile"))
  tmc <- st_read(file.path(file_path,"shapefiles", as.character(year(Sys.Date())), "Washington.shp")) |>
    select("Tmc", roadway="RoadName", geography="County") |>
    filter(geography %in% c("KING", "KITSAP","PIERCE", "SNOHOMISH")) |>
    mutate(geography = str_to_title(geography)) |>
    st_transform(coord_sys) |>
    drop_na()
  
  processed <- NULL
  for (file in list.files(path = file.path(file_path, "processed-data"), pattern = ".*.csv")) {
    d <- str_remove_all(file, "_cars_tmc_95th_percentile_speed_weekdays.csv")
    print(str_glue("Opening travel time data for weekdays in {d}"))
    
    t <- read_csv(file.path(file_path,  "processed-data", file), show_col_types = FALSE) |> 
      mutate(date=d) |>
      mutate(lane_miles = miles * thrulanes_unidir) |>
      separate(col=date, into = c("date","year")) |>
      mutate(year = as.character(year)) |>
      mutate(date = mdy(paste0(date,"-01-",year))) |>
      filter(!(is.na(f_system))) |>
      select("Tmc", "date", "year", am_peak="7am_ratio", midday="Noon_ratio", pm_peak="4pm_ratio") |>
      drop_na()
    
    print(str_glue("Joining speed data to shapefile for {d}"))
    p <- left_join(tmc, t, by="Tmc") |> drop_na()
    
    ifelse(is.null(processed), processed <- p, processed <- bind_rows(processed, p))
    
  }
  
  return(processed)
  
}

process_npmrds_data <- function(file_path) {
  
  # Lists for aggregation
  am_peak <- c("5am", "6am","7am", "8am")
  midday <- c("9am", "10am", "11am","Noon", "1pm", "2pm")
  pm_peak <- c("3pm", "4pm","5pm", "6pm")
  evening <- c("7pm", "8pm", "9pm", "10pm")
  overnight <- c("11pm", "Midnight", "1am", "2am", "3am", "4am")
  
  processed <- NULL
  for (file in list.files(path = file.path(file_path, "processed-data"), pattern = ".*.csv")) {
    d <- str_remove_all(file, "_cars_tmc_95th_percentile_speed_weekdays.csv")
    
    print(str_glue("Opening travel time data for weekdays in {d}"))
    
    t <- read_csv(file.path(file_path,  "processed-data", file), show_col_types = FALSE) |> 
      mutate(date=d) |>
      mutate(lane_miles = miles * thrulanes_unidir) |>
      separate(col=date, into = c("date","year")) |>
      mutate(year = as.character(year)) |>
      mutate(date = mdy(paste0(date,"-01-",year))) |>
      filter(!(is.na(f_system)))
    
    ifelse(is.null(processed), processed <- t, processed <- bind_rows(processed, t))
    
  }
  
  m <- "county" 
  
  print(str_glue("Summarizing Travel Time Data by {m}"))
  c <- processed |>
    select("Tmc", all_of(m), "lane_miles", contains("ratio"), "date", "year") |>
    pivot_longer(cols = contains("ratio")) |>
    mutate(name = str_remove_all(name, "_ratio")) |>
    mutate(geography = str_to_title(county), variable = name) |>
    mutate(grouping = case_when(
      value < severe_congestion_threshold ~ "Severe",
      value < heavy_congestion_threshold ~ "Heavy",
      value < moderate_congestion_threshold ~ "Moderate",
      value >= moderate_congestion_threshold ~ "Minimal")) |>
    drop_na() |>
    group_by(geography, date, year, variable, grouping) |>
    summarise(congested = sum(lane_miles)) |>
    as_tibble()
  
  t <- processed |>
    mutate(geography = str_to_title(county)) |>
    group_by(geography, date, year) |>
    summarise(total = sum(lane_miles)) |>
    as_tibble()
  
  c <- left_join(c,t, by=c("geography", "date", "year")) |>
    mutate(variable = case_when(
      variable %in% am_peak ~ "AM Peak Period",
      variable %in% midday ~ "Midday",
      variable %in% pm_peak ~ "PM Peak Period",
      variable %in% evening ~ "Evening",
      variable %in% overnight ~ "Overnight")) |>
    group_by(geography, date, year, variable, grouping) |>
    summarise(estimate = sum(congested), total = sum(total)) |>
    as_tibble() |>
    mutate(metric = "Congested Lane-Miles", geography_type="County")
  
  r <- c |>
    group_by(date, year, variable, grouping) |>
    summarise(estimate = sum(estimate), total = sum(total)) |>
    as_tibble() |>
    mutate(metric = "Congested Lane-Miles", geography_type="Region", geography="Region")
  
  c <- bind_rows(c, r) |>
    mutate(share = estimate / total) |> 
    select(-"total") |>
    select("year", "date", "geography", "geography_type", "variable", "grouping", "metric", "estimate", "share")
  
  return(c)
  
}
