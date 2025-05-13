library(tidyverse)
library(DBI)

# Inputs ------------------------------------------------------------------
total_possible_score <- 90

# General Project information columns
project_id_column <- "MTPID"
project_title_column <- "Title"
cost_column <- "ScaledCost"
sponsor_column <- "Sponsor"
start_column <- "StartYear"
completion_column <- "CompletionYear"
cost_year_column <- "CostYear"
original_cost_column <- "ReportedCost"
current_funds_column <- c("FundsCommitted")
current_funds_descriptions_column <- c("CommittedFundSources")
county_column <- "CountyName"

county_order <- c("Region", "King", "Kitsap", "Pierce", "Snohomish", "Multiple")

# Temporary Project County Fixes
king_county_projects <- c(5808, 5822, 5827, 5832, 5837, 5850)
pierce_county_projects <- c(5812, 5814, 5815, 5816)

# Sponsor Categories ------------------------------------------------------
local_transit_sponsors <- c("COMMUNITY TRANSIT", "EVERETT TRANSIT", "KING COUNTY/METRO", "KITSAP TRANSIT", "PIERCE TRANSIT")
regional_transit_sponsors <- c("SOUND TRANSIT")
wsdot_sponsors <- c("WSDOT", "WSDOT NORTHWEST REGION", "WSDOT OLYMPIC REGION")
wsf_sponsors <- c("WASHINGTON STATE FERRIES", "WSF")
county_sponsors <- c("KING COUNTY DEPT. OF TRANSPORTATION (ROADS)", "KITSAP COUNTY", "PIERCE COUNTY", "SNOHOMISH COUNTY")
tribal_sponsors <- c("TULALIP TRIBES")
port_sponsors <- c("PORT OF SEATTLE")

# Scope Element Columns
bridge_project_columns <- c("Bridge Replacement or Rehabiltation", "New or Widened Bridge")

transit_project_columns <- c( "Park & Ride", "Add or Remove Business Access Transit Lanes",
                             "New or Relocated Transit Center or Station", "Transit Center or Station Improvement", 
                             "New High Capacity Transit Alignment", "Interstate Bus Flyer Stop",
                             "Transit Amenities", "Transit Maintenance and Operation Base", "Vehicle Purchase")

nonmotorized_project_columns <- c("Bike/Pedestrian Trail or Path", "Other Bike/Pedestrian Path", "Bike Lane",
                                  "Sidewalk", "Bike/Pedestrian Bridge", "Other Bicycle/Pedestrian Improvements",
                                  "Transportation Demand Management")

ferry_project_columns <- c("New Ferry Route","Change to Existing Ferry Service", "New or Relocated Ferry Terminal", 
                           "Ferry terminal improvement", "Ferry vessel purchase or maintenance")

interchange_project_columns <- c("New Interchange", "Interchange Improvements", "Grade Separation")

maintenance_preservation_operations_project_columns <- c("Road Resurfacing/Rehabilitation", "Intersection Improvements",
                                                         "Intelligent Transportation System", "Other Preservation/Maintenance")

roadway_project_columns <- c("New Roadway Facility", "Roadway Relocation", "Add or Remove General Purpose Capacity Lanes", 
                             "Change Roadway Usage/Operations", "Add or Remove High Occupancy Vehicle Lanes")

all_scope_columns <- c(bridge_project_columns, transit_project_columns, nonmotorized_project_columns, 
                       ferry_project_columns, interchange_project_columns, 
                       maintenance_preservation_operations_project_columns, 
                       roadway_project_columns)

# Plan Consistency Columns
overall_score_column <- "TotalConsistencyScore"
freight_score_column <- "FreightMovementScore"
employment_score_column <- "EmploymentScore"
emissions_score_column <- "EmissionsScore"
water_score_column <- "PugetSoundLandAndWaterScore"
alternative_score_column <- "TransportationAltenativesScore"
reliability_score_column <- "TravelReliabilityScore"
centers_score_column <- "SupportForCentersScore"
safety_score_column <- "SafetyScore"
community_score_column <- "CommunityBenefitsScore"

all_score_columns <- c(overall_score_column, freight_score_column, employment_score_column, emissions_score_column,
                   water_score_column, alternative_score_column, reliability_score_column, centers_score_column,
                   safety_score_column, community_score_column)

# Functions ---------------------------------------------------------------
create_project_scoring_summary <- function(df = rtp_projects, project_type, scope_elements = all_scope_columns) {
  
  # Overall Project Summary by County
  p <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(count = 1, `Project Type` = project_type, County = .data[[county_column]]) |>
    mutate(County = str_remove_all(County, " County")) |>
    mutate(County = str_remove_all(County, " Counties \\(any combination\\)")) |>
    group_by(`Project Type`, `County`) |>
    summarise(`# of Projects` = sum(count), 
              `Project Cost` = sum(.data[[cost_column]]),  
              `Average Consistency Score` = sum(.data[[overall_score_column]]),
              `Freight` = sum(.data[[freight_score_column]]),
              `Employment` = sum(.data[[employment_score_column]]),
              `Emissions` = sum(.data[[emissions_score_column]]),
              `Puget Sound Land & Water` = sum(.data[[water_score_column]]),
              `Transportation Alternatives` = sum(.data[[alternative_score_column]]),
              `Travel Reliability` = sum(.data[[reliability_score_column]]),
              `Support for Centers` = sum(.data[[centers_score_column]]),
              `Safety` = sum(.data[[safety_score_column]]),
              `Community Benefits` = sum(.data[[community_score_column]])) |>
    as_tibble() |>
    mutate(`Average Project Grade` = ((`Average Consistency Score`/`# of Projects`)/total_possible_score)*100) |>
    mutate(`Freight` = `Freight` / `Average Consistency Score`) |>
    mutate(`Employment` = `Employment` / `Average Consistency Score`) |>
    mutate(`Emissions` = `Emissions` / `Average Consistency Score`) |>
    mutate(`Puget Sound Land & Water` = `Puget Sound Land & Water` / `Average Consistency Score`) |>
    mutate(`Transportation Alternatives` = `Transportation Alternatives` / `Average Consistency Score`) |>
    mutate(`Travel Reliability` = `Travel Reliability` / `Average Consistency Score`) |>
    mutate(`Support for Centers` = `Support for Centers` / `Average Consistency Score`) |>
    mutate(`Safety` = `Safety` / `Average Consistency Score`) |>
    mutate(`Community Benefits` = `Community Benefits` / `Average Consistency Score`) |>
    select("Project Type", "County", "# of Projects", "Project Cost", "Average Project Grade", 
           "Freight", "Employment",
           "Emissions", "Puget Sound Land & Water",
           "Transportation Alternatives", "Travel Reliability", 
           "Support for Centers", "Safety", "Community Benefits")
  
  # Overall Project Summary for the Region
  r <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(count = 1, `Project Type` = project_type, County = "Region") |>
    group_by(`Project Type`, `County`) |>
    summarise(`# of Projects` = sum(count), 
              `Project Cost` = sum(.data[[cost_column]]),  
              `Average Consistency Score` = sum(.data[[overall_score_column]]),
              `Freight` = sum(.data[[freight_score_column]]),
              `Employment` = sum(.data[[employment_score_column]]),
              `Emissions` = sum(.data[[emissions_score_column]]),
              `Puget Sound Land & Water` = sum(.data[[water_score_column]]),
              `Transportation Alternatives` = sum(.data[[alternative_score_column]]),
              `Travel Reliability` = sum(.data[[reliability_score_column]]),
              `Support for Centers` = sum(.data[[centers_score_column]]),
              `Safety` = sum(.data[[safety_score_column]]),
              `Community Benefits` = sum(.data[[community_score_column]])) |>
    as_tibble() |>
    mutate(`Average Project Grade` = ((`Average Consistency Score`/`# of Projects`)/total_possible_score)*100) |>
    mutate(`Freight` = `Freight` / `Average Consistency Score`) |>
    mutate(`Employment` = `Employment` / `Average Consistency Score`) |>
    mutate(`Emissions` = `Emissions` / `Average Consistency Score`) |>
    mutate(`Puget Sound Land & Water` = `Puget Sound Land & Water` / `Average Consistency Score`) |>
    mutate(`Transportation Alternatives` = `Transportation Alternatives` / `Average Consistency Score`) |>
    mutate(`Travel Reliability` = `Travel Reliability` / `Average Consistency Score`) |>
    mutate(`Support for Centers` = `Support for Centers` / `Average Consistency Score`) |>
    mutate(`Safety` = `Safety` / `Average Consistency Score`) |>
    mutate(`Community Benefits` = `Community Benefits` / `Average Consistency Score`) |>
    select("Project Type", "County", "# of Projects", "Project Cost", "Average Project Grade", 
           "Freight", "Employment",
           "Emissions", "Puget Sound Land & Water",
           "Transportation Alternatives", "Travel Reliability", 
           "Support for Centers", "Safety", "Community Benefits")
  
  p <- bind_rows(p, r) |>
    mutate(County = factor(County, levels = county_order)) |>
    arrange(County)
  
  # Project Scores by County and Score Range
  prc <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(`Average Project Grade` = (.data[[overall_score_column]]/total_possible_score)*100) |>
    mutate(`Project Grade Range` = case_when(
      `Average Project Grade` <= 25 ~ "under 25%",
      `Average Project Grade` <= 50 ~ "25% to 50%",
      `Average Project Grade` <= 75 ~ "50% to 75%",
      `Average Project Grade` <= 100 ~ "over 75%")) |>
    mutate(count = 1, `Project Type` = project_type, County = .data[[county_column]]) |>
    mutate(County = str_remove_all(County, " County")) |>
    mutate(County = str_remove_all(County, " Counties \\(any combination\\)")) |>
    group_by(`Project Type`, `County`, `Project Grade Range`) |>
    summarise(`# of Projects` = sum(count)) |>
    as_tibble() |>
    pivot_wider(names_from = `Project Grade Range`, values_from = `# of Projects`)
  
  prr <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(`Average Project Grade` = (.data[[overall_score_column]]/total_possible_score)*100) |>
    mutate(`Project Grade Range` = case_when(
      `Average Project Grade` <= 25 ~ "under 25%",
      `Average Project Grade` <= 50 ~ "25% to 50%",
      `Average Project Grade` <= 75 ~ "50% to 75%",
      `Average Project Grade` <= 100 ~ "over 75%")) |>
    mutate(count = 1, `Project Type` = project_type, County = "Region") |>
    group_by(`Project Type`, `County`, `Project Grade Range`) |>
    summarise(`# of Projects` = sum(count)) |>
    as_tibble() |>
    pivot_wider(names_from = `Project Grade Range`, values_from = `# of Projects`)
  
  prc <- bind_rows(prc, prr) |>
    mutate(County = factor(County, levels = county_order)) |>
    arrange(County)
  
  # Add the Score Ranges
  p <- left_join(p, prc, by=c("Project Type", "County")) |>
    select("Project Type", "County", "# of Projects", "Project Cost", "Average Project Grade",
           contains("%"),
           "Freight", "Employment",
           "Emissions", "Puget Sound Land & Water",
           "Transportation Alternatives", "Travel Reliability", 
           "Support for Centers", "Safety", "Community Benefits")
  
  return(p)
  
}

# Database Connection -----------------------------------------------------
c <- dbConnect(odbc::odbc(),
               driver = "ODBC Driver 17 for SQL Server",
               server = "SQLserver",
               database = "MTPData",
               trusted_connection = "yes") 

q <- "SELECT * FROM dbo.mtpfn_ReviewProjects(68)"
rtp_projects <- dbGetQuery(c, q)
dbDisconnect(c)
rm(c, q)

# Temporary fix to missing county data for a few projects in the database
rtp_projects <- rtp_projects |>
  mutate(`Sponsor Type` = case_when(
    .data[[sponsor_column]] %in% local_transit_sponsors ~ "Local Transit",
    .data[[sponsor_column]] %in% regional_transit_sponsors ~ "Regional Transit",
    .data[[sponsor_column]] %in% county_sponsors ~ "Counties",
    .data[[sponsor_column]] %in% tribal_sponsors ~ "Counties",
    .data[[sponsor_column]] %in% port_sponsors ~ "Counties",
    .data[[sponsor_column]] %in% wsdot_sponsors ~ "WSDOT",
    .data[[sponsor_column]] %in% wsf_sponsors ~ "WSF")) |>
  mutate(`Sponsor Type` = replace_na(`Sponsor Type`, "Cities")) |>
  mutate(!!county_column := case_when(
    .data[[project_id_column]] %in% king_county_projects ~ "King County",
    .data[[project_id_column]] %in% pierce_county_projects ~ "Pierce County",
    !(.data[[project_id_column]] %in% c(king_county_projects, pierce_county_projects)) ~ .data[[county_column]]))

# Total Project Summary ---------------------------------------------------
all_projects <- create_project_scoring_summary(project_type = "All")
transit_projects <- create_project_scoring_summary(project_type = "Transit", scope_elements = transit_project_columns)
nonmotorized_projects <- create_project_scoring_summary(project_type = "Non-Motorized", scope_elements = nonmotorized_project_columns)
ferry_projects <- create_project_scoring_summary(project_type = "Ferry", scope_elements = ferry_project_columns)
bridge_projects <- create_project_scoring_summary(project_type = "Bridge", scope_elements = bridge_project_columns)
interchange_projects <- create_project_scoring_summary(project_type = "Interchange", scope_elements = interchange_project_columns)
mpo_projects <- create_project_scoring_summary(project_type = "M,P & O", scope_elements = maintenance_preservation_operations_project_columns)
roadway_projects <- create_project_scoring_summary(project_type = "Roadway", scope_elements = roadway_project_columns)

project_summary <- bind_rows(all_projects, transit_projects, nonmotorized_projects, ferry_projects, bridge_projects, interchange_projects, mpo_projects, roadway_projects)

project_summary <- project_summary |>
  mutate(`under 25%` = replace_na(`under 25%`, 0)) |>
  mutate(`25% to 50%` = replace_na(`25% to 50%`, 0)) |>
  mutate(`50% to 75%` = replace_na(`50% to 75%`, 0)) |>
  mutate(`over 75%` = replace_na(`over 75%`, 0)) |>
  select("Project Type", "County", "# of Projects", "Project Cost", "Average Project Grade",
         "under 25%", "25% to 50%", "50% to 75%", "over 75%",
         "Freight", "Employment",
         "Emissions", "Puget Sound Land & Water",
         "Transportation Alternatives", "Travel Reliability", 
         "Support for Centers", "Safety", "Community Benefits")

project_summary <- project_summary |>
  arrange(`County`, `Project Type`)

rm(all_projects, transit_projects, nonmotorized_projects, ferry_projects, bridge_projects, interchange_projects, mpo_projects, roadway_projects)
write_csv(project_summary, "output/project_summary_cost_scores.csv")

# Project Costs by Year and Sponsor Type ----------------------------------
years <- 2025:2050
project_costs_by_year <- data.frame(`Sponsor Type` = character(0),  # character column
                 setNames(lapply(years, function(x) numeric(0)), as.character(years)),
                 stringsAsFactors = FALSE)
colnames(project_costs_by_year) <- c("Sponsor Type", as.character(years))

p <- rtp_projects |>
  select("Sponsor Type", all_of(completion_column), all_of(cost_column)) |>
  group_by(`Sponsor Type`, .data[[completion_column]]) |>
  summarise(`Project Cost` = sum(.data[[cost_column]])) |>
  as_tibble() |>
  arrange(.data[[completion_column]]) |>
  pivot_wider(names_from = all_of(completion_column), values_from = `Project Cost`)

project_costs_by_year <- bind_rows(project_costs_by_year, p) |>
  arrange(`Sponsor Type`)

project_costs_by_year <- project_costs_by_year %>%
  bind_rows(
    project_costs_by_year %>%
      summarise(
        `Sponsor Type` = "Total",
        across(where(is.numeric), sum, na.rm = TRUE)
      )
  )

project_costs_by_year <- project_costs_by_year |>
  mutate(across(where(is.numeric), ~replace_na(., 0)))

rm(p)
write_csv(project_costs_by_year, "output/project_cost_by_sponsor_type.csv")

# Projects by Agency ------------------------------------------------------
city_Projects <- rtp_projects |>
  select(all_of(sponsor_column), all_of(cost_column)) |>
  mutate(!!sponsor_column := str_to_title(.data[[sponsor_column]]), count = 1) |>
  group_by(.data[[sponsor_column]]) |>
  summarise(`# of Projects` = sum(count), `Total Project Cost in 2026 $'s` = sum(.data[[cost_column]])) |>
  as_tibble()

write_csv(city_Projects, "output/total_project_cost_sponsor.csv")

# WSDOT Projects ----------------------------------------------------------
wsdot_projects <- rtp_projects |>
  select(all_of(project_id_column), all_of(project_title_column), all_of(sponsor_column), 
         all_of(start_column), all_of(completion_column), 
         all_of(cost_year_column), all_of(cost_column), all_of(current_funds_column), all_of(original_cost_column),
         all_of(current_funds_descriptions_column)) |>
  filter(.data[[sponsor_column]] %in% wsdot_sponsors) |>
  mutate(`Move Ahead Washington` = case_when(
    str_detect(.data[[current_funds_descriptions_column]], "MAW") ~ "Yes",
    str_detect(.data[[current_funds_descriptions_column]], "Move Ahead Washington") ~ "Yes")) |>
  mutate(`Move Ahead Washington` = replace_na(`Move Ahead Washington`, "No")) |>
  mutate(`% Funded` = round(.data[[current_funds_column]]/.data[[original_cost_column]],2)) |>
  select(all_of(project_id_column), all_of(project_title_column), all_of(sponsor_column), 
         all_of(start_column), all_of(completion_column), 
         all_of(cost_column), all_of(current_funds_descriptions_column), "Move Ahead Washington", "% Funded")

write_csv(wsdot_projects, "output/total_wsdot_project_cost.csv")
