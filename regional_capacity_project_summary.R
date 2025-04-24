library(tidyverse)
library(DBI)

# Inputs ------------------------------------------------------------------
total_possible_score <- 90

# General Project information columns
project_id_column <- "MTPID"
cost_column <- "ScaledCost"
sponsor_column <- "Sponsor"
county_column <- "CountyName"

# Temporary Project County Fixes
king_county_projects <- c(5808, 5822, 5827, 5832, 5837, 5850)
pierce_county_projects <- c(5812, 5814, 5815, 5816)

# Scope Element Columns
bridge_project_columns <- c("Bridge Replacement or Rehabiltation", "New or Widened Bridge")

transit_project_columns <- c("Add or Remove Business Access Transit Lanes", "Park & Ride", 
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
    mutate(count = 1, `Project Type` = paste0(project_type, ": ", .data[[county_column]])) |>
    group_by(`Project Type`) |>
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
    mutate(`% of score from Freight` = `Freight` / `Average Consistency Score`) |>
    mutate(`% of score from Employment` = `Employment` / `Average Consistency Score`) |>
    mutate(`% of score from Emissions` = `Emissions` / `Average Consistency Score`) |>
    mutate(`% of score from Puget Sound Land & Water` = `Puget Sound Land & Water` / `Average Consistency Score`) |>
    mutate(`% of score from Transportation Alternatives` = `Transportation Alternatives` / `Average Consistency Score`) |>
    mutate(`% of score from Travel Reliability` = `Travel Reliability` / `Average Consistency Score`) |>
    mutate(`% of score from Support for Centers` = `Support for Centers` / `Average Consistency Score`) |>
    mutate(`% of score from Safety` = `Safety` / `Average Consistency Score`) |>
    mutate(`% of score from Community Benefits` = `Community Benefits` / `Average Consistency Score`) |>
    select("Project Type", "# of Projects", "Project Cost", "Average Project Grade", 
           "% of score from Freight", "% of score from Employment",
           "% of score from Emissions", "% of score from Puget Sound Land & Water",
           "% of score from Transportation Alternatives", "% of score from Travel Reliability", 
           "% of score from Support for Centers", "% of score from Safety", "% of score from Community Benefits")
  
  # Overall Project Summary for the Region
  r <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(count = 1, `Project Type` = paste0(project_type, ": Regionwide")) |>
    group_by(`Project Type`) |>
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
    mutate(`% of score from Freight` = `Freight` / `Average Consistency Score`) |>
    mutate(`% of score from Employment` = `Employment` / `Average Consistency Score`) |>
    mutate(`% of score from Emissions` = `Emissions` / `Average Consistency Score`) |>
    mutate(`% of score from Puget Sound Land & Water` = `Puget Sound Land & Water` / `Average Consistency Score`) |>
    mutate(`% of score from Transportation Alternatives` = `Transportation Alternatives` / `Average Consistency Score`) |>
    mutate(`% of score from Travel Reliability` = `Travel Reliability` / `Average Consistency Score`) |>
    mutate(`% of score from Support for Centers` = `Support for Centers` / `Average Consistency Score`) |>
    mutate(`% of score from Safety` = `Safety` / `Average Consistency Score`) |>
    mutate(`% of score from Community Benefits` = `Community Benefits` / `Average Consistency Score`) |>
    select("Project Type", "# of Projects", "Project Cost", "Average Project Grade", 
           "% of score from Freight", "% of score from Employment",
           "% of score from Emissions", "% of score from Puget Sound Land & Water",
           "% of score from Transportation Alternatives", "% of score from Travel Reliability", 
           "% of score from Support for Centers", "% of score from Safety", "% of score from Community Benefits")
  
  p <- bind_rows(p, r)
  
  # Project Scores by County and Score Range
  prc <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(`Average Project Grade` = (.data[[overall_score_column]]/total_possible_score)*100) |>
    mutate(`Project Grade Range` = case_when(
      `Average Project Grade` <= 25 ~ "Projects with a Grade < 25%",
      `Average Project Grade` <= 50 ~ "Projects with a Grade 25% to 50%",
      `Average Project Grade` <= 75 ~ "Projects with a Grade 50% to 75%",
      `Average Project Grade` <= 100 ~ "Projects with a Grade > 75%")) |>
    mutate(count = 1, `Project Type` = paste0(project_type, ": ", .data[[county_column]])) |>
    group_by(`Project Type`, `Project Grade Range`) |>
    summarise(`# of Projects` = sum(count)) |>
    as_tibble() |>
    pivot_wider(names_from = `Project Grade Range`, values_from = `# of Projects`)
  
  prr <- df |>
    filter(if_any(all_of(scope_elements), ~ .x > 0)) |>
    mutate(`Average Project Grade` = (.data[[overall_score_column]]/total_possible_score)*100) |>
    mutate(`Project Grade Range` = case_when(
      `Average Project Grade` <= 25 ~ "Projects with a Grade < 25%",
      `Average Project Grade` <= 50 ~ "Projects with a Grade 25% to 50%",
      `Average Project Grade` <= 75 ~ "Projects with a Grade 50% to 75%",
      `Average Project Grade` <= 100 ~ "Projects with a Grade > 75%")) |>
    mutate(count = 1, `Project Type` = paste0(project_type, ": Regionwide")) |>
    group_by(`Project Type`, `Project Grade Range`) |>
    summarise(`# of Projects` = sum(count)) |>
    as_tibble() |>
    pivot_wider(names_from = `Project Grade Range`, values_from = `# of Projects`)
  
  prc <- bind_rows(prc, prr)
  
  # Add the Score Ranges
  p <- left_join(p, prc, by="Project Type") |>
    select("Project Type", "# of Projects", "Project Cost", "Average Project Grade",
           contains("Projects with a Grade"),
           "% of score from Freight", "% of score from Employment",
           "% of score from Emissions", "% of score from Puget Sound Land & Water",
           "% of score from Transportation Alternatives", "% of score from Travel Reliability", 
           "% of score from Support for Centers", "% of score from Safety", "% of score from Community Benefits")
  
  return(p)
  
}

# Database Connection -----------------------------------------------------
c <- dbConnect(odbc::odbc(),
               driver = "ODBC Driver 17 for SQL Server",
               server = "SQLserver",
               database = "MTPData",
               trusted_connection = "yes") 

q <- "SELECT * FROM dbo.mtpfn_RevisionProjects(68)"
rtp_projects <- dbGetQuery(c, q)
dbDisconnect(c)
rm(c, q)

# Temporary fix to missing county data for a few projects in the database
rtp_projects <- rtp_projects |>
  mutate(!!county_column := case_when(
    .data[[project_id_column]] %in% king_county_projects ~ "King County",
    .data[[project_id_column]] %in% pierce_county_projects ~ "Pierce County",
    !(.data[[project_id_column]] %in% c(king_county_projects, pierce_county_projects)) ~ .data[[county_column]]))

# Total Project Summary ---------------------------------------------------
all_projects <- create_project_scoring_summary(project_type = "All Projects")
transit_projects <- create_project_scoring_summary(project_type = "Projects with Transit Elements", scope_elements = transit_project_columns)
nonmotorized_projects <- create_project_scoring_summary(project_type = "Projects with Non-Motorized Elements", scope_elements = nonmotorized_project_columns)
ferry_projects <- create_project_scoring_summary(project_type = "Projects with Ferry Elements", scope_elements = ferry_project_columns)
bridge_projects <- create_project_scoring_summary(project_type = "Projects with Bridge Elements", scope_elements = bridge_project_columns)
interchange_projects <- create_project_scoring_summary(project_type = "Projects with Interchange Elements", scope_elements = interchange_project_columns)
mpo_projects <- create_project_scoring_summary(project_type = "Projects with M,P & O Elements", scope_elements = maintenance_preservation_operations_project_columns)
roadway_projects <- create_project_scoring_summary(project_type = "Projects with Roadway Elements", scope_elements = roadway_project_columns)

project_summary <- bind_rows(all_projects, transit_projects, nonmotorized_projects, ferry_projects, bridge_projects, interchange_projects, mpo_projects, roadway_projects)

project_summary <- project_summary |>
  mutate(`Projects with a Grade < 25%` = replace_na(`Projects with a Grade < 25%`, 0)) |>
  mutate(`Projects with a Grade 25% to 50%` = replace_na(`Projects with a Grade 25% to 50%`, 0)) |>
  mutate(`Projects with a Grade 50% to 75%` = replace_na(`Projects with a Grade 50% to 75%`, 0)) |>
  mutate(`Projects with a Grade > 75%` = replace_na(`Projects with a Grade > 75%`, 0)) |>
  select("Project Type", "# of Projects", "Project Cost", "Average Project Grade",
         "Projects with a Grade < 25%", "Projects with a Grade 25% to 50%", "Projects with a Grade 50% to 75%", "Projects with a Grade > 75%",
         "% of score from Freight", "% of score from Employment",
         "% of score from Emissions", "% of score from Puget Sound Land & Water",
         "% of score from Transportation Alternatives", "% of score from Travel Reliability", 
         "% of score from Support for Centers", "% of score from Safety", "% of score from Community Benefits")

rm(all_projects, transit_projects, nonmotorized_projects, ferry_projects, bridge_projects, interchange_projects, mpo_projects, roadway_projects)
write_csv(project_summary, "output/project_summary_cost_scores.csv")
