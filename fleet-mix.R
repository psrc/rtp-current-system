library(tidyverse)

un <- Sys.getenv("USERNAME")
data_dir <- file.path("C:/Users",str_to_lower(un),"Downloads")

total_registration_activity_file <- file.path(data_dir, "Vehicle_Registration_Summary_20250502.csv")
total_vehicle_registrations <- read_csv(total_registration_activity_file, show_col_types = FALSE)

total_registrations <- total_vehicle_registrations |>
  rename(Year = "Calendar Year") |>
  filter(`Vehicle Type` %in% c("MULTIPURPOSE PASSENGER VEHICLE (MPV)", "PASSENGER CAR", "TRUCK", "MOTORCYCLE")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Mild HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "PHEV \\(Plug-in Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "HEV \\(Hybrid Electric Vehicle\\) - Level Unknown", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Strong HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>  
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "BEV \\(Battery Electric Vehicle\\)", "Electric")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "ICE \\(Internal Combustion Engine\\)", "Gasoline")) |>
  group_by(`Year`, `Primary Use`, `Electrification Level`) |>
  summarise(`Vehicles` = sum(`Transaction Count`)) |>
  as_tibble() |>
  pivot_wider(names_from = c(`Primary Use`, `Electrification Level`), values_from = Vehicles, names_sep = ": ") |>
  mutate(across(where(is.numeric), ~replace_na(., 0))) |>
  mutate(`Electric` = `Motorcycle: Electric` + `Passenger Vehicle: Electric` + `Truck: Electric`) |> 
  mutate(`Hybrid` = `Motorcycle: Hybrid` + `Passenger Vehicle: Hybrid` + `Truck: Hybrid`) |>
  mutate(`Gasoline` = `Motorcycle: Gasoline` + `Passenger Vehicle: Gasoline` + `Truck: Gasoline`) |>
  mutate(`Total` = `Electric` + `Hybrid` +`Gasoline`) |>
  mutate(`% Electric` = `Electric` / `Total`) |>
  mutate(`% Hybrid` = `Hybrid` / `Total`) |>
  mutate(`% Gasoline` = `Gasoline` / `Total`) |>
  select("Year", "Electric", "Hybrid", "Gasoline", "Total", "% Electric", "% Hybrid", "% Gasoline")

new_registrations <- total_vehicle_registrations |>
  filter(`Transaction Type` == "Original Registration") |>
  rename(Year = "Calendar Year") |>
  filter(`Vehicle Type` %in% c("MULTIPURPOSE PASSENGER VEHICLE (MPV)", "PASSENGER CAR", "TRUCK", "MOTORCYCLE")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Mild HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "PHEV \\(Plug-in Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "HEV \\(Hybrid Electric Vehicle\\) - Level Unknown", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Strong HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>  
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "BEV \\(Battery Electric Vehicle\\)", "Electric")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "ICE \\(Internal Combustion Engine\\)", "Gasoline")) |>
  group_by(`Year`, `Primary Use`, `Electrification Level`) |>
  summarise(`Vehicles` = sum(`Transaction Count`)) |>
  as_tibble() |>
  pivot_wider(names_from = c(`Primary Use`, `Electrification Level`), values_from = Vehicles, names_sep = ": ") |>
  mutate(across(where(is.numeric), ~replace_na(., 0))) |>
  mutate(`Electric` = `Motorcycle: Electric` + `Passenger Vehicle: Electric` + `Truck: Electric`) |> 
  mutate(`Hybrid` = `Motorcycle: Hybrid` + `Passenger Vehicle: Hybrid` + `Truck: Hybrid`) |>
  mutate(`Gasoline` = `Motorcycle: Gasoline` + `Passenger Vehicle: Gasoline` + `Truck: Gasoline`) |>
  mutate(`Total` = `Electric` + `Hybrid` +`Gasoline`) |>
  mutate(`% Electric` = `Electric` / `Total`) |>
  mutate(`% Hybrid` = `Hybrid` / `Total`) |>
  mutate(`% Gasoline` = `Gasoline` / `Total`) |>
  select("Year", "Electric", "Hybrid", "Gasoline", "Total", "% Electric", "% Hybrid", "% Gasoline")

used_registrations <- total_vehicle_registrations |>
  filter(`Transaction Type` == "Registration at time of Transfer") |>
  rename(Year = "Calendar Year") |>
  filter(`Vehicle Type` %in% c("MULTIPURPOSE PASSENGER VEHICLE (MPV)", "PASSENGER CAR", "TRUCK", "MOTORCYCLE")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Mild HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "PHEV \\(Plug-in Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "HEV \\(Hybrid Electric Vehicle\\) - Level Unknown", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Strong HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>  
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "BEV \\(Battery Electric Vehicle\\)", "Electric")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "ICE \\(Internal Combustion Engine\\)", "Gasoline")) |>
  group_by(`Year`, `Primary Use`, `Electrification Level`) |>
  summarise(`Vehicles` = sum(`Transaction Count`)) |>
  as_tibble() |>
  pivot_wider(names_from = c(`Primary Use`, `Electrification Level`), values_from = Vehicles, names_sep = ": ") |>
  mutate(across(where(is.numeric), ~replace_na(., 0))) |>
  mutate(`Electric` = `Motorcycle: Electric` + `Passenger Vehicle: Electric` + `Truck: Electric`) |> 
  mutate(`Hybrid` = `Motorcycle: Hybrid` + `Passenger Vehicle: Hybrid` + `Truck: Hybrid`) |>
  mutate(`Gasoline` = `Motorcycle: Gasoline` + `Passenger Vehicle: Gasoline` + `Truck: Gasoline`) |>
  mutate(`Total` = `Electric` + `Hybrid` +`Gasoline`) |>
  mutate(`% Electric` = `Electric` / `Total`) |>
  mutate(`% Hybrid` = `Hybrid` / `Total`) |>
  mutate(`% Gasoline` = `Gasoline` / `Total`) |>
  select("Year", "Electric", "Hybrid", "Gasoline", "Total", "% Electric", "% Hybrid", "% Gasoline")

renewal_registrations <- total_vehicle_registrations |>
  filter(`Transaction Type` == "Registration Renewal") |>
  rename(Year = "Calendar Year") |>
  filter(`Vehicle Type` %in% c("MULTIPURPOSE PASSENGER VEHICLE (MPV)", "PASSENGER CAR", "TRUCK", "MOTORCYCLE")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Mild HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "PHEV \\(Plug-in Hybrid Electric Vehicle\\)", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "HEV \\(Hybrid Electric Vehicle\\) - Level Unknown", "Hybrid")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "Strong HEV \\(Hybrid Electric Vehicle\\)", "Hybrid")) |>  
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "BEV \\(Battery Electric Vehicle\\)", "Electric")) |>
  mutate(`Electrification Level` = str_replace_all(`Electrification Level`, "ICE \\(Internal Combustion Engine\\)", "Gasoline")) |>
  group_by(`Year`, `Primary Use`, `Electrification Level`) |>
  summarise(`Vehicles` = sum(`Transaction Count`)) |>
  as_tibble() |>
  pivot_wider(names_from = c(`Primary Use`, `Electrification Level`), values_from = Vehicles, names_sep = ": ") |>
  mutate(across(where(is.numeric), ~replace_na(., 0))) |>
  mutate(`Electric` = `Motorcycle: Electric` + `Passenger Vehicle: Electric` + `Truck: Electric`) |> 
  mutate(`Hybrid` = `Motorcycle: Hybrid` + `Passenger Vehicle: Hybrid` + `Truck: Hybrid`) |>
  mutate(`Gasoline` = `Motorcycle: Gasoline` + `Passenger Vehicle: Gasoline` + `Truck: Gasoline`) |>
  mutate(`Total` = `Electric` + `Hybrid` +`Gasoline`) |>
  mutate(`% Electric` = `Electric` / `Total`) |>
  mutate(`% Hybrid` = `Hybrid` / `Total`) |>
  mutate(`% Gasoline` = `Gasoline` / `Total`) |>
  select("Year", "Electric", "Hybrid", "Gasoline", "Total", "% Electric", "% Hybrid", "% Gasoline")
  

write_csv(total_registrations, "output/total_vehicle_registrations.csv")
write_csv(new_registrations, "output/new_vehicle_registrations.csv")
write_csv(used_registrations, "output/used_vehicle_registrations.csv")
write_csv(renewal_registrations, "output/renewal_registrations.csv")
