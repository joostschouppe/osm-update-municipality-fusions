local_directory <- "C:/temp/fusions"  
municipality <- "Beveren"

# download the changed addresses etc ----
### download the zip file 
url <- "https://assets.vlaanderen.be/raw/upload/v1730364010/Verschilbestanden_staging_ltaic1.zip"
local_zip_file <- file.path(local_directory, "Verschilbestanden_staging_ltaic1.zip")

# Download the file
download.file(url, destfile = local_zip_file, mode = "wb")

# Unzip the file
unzip(local_zip_file, exdir = local_directory)



# load the streetname file ----
# load the address file ----
# open the Verschilbestand straatnamen staging.csv file
streetname_file <- file.path(local_directory, "Verschilbestand straatnamen staging.csv")
streetname_data <- readr::read_delim(streetname_file, delim = ";")

# replace spaces in column names with _
colnames(streetname_data) <- stringr::str_replace_all(colnames(streetname_data), " ", "_")
# make all column names lower caps
colnames(streetname_data) <- tolower(colnames(streetname_data))

# make a list of streetname changes per municipality----
streetname_data <- streetname_data %>% 
  select(oud_gemeentenaam, oud_straatnaam, nieuw_gemeentenaam, nieuw_straatnaam)

# filter cases where old and new straatnaam are not the same
streetname_data <- streetname_data %>% 
  filter(oud_straatnaam != nieuw_straatnaam)

# write the list for a municipality to a file with a list like way["highway"]["name"="Lepelstraat"](area.searchArea); ----
# create a column "query" with the string "way["highway"]["name"="oud_straatnaam"](area.searchArea);
streetname_data <- streetname_data %>% 
  mutate(query = paste0("way[\"highway\"][\"name\"=\"", oud_straatnaam, "\"](area.searchArea);"))


# select streets that get duplicated (oud streets that have more than one nieuw street)
streetname_data_duplicates <- streetname_data %>% 
  group_by(oud_gemeentenaam, oud_straatnaam) %>% 
  filter(n() > 1) %>% 
  ungroup()
# write to a flat txt file
write.table(streetname_data_duplicates, file = file.path(local_directory, paste0("DUPLICATE_streetname_changes_",municipality,".txt")), row.names = FALSE, col.names = FALSE, quote = FALSE)

# select only the streets that are not duplicated
streetname_data <- streetname_data %>% 
  group_by(oud_gemeentenaam, oud_straatnaam) %>% 
  filter(n() == 1) %>% 
  ungroup()


# select a municipality
streetname_data_output <- streetname_data %>% 
  filter(oud_gemeentenaam == municipality) %>%
  select(query)


# write to a flat txt file
write.table(streetname_data_output, file = file.path(local_directory, paste0("streetname_changes_",municipality,".txt")), row.names = FALSE, col.names = FALSE, quote = FALSE)

streetname_data_output <- streetname_data %>% 
  filter(oud_gemeentenaam == municipality) %>%
  select(oud_straatnaam, nieuw_straatnaam)


# download relevant data to Level0 with a query like https://overpass-turbo.eu/s/1UDd
# save the output to a txt file in the local_directory with filename like beveren_level0_original ----
# load the output file----
level0_file <- file.path(local_directory, paste0(municipality, "_level0_original.txt"))

# Read the file into a dataframe
level0_data <- read.table(level0_file, header = FALSE, sep = "\t", stringsAsFactors = FALSE, fill = TRUE)


# apply changes to the data----
  # add proposed:name with the new name if proposed:name does not exist yet----
  # add fixme:name=WikiProject_Belgium/Municipality_Fusions/2025----

# Initialize an empty list to collect all rows
new_data <- list()

# Iterate over each row of level0_data
for (i in 1:nrow(level0_data)) {
  current_row <- level0_data[i, , drop = FALSE]  # Keep current row as a dataframe
  new_data[[length(new_data) + 1]] <- current_row  # Keep the original row
  
  # Check if the row contains any street name from 'oud_straatnaam'
  for (j in 1:nrow(streetname_data_output)) {
    oud_name <- streetname_data_output$oud_straatnaam[j]
    nieuw_name <- streetname_data_output$nieuw_straatnaam[j]
    
    # If the current row contains 'name = <oud_name>', add the proposed row
    if (grepl(paste0("name = ", oud_name), current_row$V1)) {
      # Add the proposed name row
      new_row <- data.frame(V1 = paste0("  proposed:name = ", nieuw_name), stringsAsFactors = FALSE)
      new_data[[length(new_data) + 1]] <- new_row  # Add the new row after the match
      
      # Add the fixme:name row
      fixme_row <- data.frame(V1 = "  fixme:name = WikiProject_Belgium/Municipality_Fusions/2025", stringsAsFactors = FALSE)
      new_data[[length(new_data) + 1]] <- fixme_row  # Add the fixme row after the proposed one
    }
  }
}

# Convert the list back to a dataframe
level0_data_updated <- do.call(rbind, new_data)


# save to a flat txt file----
write.table(level0_data_updated, file = file.path(local_directory, paste0(municipality, "_level0_updated.txt")), row.names = FALSE, col.names = FALSE, quote = FALSE)


# open in level0----
# apply the change----
# manually deal with duplicates----

  
  
# NEXT: Update on 31/12----
  # select where fixme:name=WikiProject_Belgium/Municipality_Fusions/2025 & proposed:name exists
  # set name to old_name
  # set proposed:name to name
  # set existing wikidata etymology references to old:wikidata