# Libraries -------------------------------
# """""""""""""""""" ----------------------
library(dplyr)
library(sf) # simple features packages for handling vector GIS data
library(stringr) # string manipulation
library(httr) # generic webservice package
library(tidyverse) # a suite of packages for data wrangling, transformation, plotting, ...
library(ows4R) # interface for OGC webservices
library(RPostgres)
library(readr)

# Settings
local_folder <- "C:/temp/fusions/"

### NCCN OSM DB connection ----
readRenviron("C:/projects/pgn-data-airflow/.Renviron")
osm_host_name <- Sys.getenv("POSTGRES_HOST_NAME_OSM")
osm_user <- Sys.getenv("POSTGRES_USER_OSM")
osm_password <- Sys.getenv("POSTGRES_PASSWORD_OSM")
osm_db_name <- Sys.getenv("POSTGRES_DB_NAME_OSM")

get_con_osm<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=osm_user, 
                      password=osm_password,
                      host=osm_host_name,
                      dbname=osm_db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}



# download wegenregister ----

# SKIP THIS PART: the WFS download appears to fail

## set up WFS download

# WFS download as explained on https://inbo.github.io/tutorials/tutorials/spatial_wfs_services/

# Define the base WFS URL
wfs_service <- "https://geo.api.vlaanderen.be/Wegenregister/wfs"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs_service)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "Wegenregister:Wegsegment",
    srsName = "EPSG:31370",
    startIndex = start_index,
    maxFeatures = 50000,
    outputFormat = "application/json"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 50000
has_more_features <- TRUE

# Loop to fetch data in batches
while (has_more_features) {
  # Build the request URL for the current batch
  request_url <- build_request_url(start_index)
  
  # Fetch the data
  batch <- read_sf(request_url)
  
  # Check if there are no more features to fetch
  if (nrow(batch) == 0) {
    has_more_features <- FALSE
  } else {
    # Append the fetched features to the list
    all_features <- append(all_features, list(batch))
    # Increment the start index for the next batch
    start_index <- start_index + batch_size
  }
}


# Combine all fetched features into a single data frame
wegenregister <- do.call(rbind, all_features)

wegenregister_backup<-wegenregister

# set all columns to lowercase
wegenregister <- wegenregister %>% 
  rename_all(tolower)

# Print the number of features retrieved
cat("Total road segments retrieved:", nrow(wegenregister), "\n")

## save a local copy
st_write(wegenregister, paste0(local_folder,"wegenregister_wfs.gpkg"))



## END SKIPPED PART



# download the WFS with QGIS, and save as gpkg
wegenregister_orig <- st_read(paste0(local_folder,"wegenregister_20250109.gpkg"))
wegenregister <- wegenregister_orig

# set all names lowercase
wegenregister <- wegenregister %>% 
  rename_all(tolower)

wegenregister <- wegenregister %>%
  select(
    objectid,
    linkerstraatnaam, rechterstraatnaam,wegbeheerder,labelwegbeheerder, linkerstraatnaamobjectid, rechterstraatnaamobjectid) %>%
  filter(!is.na(linkerstraatnaam) | !is.na(rechterstraatnaam))

## done preparing




# select streets by new name & new municipality based on change list ----

## get the affected addresses ----
### download the change file 
url <- "https://assets.vlaanderen.be/raw/upload/v1734692529/repositories-prd/Verschilbestanden_Fusies_20-12-2024_wmyxyo.zip"
local_zip_file <- file.path(local_folder, "Verschilbestanden_Fusies_20-12-2024_wmyxyo.zip")

# Download the file
download.file(url, destfile = local_zip_file, mode = "wb")

# Unzip the file
unzip(local_zip_file, exdir = local_folder)

# load the relevant data
address_file <- file.path(local_folder, "fusies-verschilbestand-adressen-20241220.csv")
address_data <- readr::read_delim(address_file, delim = ";")


# replace spaces in column names with _
colnames(address_data) <- stringr::str_replace_all(colnames(address_data), " ", "_")
# make all column names lower caps
colnames(address_data) <- tolower(colnames(address_data))

# filter cases where old and new straatnaam are not the same
address_data <- address_data %>% 
  filter(oud_straatnaam != nieuw_straatnaam)

# if oud_straatnaam != nieuw_straatnaam but oud_huisnummer=nieuw_huisnummer set addr_street_only to 1
# if oud_huisnummer!=nieuw_huisnummer set addr_number_change to 1 
address_data <- address_data %>%
  mutate(
    addr_street_only = ifelse(oud_straatnaam != nieuw_straatnaam & oud_huisnummer == nieuw_huisnummer, 1, 0),
    addr_number_change = ifelse(oud_huisnummer != nieuw_huisnummer, 1, 0)
  )

# from nieuw_straatnaamid...13 create nieuw_straatid with just the number after https://data.vlaanderen.be/id/straatnaam/
address_data <- address_data %>%
  mutate(
    nieuw_straatid = stringr::str_extract(nieuw_straatnaamid...13, "\\d+")
  )

# aggregate by unique combination of nieuw_straatnaam and nieuw_gemeente. Add the sum of addr_street_only across rows as well as addr_number_change
address_agg <- address_data %>%
  group_by(nieuw_gemeentenaam,nieuw_straatnaam, nieuw_straatid) %>%
  summarise(
    total_addr_street_only = sum(addr_street_only, na.rm = TRUE),
    total_addr_number_change = sum(addr_number_change, na.rm = TRUE),
    .groups = "drop" # Ensures the result is ungrouped after summarisation
  )



## extend with affected streets ----
# because some changed streets have no addresses!
# open the Verschilbestand straatnamen staging.csv file
streetname_file <- file.path(local_folder, "fusies-verschilbestand-straatnamen-20241218.csv")
streetname_data <- readr::read_delim(streetname_file, delim = ";")

# replace spaces in column names with _
colnames(streetname_data) <- stringr::str_replace_all(colnames(streetname_data), " ", "_")
# make all column names lower caps
colnames(streetname_data) <- tolower(colnames(streetname_data))

streetname_data <- streetname_data %>% 
  mutate(
    nieuw_straatid = stringr::str_extract(nieuw_straatnaamid, "\\d+")
  )


# make a list of streetname changes per municipality----
streetname_data <- streetname_data %>% 
  select(oud_gemeentenaam, oud_straatnaam, nieuw_gemeentenaam, nieuw_straatnaam, nieuw_straatid)

# filter cases where old and new straatnaam are not the same
streetname_data <- streetname_data %>% 
  filter(oud_straatnaam != nieuw_straatnaam)

# aggregate by nieuw_straatnaam & nieuw_gemeente. Keep oud_straatnaam as a concat of unique value over the merged records
street_agg <- streetname_data %>% 
  group_by(nieuw_gemeentenaam,nieuw_straatnaam,nieuw_straatid) %>%
  summarise(
    oud_straatnaam = paste(unique(oud_straatnaam), collapse = ", "),
    .groups = "drop" # Ensures the result is ungrouped after summarisation
  )

# classify between renumbering & simple renaming on the addresses ----
affected_streets <- full_join(
  address_agg,
  street_agg,
  by = c("nieuw_straatnaam", "nieuw_gemeentenaam", "nieuw_straatid")
)

# keep only wegenregister segments which are affected ----



# make straatnaam: if they are the same use linkerstraatnaam. If either is empty, use the other. If different and both not empty, use a ; to merge them
wegenregister <- wegenregister %>%
  mutate(
    straatnaam = case_when(
      linkerstraatnaam == rechterstraatnaam ~ linkerstraatnaam,
      is.na(linkerstraatnaam) ~ rechterstraatnaam,
      is.na(rechterstraatnaam) ~ linkerstraatnaam,
      TRUE ~ paste(linkerstraatnaam, ";", rechterstraatnaam)
    )
  ) %>%
  select(-linkerstraatnaam, -rechterstraatnaam)

joined_new <- affected_streets %>%
  select(nieuw_straatid, oud_straatnaam)

# set linkerstraatnaamobjectid and rechterstraatnaamobjectid to char
wegenregister$linkerstraatnaamobjectid <- as.character(wegenregister$linkerstraatnaamobjectid)
wegenregister$rechterstraatnaamobjectid <- as.character(wegenregister$rechterstraatnaamobjectid)

# left join joined_new to wegenregister 
wegenregister <- left_join(
  wegenregister,
  joined_new,
  by = c("linkerstraatnaamobjectid" = "nieuw_straatid")
)
# rename oud_straatnaam to oud_straatnaam_links
wegenregister <- wegenregister %>%
  rename(oud_straatnaam_links = oud_straatnaam)

# do the same for rechterstraatnaam
wegenregister <- left_join(
  wegenregister,
  joined_new,
  by = c("rechterstraatnaamobjectid" = "nieuw_straatid")
)
wegenregister <- wegenregister %>%
  rename(oud_straatnaam_rechts = oud_straatnaam)

# select only cases where oud_straatnaam_links or oud_straatnaam_rechts is not NA
wegenregister <- wegenregister %>%
  filter(!is.na(oud_straatnaam_links) | !is.na(oud_straatnaam_rechts))



# do the same with linkerstraatnaamobjectid and rechterstraatnaamobjectid
wegenregister <- wegenregister %>%
  mutate(
    nieuw_straatid = case_when(
      linkerstraatnaamobjectid == rechterstraatnaamobjectid ~ as.character(linkerstraatnaamobjectid),
      linkerstraatnaamobjectid == -9 | is.na(oud_straatnaam_links) ~ as.character(rechterstraatnaamobjectid),
      rechterstraatnaamobjectid == -9 | is.na(oud_straatnaam_rechts) ~ as.character(linkerstraatnaamobjectid),
      TRUE ~ paste(as.character(linkerstraatnaamobjectid), 
                   as.character(rechterstraatnaamobjectid), 
                   sep = ";")
    )
  ) %>%
  select(-linkerstraatnaamobjectid, -rechterstraatnaamobjectid)





# aggregate wegenregister by straatid

# create nieuw_gemeentenaam if the labelwegbeheerder starts with  "Gemeente " or "Stad ". Remove that string from the result
wegenregister <- wegenregister %>%
  mutate(
    gemeentenaam = case_when(
      str_detect(labelwegbeheerder, "^Gemeente ") ~ str_remove(labelwegbeheerder, "^Gemeente "),
      str_detect(labelwegbeheerder, "^Stad ") ~ str_remove(labelwegbeheerder, "^Stad "),
      TRUE ~ NA_character_
    )
  )


# join affected streets based on straatid
wegenregister_extended <- full_join(
  wegenregister,
  affected_streets,
  by = "nieuw_straatid"
)

wegenregister_extended<-wegenregister_extended %>%
  filter(!is.na(nieuw_straatnaam))

# filter weird cases to new dataset (onjectid NA)
wegenregister_missing <- as.data.frame(wegenregister_extended) %>%
  filter(is.na(objectid)) %>%
  select(nieuw_straatid, nieuw_straatnaam, nieuw_gemeentenaam, oud_straatnaam)
# write to csv
write_csv(wegenregister_missing, paste0(local_folder,"wegenregister_missing.csv"))

wegenregister_extended<-wegenregister_extended %>%
  filter(!is.na(objectid))




# find old street name in neigbourhood ----
# Initialize an empty list to collect processed items
processed_items <- list()
run_counter <- 0
# Loop through each unique nieuw_straatid in the data
for (straat_id in unique(wegenregister_extended$nieuw_straatid)) {
  
  run_counter <- run_counter + 1
  
  # Print the current run number
  print(paste("Processing item", run_counter))
  
  ## Calculate a bbox for a single selected road based on the old name
  # Find and filter for the current straat_id
  loop_item <- wegenregister_extended %>%
    filter(nieuw_straatid == straat_id)
  plot(loop_item$geom)
  query_item<-loop_item
  
  
  # Summarize and group by nieuw_straatid, and aggregate geometries
  loop_item <- loop_item %>%
    group_by(nieuw_straatid) %>%
    summarize(geometry = st_union(geom), .groups = "drop")
  
  # Add bbox geometry & make the bbox larger (buffer 200m)
  loop_item <- loop_item %>%
    mutate(
      bbox = list(st_bbox(st_transform(st_buffer(geometry, 200),4326))) %>% as.list()
      )
    
  
  # Transform to 4326
  loop_item$geometry <- st_transform(loop_item$geometry, 4326)
  
  # Combine nieuw_straatnaam and oud_straatnaam, then get unique values
  query_string <- unique(c(
    query_item$nieuw_straatnaam,
    unlist(str_split(query_item$oud_straatnaam, ", "))
  ))

  # Create the PostgreSQL query string
  query_string0 <- paste0("name='", query_string, "'", collapse=" OR ")
  query_string1 <- paste0("tags->'addr:street'='", query_string, "'", collapse=" OR ")
  query_string2 <- paste0("tags->'old_name'='", query_string, "'", collapse=" OR ")
  
  query_string <- paste0(query_string0," OR ", query_string1," OR ", query_string2)
  query_string <- paste0("(", query_string, ")") # Add parentheses for safety
  
  # Extract bbox values
  bbox_values <- loop_item$bbox[[1]]
  xmin <- bbox_values["xmin"]
  ymin <- bbox_values["ymin"]
  xmax <- bbox_values["xmax"]
  ymax <- bbox_values["ymax"]
  
  # Connect to PostgreSQL
  con_pg <- get_con_osm()
  
  # Construct the SQL query to fetch OSM data
  tags_query <- paste0("SELECT name, (tags -> 'old_name') = 'old_name' AS old_name, (tags -> 'addr:street') = 'addr:street' AS addr_street FROM public.planet_osm_line WHERE ", query_string,
                       " AND ST_Transform(way, 4326) && ST_MakeEnvelope(", xmin, ", ", ymin, ", ", xmax, ", ", ymax, ", 4326)")
  
  ### Download highway with name=oud_naam and anything with addr:street=oud_naam
  # Execute the query
  osm_item <- dbGetQuery(con_pg, tags_query)
  
  # Disconnect from the database
  dbDisconnect(con_pg)
  
  # Count the number of cases old name or new name exists
  straatnamen_to_check <- unique(c(query_item$nieuw_straatnaam, query_item$oud_straatnaam))
  
  nieuw_straatnaam <- unique(query_item$nieuw_straatnaam)
  oud_straatnaam <- unique(query_item$oud_straatnaam)
  
  old_name_exists <- any(oud_straatnaam %in% osm_item$name) | any(oud_straatnaam %in% osm_item$addr_street)
  new_name_exists <- any(nieuw_straatnaam %in% osm_item$name)
  
  ## Note: the old name not being found can also mean that the old name was mis-spelled. So we should only delete cases that do not have the old name nearby IF we do find the new name nearby
  # Keep the case if old name exists or if new name exists (even without old name)
  keep_the_case <- old_name_exists | (!old_name_exists & !new_name_exists)
  
  # Add the old and new names to the set
  loop_item <- loop_item %>%
    mutate(
      nieuw_straatnaam = unique(query_item$nieuw_straatnaam),
      oude_straatnamen = unique(query_item$oud_straatnaam),
      old_name_exists = old_name_exists,
      new_name_exists = new_name_exists,
      keep_the_case = keep_the_case,
      xmin = xmin,
      ymin = ymin,
      xmax = xmax,
      ymax = ymax
    )
  
  # Append the current processed item to the list
  processed_items[[straat_id]] <- loop_item
}

# Combine all processed items into a single dataset
processed_items_df <- bind_rows(processed_items)



# Add an Overpass query column
maproulette_items <- processed_items_df %>%
  rowwise() %>%
  mutate(
    overpass = {
      # Split oude_straatnamen by comma
      oude_names <- str_split(oude_straatnamen, ", ")[[1]]
      
      # Create queries for each part of oude_straatnamen
      queries <- sapply(oude_names, function(oude_name) {
        oude_name <- str_trim(oude_name)
        paste0(
          "way[\"name\"=\"", oude_name, "\"](", ymin, ",", xmin, ",", ymax, ",", xmax, ");",
          "way[\"name:right\"=\"", oude_name, "\"](", ymin, ",", xmin, ",", ymax, ",", xmax, ");",
          "way[\"name:left\"=\"", oude_name, "\"](", ymin, ",", xmin, ",", ymax, ",", xmax, ");",
          "nwr[\"addr:street\"=\"", oude_name, "\"](", ymin, ",", xmin, ",", ymax, ",", xmax, ");"
        )
      })
      
      # Combine all queries into a single string
      paste0("[out:json][timeout:60];(", paste(queries, collapse = " "), ");out body;>;out skel qt;")
    }
  ) %>%
  ungroup()






# filter only columns we need
# keep all cases where an old street name still exists
maproulette_items <- maproulette_items %>%
  filter(keep_the_case) %>%
  select(id=nieuw_straatid, nieuw_straatnaam, oude_straatnamen, old_name_exists, new_name_exists, overpass)

# add the classification of the change: renumbering or just renaming
change_class <- as.data.frame(wegenregister_extended) %>%
  group_by(nieuw_straatid) %>%
  summarize(
    total_addr_street_only_sum = sum(total_addr_street_only, na.rm = TRUE),
    total_addr_number_change_sum = sum(total_addr_number_change, na.rm = TRUE),
    .groups = "drop"
  )

change_class <- change_class %>%
  mutate(
    change_class = case_when(
      total_addr_street_only_sum > 0 & total_addr_number_change_sum == 0 ~ "rename only",
      total_addr_street_only_sum == 0 & total_addr_number_change_sum > 0 ~ "renumbering only",
      total_addr_street_only_sum > 0 & total_addr_number_change_sum > 0 ~ "renumbering",
      total_addr_street_only_sum == 0 & total_addr_number_change_sum == 0 ~ "no addresses affected",
      TRUE ~ "unknown"
    )
  ) %>%
  select(id=nieuw_straatid, change_class)

# join to maproulette items
maproulette_items <- maproulette_items %>%
  left_join(change_class, by = "id")



# save to maproulette
## save as geojson
st_write(maproulette_items, paste0(local_folder,format(Sys.time(), "%Y%m%d_%H%M%S"),"_maproulette_items.geojson"))
