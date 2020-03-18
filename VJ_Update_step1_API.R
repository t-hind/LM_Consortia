#' ========================================================
#' 
#'    VJ_Update_step1_API.R
#' 
#'    ****************************************
#'    
#'    Vicinity Jobs API downloader
#'    
#'    This script points to Vicinity Job's API source at
#'    http://canreporting.vicinityjobs.com/api/job-postings
#'    
#'    For each month's data, the paginate JSON files are
#'    download and stored in a sub folder of the user-defined
#'    parent folder.
#'    
#'    Requires a pointer to an access token for the API.
#' 
#' 
#'    In Arguments:
#'     - MONTH in YYYY-MM (character) format
#'     - OUTPUT folder (must already exist)
#'     - ACCESS TOKEN 
#' 
#'    ****************************************
#'     
#'    Required packages:
#'    - data.table
#'    - zoo
#'    - rstudioapi
#'    - jsonlite
#'    - httr
#' 
#' 
#' ========================================================

options(encoding = "utf8")

rm(list=ls()) # clears work space
gc()          #' garbage collector removes R's deadweight from RAM

#' 3 Preliminary Options:

#' (1) Month to download (will be name of subfolder in out_folder_parent)
VIC_month <- "2020-02" #(YYYY-MM) -- downloads 1 month at a time:


#' (2) Name Local or network drive & folder to save JSON files
out_folder_parent <- "//LMIC-Apps01/Projects/DATA/VicinityJobs/raw_data"


#' (3) Point to access token

#' set working directory to script directory (works in Rstudio only)
# install.packages('rstudioapi')
local_dir <- dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(local_dir)

source(file.path(local_dir, 'token.R'))




# Initialization ----------------------------------------------------------

#' required packages
req_pack <- c('data.table', 'zoo', 'rstudioapi', 'jsonlite', 'httr', 'utf8')

#' list of required packages not installed. Install if missing.
to_install <- req_pack[!req_pack %in% rownames(installed.packages())] 
if(length(to_install) > 0) install.packages(to_install)


# load R packages
library(rstudioapi)
library(zoo)         # for year-qtr date type
library(httr)
library(jsonlite)
library(utf8)
library(data.table)



#' Set and create output folder
if(!file.exists(out_folder_parent)) {
  stop(paste0('Cannot find folder\n', out_folder_parent))
}

vj_data_folder <- file.path(out_folder_parent, VIC_month)
if(!dir.exists(vj_data_folder)) dir.create(vj_data_folder)



# Create Date Variables ---------------------------------------------------
VIC_init <- as.Date(as.yearmon(VIC_month), 0)
VIC_date <- as.Date(as.yearmon(VIC_month), 1)


# Create Connection to API ------------------------------------------------
df = httr::GET(paste0("http://canreporting.vicinityjobs.com/api/job-postings?",
                "dateFrom=", VIC_init, "&", 
                "dateTo=",   VIC_date, 
                "&page=1"),
         
         accept_json(),
         add_headers(Authorization = paste0("Bearer ",token)))

contentdf <- content(df, as = "text", encoding = "UTF-8")
jsondf    <- jsonlite::fromJSON(contentdf)
next.page <- jsondf$links$`next`


#' Initialize downlaod with first page of download
write(toJSON(jsondf, auto_unbox = TRUE), file = file.path(vj_data_folder, "1.json"))


i = 2

#' loop over paginated JSON files
while(!is.null(next.page)){

  
  print(i)

  df = GET(url = next.page,
           accept_json(),
           add_headers(Authorization = paste0("Bearer ",token)))

  contentdf <- content(df, as = "text", encoding = "UTF-8")
  jsondf    <- jsonlite::fromJSON(contentdf)
  next.page <- jsondf$links$`next`
  
  #' export files as "{i}.json"
  write(toJSON(jsondf, auto_unbox = TRUE), 
        file =  file.path(vj_data_folder, paste0(i,".json")))
  
  i = i+1
  
}


