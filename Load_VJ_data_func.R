
#' LOAD VICINITY RDA FILES

# Load Data ---------------------------------------------------------------

vj_load <- function(yr, mon, content = c("All", 'Skills', 'Certs'), f = in_folder){
  
  #' Check \content input argument are valid
  load_content <- content[content %in% c("All", 'Skills', 'Certs')]
  if(length(load_content) < length(content)){
    if(length(load_content)==0){
      stop(paste0('Content input argument(s) invalid!\nContent can only be',
                  ' "All", "Skills" and/or "Certs" (case sensitive).'))
      
    }else{
      
      warning(paste0("Content argument(s) ", 
                     paste(dput(content[!content %in% load_content]), collapse=', '), " invalid.\n",
                     "Proceeding with valid content arugment ", dput(load_content), " only.", collapse = ','))
    }
  }
  
  #' replace "ALL" with the "T" name convention used.
  load_content <- gsub('All', 'T', load_content)
  
  message(paste("Loading", load_content,collapse=', '))
  
  if(nchar(mon)==1){mon <- paste0('0', mon)}
  
  #' load identified RDS files
  out_ls <-
    lapply(load_content, function(x) {
      readRDS(file.path(f, paste0("vicinity-",yr,"-",mon,"-ALL_", x, ".rds")))
    })
  
  # setwd(keep_dir)
  names(out_ls) <- load_content
  
  return(out_ls)
}
