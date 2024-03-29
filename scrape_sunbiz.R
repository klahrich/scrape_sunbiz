library(rvest)
library(purrr)
library(stringr)
library(dplyr)
library(tibble)
library(glue)
library(readr)
library(tidyr)

root <- "http://search.sunbiz.org"

session <- html_session(root)


#' This is the main function. It will start at the provided url, and keep going to the next page until none
#' is found.
#' 
#' @param url The start URL
#' @param verbose Should we print where we are as the scraping happens ?
#' @param max_pages The maximum number of pages to scrape
#' @param start_from_scratch set to FALSE to start from the last url scraped.
#' If no url was scraped before, will fall back to provided url parameter.
scrape_listing_recurrent <- function(url = NULL, 
                                     verbose = TRUE, 
                                     max_pages = Inf, 
                                     output_folder = '.',
                                     start_from_scratch = FALSE){
  
  if(start_from_scratch){
    
    if(is.null(url)){
      stop("In order to start from scratch, please provide the url parameter.")
    }
    
    start_counter <- 1
    
  }else{
    
    if(file.exists(glue("{output_folder}\\last_url_scraped.txt"))){
      
      url <- readLines(glue("{output_folder}\\last_url_scraped.txt"))
      
    }else{
      
      if(is.null(url)){
        stop("start_from_scratch is FALSE, 
              but file last_url_scraped.txt could not be found in the output folder,
              and no url was provided.")
      }
    }
    
    files <- list.files(output_folder, pattern = ".rds", full.names = TRUE)
    
    if(length(files) == 0){
      start_counter <- 1
    }else{
      start_counter <- length(files)
    }
    
  }
  
  keep_going <- TRUE
  
  i <- start_counter
  
  while(keep_going){
    
    if(verbose){
      print(glue("Scraping page {i}"))
      print(glue("URL: {url}"))
    }
    
    html <- session %>% jump_to(url)
    
    res <- scrape_listing(html, verbose)
    res <- add_column(res, page = i, .before = 1)
    
    saveRDS(res, glue("{output_folder}/res_{str_pad(i, 8, pad='0')}.rds"))
    
    writeLines(url, glue("{output_folder}\\last_url_scraped.txt"))
    
    keep_going <- 
      tryCatch({
        # this will fail if there is no "Next List" link, which is what we want
        
        navigation_links <- 
          html %>% 
          html_nodes(".navigationBarPaging") %>% 
          html_nodes("a")
        
        next_page_link <- 
          navigation_links %>% 
          purrr::keep(~ identical(html_attr(., "title"), "Next List"))
          
        next_page_link <- next_page_link[[1]]
        
        url <-
          next_page_link %>% 
          html_attr("href")
        
        i <- i + 1
        
        (i  - start_counter < max_pages)
      },
      error = function(e){
        FALSE
      })
    
  }
  
}


#' This function scrapes a page of search results 
#' (e.g. http://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?inquiryType=EntityName&searchNameOrder=A&searchTerm=A)
#' It calls the `scrape_company` function to do so.
#' This function is called by the `scrape_listing_recursive` function
#' 
#' @return A dataframe, containing all the scraped information
scrape_listing <- function(html, verbose=TRUE){
  
  company_nodes <- 
    html %>%
    html_nodes("table:nth-child(2) tbody:nth-child(2) tr td.large-width > a")
  
  company_urls <- 
    company_nodes %>% 
    html_attr("href")
  
  company_names_short <- 
    company_nodes %>% 
    html_text()
  
  res <- 
    pmap(list(company_urls, company_names_short, 1:length(company_urls)),
         safely(scrape_company)) %>% 
    transpose()
  
  res$url <- company_urls
  
  
  idx_errors <- res$error %>% map_lgl(negate(is.null)) %>% which()
  url_errors <- res$url[idx_errors]
  
  if(length(url_errors) > 0){
    print("Error urls:")
    url_errors %>% walk(print)
  }
  
  bind_rows(res$result)
}



#' This is the function tht scrapes a company page
#' 
#' @param name The short name of the company, as displayed in the search result page
#' @param id An id that will allow to differentiate between companies
#' 
#' @return A dataframe
scrape_company <- function(url, name, id){
  
  html <- session %>% jump_to(url)
  
  # get the detail sections
  detail_sections <- 
    html %>% 
    html_nodes("div.detailSection")
  
  l <- list()
  
  for(s in detail_sections){
    
    if(str_detect(html_attr(s, "class"), "corporationName")){
      # this section contains the company name
      
      category <- "Company name"
      label <- ""
      value <- s %>% html_text() %>% str_trim()
      
    } else if(str_detect(html_attr(s, "class"), "filingInformation")){
      # this section contains filing information
      
      category <- "Filing information"
      
      # information starts with the 2nd span
      s2 <- s %>% html_node("span:nth-child(2)")
      label <- s2 %>% html_nodes("label") %>% html_text() %>% str_trim()
      value <- s2 %>% html_nodes("span") %>% html_text() %>% str_trim()
      
      # in case there's some additional text, trim it
      value <- value[1:length(label)]
      
    } else{
      # rest of the information
      
      category <- s %>% html_node("span") %>% html_text() %>% str_trim()
      label <- ""
      
      # extract text from the spans
      if(str_detect(category, "Officer/Director")){
        # this section is a bit tricky. We will extract all the text & process it
        value <- 
          s %>% 
          html_text() %>% 
          str_split("(\\r\\n)+") %>% 
          .[[1]] %>% 
          map_chr(str_trim) %>% 
          keep(~ str_length(.) > 1)
        
        # remove 1st two elements ("Officer/Director Detail" "Name & Address" )
        value <- value[-(1:2)] %>% paste(collapse = "\n") %>% str_trim()
        
      } else{
        value <- s %>% html_nodes("span") %>% .[-1] %>% html_text() %>% str_trim()
      }

    }
    
    l[[length(l) + 1]] <- tibble(category=category, label=label, value=value)
    
    if(str_detect(category, "Officer/Director")){
      # we stop after this section
      break
    }
    
  }
  
  res <- bind_rows(l)
  res <- add_column(res, name = name, .before=1)
  res <- add_column(res, id = id, .before=1)
  
  # (KL) UPDATE 2019-10-22: Change from long to wide format, at request of client
  res$category <- ifelse(res$category %in% c("Company name", 
                                           "Filing information", 
                                           "Mailing Address", 
                                           "Principal Address"),
                        res$category,
                        "Person(s) Detail")
  
  res$key <- ifelse(map_lgl(res$label, ~ str_length(.) > 0),
                   paste(res$category, res$label, sep = " - "), 
                   res$category)
  
  res_summ <- 
    res %>% 
    group_by(
      id,
      name,
      key
    ) %>% 
    summarise(
      value2 = paste(value, collapse = " - ")
    )
  
  res_wide <- 
    spread(res_summ, key, value2)
  
  
  res_wide
}

# CHANGE THIS
output_folder <- "C:\\Users\\HP\\Documents\\scrape_sunbiz"

# If you are starting from scratch, use this start_url and start_counter
start_url <- "/Inquiry/CorporationSearch/SearchResults?inquiryType=EntityName&searchNameOrder=A&searchTerm=0"

# Main function call
# Note: This will created a series of .rds files in the output folder
scrape_listing_recurrent(start_url, 
                         output_folder = output_folder,
                         max_pages = 4,
                         start_from_scratch = FALSE)

# This will read all the .RDS files and append them in one dataframe
files <- list.files(output_folder, pattern = ".rds", full.names = TRUE)

data <- 
  files %>% 
  map(readRDS) %>% 
  bind_rows()

# This will save all the data in one CSV
data %>% write_csv(glue("{output_folder}\\final_data.csv"))


