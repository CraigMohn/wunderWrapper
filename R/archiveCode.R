#' Fetch data for a specific station for a range of dates
#'
#' \code{store_weather_data} Fetch hourly data for a date range for a specific station.  
#'   the user is responsible for not exceeding daily quotas, the rate is capped at 10 per 
#'   minute.  The data will be written to an rda file in the specified archiveDir.
#'   The file will have a name like STATION_YEARXXXX.rda.  There are several minute delays 
#'   in execution, so that you can't accidentally exceed the rate limit.
#'
#' @param weatherstation string containing weatherstation. If specifying an all-numeric
#'   station id, precede the digits with "ZIP", if specifying a personal weatherstion id, 
#'   precede the id with "pws:".
#' @param year numeric 4 digit year for desired data.
#' @param quarter if numeric: 0 = whole year, 1-4= quarter. If a string, use daystart 
#'   and dayend and include the string in the names of the archive filename and dataframe
#'   created.
#' @param daystart string containing 4-digit "MMDD" date for first date requested.
#' @param dayend string containing 4-digit "MMDD" date for last date requested.
#' @param archiveDir string containing the locally-archived wunderground data directory.
#'
#' @return a list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   The other contains daily mins, max, means and totals for the things in the hourly 
#'   reports.  Mins and maxs are based on all daily observations plus interpolated 
#'   values for midnight (local time) at the beginning and end of the day.  Means are
#'   based on data plus the midnight interpolations, weighted by the elapsed time between
#'   them.  Precip totals are the sum of hourly precip totals over those observations 
#'   which are taken at the most common time after the hour, for many stations that is 53
#'   minutes after the hour.
#'   
#' @seealso \code{\link{update_weather_data}}    \code{\link{merge_location_data}}
#'
#' @export
store_weather_data <- function(weatherstation,
                                year,quarter="",daystart="",dayend="",
                                archiveDir="") {
  #  no checks on overwriting, or number of calls
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  if (archiveDir!="") setwd(archiveDir)
  
  nameweatherstation <- gsub("pws:","",weatherstation)
  if (!missing(quarter) & is.numeric(quarter)) {
    if (quarter == 1) {
      dbeg <- "0101"
      dend <- "0331"
      qstr <- "Q1"
    } else if (quarter == 2) {
      dbeg <- "0401"
      dend <- "0630"
      qstr <- "Q2"
    } else if (quarter == 3) {
      dbeg <- "0701"
      dend <- "0930"
      qstr <- "Q3"
    } else if (quarter == 4) {
      dbeg <- "1001"
      dend <- "1231"
      qstr <- "Q4"
    } else if (quarter == 0) {
      dbeg <- "0101"
      dend <- "1231"
      qstr <- ""
    } else {
      stop("numeric value of quarter must be 0,1,2,3,or 4")
    }
  }  else {
    dbeg <- "0101"
    dend <- "1231"
    if (daystart!="") dbeg <- daystart
    if (dayend!="") dend <- dayend
    qstr <- quarter
  }
  date_start=paste0(year,dbeg)
  date_end<-min(paste0(year,dend),as.character(format(Sys.Date(),"%Y%m%d")))
  Sys.sleep(60)
  wdata <- zip_history_range(location=weatherstation,date_start=date_start,
                             date_end=date_end,key=get_api_key())
  if (nrow(wdata)>0) wdata$weatherstation <- nameweatherstation
  assign(paste0(nameweatherstation,"_",year,qstr),wdata)
  save(list=paste0(nameweatherstation,"_",year,qstr),file=paste0(nameweatherstation,"_",year,qstr,".rda"))                   
  return(wdata)
}
#' Update recent weather file for a location with new data
#'
#' \code{update_weather_data} Fetch the necessary data from wunderground,
#'   archive it, process it and update the hourly detail and the daily 
#'   summary data frames.
#'
#' The data archive and update files are stored in a dataframe as returned
#' from wunderground.  Timezones are set to local for the station, so when 
#' you combine these into a single frame, the TZ info will be wiped out, 
#' and the date/time variables will be affected.
#' 
#' Before combining these dataframes from different stations, we store
#' the Timezone, and the local Time and Date as character strings reflecting
#' the time at the site, regardless of your tz settings.  
#'
#' @param weatherstation string containing the station ID (preface zip codes 
#'   with ZIPxxxxx)
#' @param beginDate a string "YYYYMMDD" identifying the first date to fetch in
#'   the interval. If not specified, and an update data file is found, the last
#'   date present is used.  Otherwise, the current date will be used. 
#' @param endDate a string "YYYMMDD" identifying the last date to fetch in the 
#'   interval.  If not specified, the current Sys.Date() is used.
#' @param updateDir string containing the locally-archived wunderground data 
#'   updates directory, so that data fetched can be re-used.
#' @param newfileok if no existing update file found, create one
#' @param nofetchlimit do not retrieve more than 50 unless true
#'
#' @return a dataframe containing the wunderground data.
#'
#' @seealso \code{\link{store_weather_data}}  \code{\link{merge_location_data}}
#'
#' @export
update_weather_data <- function(weatherstation,
                                beginDate="",endDate=NA,updateDir="",
                                newfileok=FALSE,nofetchlimit=FALSE) {

  nameweatherstation <- gsub("pws:","",weatherstation)
  if (missing(endDate)|is.na(endDate)) 
    endDate <- as.character(format(Sys.Date(),"%Y%m%d"))
  
  if (updateDir=="") {
    updfile <- paste0(nameweatherstation,"weatherupdate.rda")
  } else {
    updfile <- paste0(updateDir,"/",nameweatherstation,"weatherupdate.rda")
  }
  if (file.exists(updfile)) {
    load(updfile)
    file.rename(updfile,paste0(updfile,"old"))
    if (beginDate=="") {
      temp <- hourly_localtimes(weatherupdate)$localdate
      beginDate <- temp[nrow(temp)]
    }
    temp <- hourly_localtimes(weatherupdate)
  } else if (!newfileok) {
    stop(paste0(updfile," not found and newfileok=FALSE"))
  } else {
    weatherupdate <- NULL
    if (beginDate=="") {
      beginDate <- as.character(format(Sys.Date(),"%Y%m%d"))  
    }
  }
  if (endDate < beginDate) 
    stop(paste0("endDate ",endDate," before beginDate ",beginDate))
  ndates <- ceiling(as.double(difftime(as.Date(endDate,format="%Y%m%d"),
                                       as.Date(beginDate,format="%Y%m%d"),
                                       units="days"))+1)
  if (ndates > 50 & !nofetchlimit) {
    stop(paste0("too many dates (",ndates,
                "). set nofetchlimit=TRUE if you intend this"))
  }
  cat(ndates," call(s) to wunderground. 60 sec sleep to avoid rate limit.\n")
  Sys.sleep(60)
  new_data <- zip_history_range(location=weatherstation,
                                date_start=beginDate,
                                date_end=endDate,
                                key=rwunderground::get_api_key())
  new_data$weatherstation <- nameweatherstation
  if (is.null(weatherupdate)) {
    weatherupdate <- dplyr::arrange(new_data,date)
  } else {
    weatherupdate <- merge_location_data(new_data,weatherupdate,byvar="date",
                                  rankvar="fetchtime",sortvar="date")
  }  
  save(weatherupdate,file=updfile)
  return(new_data)
} 
#' Merge two dataframes of location data, keeping most recently scraped data
#'
#' \code{merge_location_data} Given two data frames, group observations, then 
#'   select which dataset to use based on rank.
#'
#' @param df1 data frame of archived wunderground data.
#' @param df2 data frame of archived wunderground data.
#' @param byvar variable to group by in choosing data from df1 and df2.
#' @param rankvar variable to use in choosing data from df1 and df2.
#' @param sortvar variable to sort results by.
#'
#' @return a dataframe containing data from df1 and df2, where data is 
#'   included rom the dataframe which has the largest rankvar if the 
#'   same byvar group is present in both data frames, or if it is only in 
#'   one of df1 or df2.
#'   
#' @seealso \code{\link{update_weather_data}}    \code{\link{store_weather_data}}
#'
#' @export
merge_location_data <- function(df1,df2,byvar="localdate",
                              rankvar="fetchtime",sortvar="date") {
  if (df1$weatherstation[1] != df2$weatherstation[1]) 
    stop("cannot merge different locations")
  if (is.null(df1)) return(df2)
  if (is.null(df2)) return(df1)
  
  addld1 <- addld2 <- FALSE
  if (!(rankvar %in% names(df1)))  df1[[rankvar]] <- NA
  if (!(rankvar %in% names(df2)))  df2[[rankvar]] <- NA
  if (missing(byvar)) {
    if (!(byvar %in% names(df1))) {
      addld1 <- TRUE
      df1$localdate <- as.character(lubridate::date(df1$date),"%Y%m%d")
    }
    if (!(byvar %in% names(df2))) {
      addld2 <- TRUE
      df2$localdate <- as.character(lubridate::date(df2$date),"%Y%m%d")
    }
  }
  if (lubridate::is.POSIXct(df1[[rankvar]]) | 
      lubridate::is.POSIXct(df2[[rankvar]])) {
    df1 <- force_POSIX_dfvar(df1,rankvar)
    df2 <- force_POSIX_dfvar(df2,rankvar)
  } else {
    df1[is.na(df1[[rankvar]]),rankvar] <- -Inf
    df2[is.na(df2[[rankvar]]),rankvar] <- -Inf
  }
  flag1 <- df1 %>%
    dplyr::select(c(byvar,rankvar)) %>%
    dplyr::group_by_(byvar) %>%
    dplyr::summarize_all(c("max"))
  names(flag1) <- gsub(rankvar,"rank1",names(flag1))
  flag2 <- df2 %>%
    dplyr::select(c(byvar,rankvar)) %>%
    dplyr::group_by_(byvar) %>%
    dplyr::summarize_all(c("max"))
  names(flag2) <- gsub(rankvar,"rank2",names(flag2))
  flag1a <- left_join(flag1,flag2,by=byvar) 
  flag1a$from1 <- is.na(flag1a$rank2) | (flag1a$rank1 >= flag1a$rank2)
  flag1a <- dplyr::select(flag1a,dplyr::one_of(byvar,"from1"))
  flag2a <- right_join(flag1,flag2,by=byvar) 
  flag2a$from2 <- is.na(flag2a$rank1) | (flag2a$rank2 > flag2a$rank1)
  flag2a <- dplyr::select(flag2a,dplyr::one_of(byvar,"from2"))
  df1 <- df1 %>% dplyr::left_join(flag1a,by=byvar)
  df1 <- df1[df1$from1,] 
  df1$from1 <- NULL
  df2 <- df2 %>% dplyr::left_join(flag2a,by=byvar)
  df2 <- df2[df2$from2,] 
  df2$from2 <- NULL
  if (addld1) df1 <- dplyr::select(df1,-dplyr::one_of(byvar))
  if (addld2) df2 <- dplyr::select(df2,-dplyr::one_of(byvar))
  return(dplyr::bind_rows(df1,df2) %>%
           dplyr::arrange_(sortvar)  )
}

