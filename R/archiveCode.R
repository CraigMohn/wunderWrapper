#' Fetch data for a specific station for a range of dates
#'
#' \code{nocap_build_archive} Fetch hourly data for a date range for a specific station.  
#'   the user is responsible for not exceeding daily quotas, the rate is capped at 10 per 
#'   minute.  The data will be written to an rda file in the specified archive.dir.
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
#' @param archive.dir string containing the locally-archived wunderground data directory.
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
#' @seealso \code{\link{update_weatherdata}}    \code{\link{rebuild_weather}}
#'
#' @export
nocap_build_archive <- function(weatherstation,
                                year,quarter="",daystart="",dayend="",
                                archive.dir="") {
  #  no checks on overwriting, or number of calls
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  if (archive.dir!="") setwd(archive.dir)
  
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
#' Update weatherdata pair with new data from wunderground
#'
#' \code{update_weatherdata} Fetch the necessary data from wunderground,
#'   archive it, process it and update the hourly detail and the daily 
#'   summary data frames.
#'
#' The data archive and update files are stored in a dataframe as returned
#' from wunderground.  Timezones are set to local for the station, so when 
#' you combine these into a single frame, the TZ info will be wiped out, 
#' and the date/time variables will be affected.
#' 
#' Before combining these dataframes from different stations, we store the Timezone,
#' and the local Time and Date as character strings reflecting the time at the site, 
#' regardless of your tz settings.  
#'
#' @param weatherpair a 2 item list containing the hourly and daily dataframes
#' @param weatherstation string containing the station ID (preface zip codes 
#'   with ZIPxxxxx)
#' @param begin.date a string "YYYYMMDD" identifying the first date to fetch in
#'   the interval.  If not specified, it chooses the last date specied for the
#'   weatherstation in th weatherpair dataframes.  If there is no such date, the
#'   begin.date is set to the same value as end.date.
#' @param end.date a string "YYYMMDD" identifying the last date to fetch in the 
#'   interval.  If not specified, the current Sys.Date() is used.
#' @param update.dir string containing the locally-archived wunderground data 
#'   updates directory, so that data fetched can be re-used.
#' @param ... validity limit variables for data cleaning
#'
#' @return a list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   The other contains daily mins, max, means and totals for the things in the hourly 
#'   reports.  Mins and maxs are based on all daily observations plus interpolated 
#'   values for midnight (local time) at the beginning and end of the day.  Means are
#'   based on data plus the midnight interpolations, weighted by the elapsed time between
#'   them.  If the data is for a time and place where Daylight Savings Time is in effect, 
#'   there will be one 23 hour and one 25 hour day per year.  Precip totals are based on
#'   the average hourly rainfall rate and a 24 hour day.
#'
#' @seealso \code{\link{rebuild_weather}}     \code{\link{clean_hourly_data}}
#'
#' @export
update_weatherdata <- function(weatherpair,weatherstation,begin.date=NA,end.date=NA,
                                    update.dir="",...) {
  nameweatherstation <- gsub("pws:","",weatherstation)
  weatherdata <- weatherpair[["hourly"]]
  weatherdaily <- weatherpair[["daily"]]
  station_data <- weatherdata[weatherdata$weatherstation==nameweatherstation,]
  station_daily <- weatherdaily[weatherdaily$weatherstation==nameweatherstation,]
  other_data <- weatherdata[weatherdata$weatherstation!=nameweatherstation,]
  other_daily <- weatherdaily[weatherdaily$weatherstation!=nameweatherstation,]
  
  if (missing(end.date)|is.na(end.date)) end.date <- as.character(format(Sys.Date(),"%Y%m%d"))

  if (nrow(station_data)==0) {
    if (missing(begin.date)|is.na(begin.date)) begin.date <- end.date
  } else {
    if (missing(begin.date)|is.na(begin.date)) 
         begin.date <- max(station_data$localdate)
  }
  if (nrow(station_daily)>=2) {
    daybefore <- as.character(as.Date(begin.date,"%Y%m%d")-1,"%Y%m%d")
    hourlybefore <- station_data[station_data[["localdate"]]==daybefore,]
  } else {
    daybefore <- NA
    hourlybefore <- NULL
  }
  
  if (end.date < begin.date) stop("end.date , begin.date")
  ndates <- ceiling(as.double(difftime(as.Date(end.date,format="%Y%m%d"),
                                       as.Date(begin.date,format="%Y%m%d"),
                                       units="days"))+1)
  if (ndates > 50) {
    cat("too many dates requested in one call to update_weatherdata\n")
    cat(begin.date,"    ",end.date,"\n")
  }  else {
  cat(ndates," call(s) to wunderground. 60 second sleep to avoid rate limit.\n")
  Sys.sleep(60)
  new_data <- zip_history_range(location=weatherstation,date_start=begin.date,
                              date_end=end.date,key=rwunderground::get_api_key())
  new_data$weatherstation <- nameweatherstation
  new_data_local <- hourly_localtimes(new_data)
  new_data_local <- clean_hourly_data(new_data_local,...)
  new_daily <- dailysummary(bind_rows(hourlybefore,new_data_local))
  if (!is.na(daybefore)) new_daily <- new_daily[new_daily$localdate != daybefore,]
    
  if (update.dir!="") {
    updfile <- paste0(update.dir,"/",nameweatherstation,"weatherupdate.rda")
    if (file.exists(updfile)) {
      load(updfile)
      file.rename(updfile,paste0(updfile,"old"))
      weatherupdate <- dplyr::bind_rows(dplyr::anti_join(weatherupdate,new_data,
                                           by=c("weatherstation","date")),new_data) %>%
              dplyr::arrange(weatherstation,date)
    } else {
      weatherupdate <- new_data
    }
    save(weatherupdate,file=updfile)
  }
  weatherdata <- dplyr::bind_rows(dplyr::anti_join(station_data,new_data_local,
                                          by=c("weatherstation","date")),
                                  new_data_local,other_data)     %>%
                 dplyr::arrange(weatherstation,date)
  weatherdaily <- dplyr::bind_rows(dplyr::anti_join(station_daily,new_daily,
                                           by=c("weatherstation","localdate")),
                                   new_daily,other_daily)        %>%
                  dplyr::arrange(weatherstation,localdate)
  }
  cat(nrow(new_data_local)," new observations on  ",nrow(new_daily)," days\n")
  return(list(daily=weatherdaily,hourly=weatherdata))
}

