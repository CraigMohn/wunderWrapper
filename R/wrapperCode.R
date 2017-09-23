#' Fetch data for a specific station for a range of dates
#'
#' \code{nocap_build_archive} Fetch hourly data for a date range for a specific station.  
#'   the user is responsible for not exceeding daily quotas, the rate is capped at 10 per 
#'   minute.  The data will be written to an rda file in the specified archive.dir.
#'   The file will have a name like STATION_YEARXXXX.rda.  There are several minute delays 
#'   in execution, so that you can't accidentally exceed the rate limit.
#'
#' @param weatherstation string containing weatherstation. if specifying an all-numeric
#'   station id, precede the digits with "ZIP".
#' @param year numeric 4 digit year for desired data.
#' @param quarter if numeric: 0 = whole year, 1-4= quarter. if a string, use daystart 
#'   and dayend and name file using the string. 
#' @param daystart string containing 4-digit "MMDD" date for first date requested.
#' @param dayend string containing 4-digit "MMDD" date for last date requested.
#' @param archive.dir string containing the locally-archived wunderground data directory.
#'
#' @return a list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   The other contains daily mins, max, and totals for the things in the hourly reports.
#'
#' @seealso \code{\link{update_weatherdata}}
#'
#' @export
nocap_build_archive <- function(weatherstation,
                                year,quarter=0,daystart="",dayend="",
                                archive.dir="") {
  #  no checks on overwriting, or number of calls
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  if (archive.dir!="") setwd(archive.dir)
  
  if (is.numeric(quarter)) {
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
  wdata$weatherstation <- weatherstation
  assign(paste0(weatherstation,"_",year,qstr),wdata)
  save(list=paste0(weatherstation,"_",year,qstr),file=paste0(weatherstation,"_",year,qstr,".rda"))                   
  return(wdata)
}

#' Pull together the weather archive files and stored updates
#'
#' \code{rebuild_weather} Read in all of the archived and stored updates for  
#'  weather data.  It returns a list of 2 dataframes (hourly and daily).
#'
#' The data archive and update files are stored in a dataframe as returned
#' from wunderground.  Timezones are set to local, so when you combine these
#' into a single frame, the TZ info will be wiped out, and the date/time
#' variables will be affected.
#' 
#' Before combining these dataframes from different stations, we store the Timezone,
#' and the local Time and Date as character strings reflecting the time at the site, regardless
#' your tz settings.  
#'
#' @param archive.dir string containing the locally-archived wunderground data directory.
#' @param update.dir string containing the locally-archived wunderground data updates directory.
#'
#' @return A list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   Th other contains daily mins, max, and totals for the things in the hourly reports.
#'
#' @seealso \code{\link{update_weatherdata}}
#'
#' @export
rebuild_weather <- function(archive.dir,update.dir="") {
###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  weatherstation <- weatherupdate <- NULL
  
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  setwd(archive.dir)
  fl <- list.files(pattern=".rda")
  weather_hourly <- NULL
  for (x in fl) {
    cat("\n loading data file ",x)
    load(x)
    assign("temp",eval(parse(text=gsub(".rda","",x))))
    temp <- hourly_localtimes(temp)
    weather_hourly <- dplyr::bind_rows(weather_hourly,temp)
  }  
  setwd(old_wd)
  if (update.dir!="") {
    setwd(update.dir)
    fl <- list.files(pattern=".rda")
    flold <- list.files(pattern=".rdaold")
    fl <- setdiff(fl,flold)
    for (x in fl) {
      cat("\n loading data file ",x)
      load(x)
      temp <- hourly_localtimes(weatherupdate)
      weather_hourly <- dplyr::bind_rows(dplyr::anti_join(weather_hourly,temp,
                                                   by=c("weatherstation","localtime")),
                                         temp)
    }  
    setwd(old_wd)
  }
  weather_hourly <- weather_hourly %>%  dplyr::arrange(weatherstation,date)  
  weather_daily <-  dailysummary(weather_hourly)
  return(list(daily=weather_daily,hourly=weather_hourly))
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
#'
#' @return A list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   The other contains daily mins, max, and totals for the things in the hourly reports.
#'
#' @seealso \code{\link{rebuild_weather}}
#'
#' @export
update_weatherdata <- function(weatherpair,weatherstation,begin.date=NA,end.date=NA,
                                    update.dir="") {
###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  localtime <- localdate <- NULL
  weatherdata <- weatherpair[["hourly"]]
  weatherdaily <- weatherpair[["daily"]]
  station_data <- weatherdata[weatherdata$weatherstation==weatherstation,]
  station_daily <- weatherdaily[weatherdaily$weatherstation==weatherstation,]
  other_data <- weatherdata[weatherdata$weatherstation!=weatherstation,]
  other_daily <- weatherdaily[weatherdaily$weatherstation!=weatherstation,]
  
  if (missing(end.date)|is.na(end.date)) end.date <- as.character(format(Sys.Date(),"%Y%m%d"))

  if (nrow(station_data)==0) {
    if (missing(begin.date)|is.na(begin.date)) begin.date <- end.date
  } else {
    if (missing(begin.date)|is.na(begin.date)) 
         begin.date <- gsub("-","",max(station_data$localdate))
  }
  if (end.date < begin.date) stop("end.date , begin.date")
  ndates <- ceiling(as.double(difftime(as.Date(end.date,format="%Y%m%d"),
                                       as.Date(begin.date,format="%Y%m%d"),
                                       units="days"))+1)
  if (ndates > 50) {
    cat("\ntoo many dates requested in one call to update_home_weatherdata")
    cat("\n", begin.date,"    ",end.date)
  }  else {
    cat("\n",ndates," calls to wunderground. 60 second sleep to avoid rate limit")
    Sys.sleep(60)
    new_data <- zip_history_range(location=weatherstation,date_start=begin.date,
                              date_end=end.date,key=rwunderground::get_api_key())
    new_data$weatherstation <- weatherstation
    new_data_local <- hourly_localtimes(new_data)
    new_daily <- dailysummary(new_data_local)
    
    if (update.dir!="") {
      updfile <- paste0(update.dir,"/",weatherstation,"weatherupdate.rda")
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
                                                     by=c("weatherstation","localtime")),
                                    new_data_local,other_data)              %>%
                   dplyr::arrange(weatherstation,localtime)
    weatherdaily <- dplyr::bind_rows(dplyr::anti_join(station_daily,new_daily,
                                                       by=c("weatherstation","localdate")),
                                     new_daily,other_daily)                        %>%
                    dplyr::arrange(weatherstation,localdate)
  }
  return(list(daily=weatherdaily,hourly=weatherdata))
}
######## internal use 
####################################################################################
hourly_localtimes <- function(hourlydata) {
  if (!is.null(hourlydata)) {
    hourlydata$localtz <- lubridate::tz(hourlydata$date)
    hourlydata$localdate <- as.character(lubridate::date(hourlydata$date))
    hourlydata$localtime <- as.character(hourlydata$date)
    hourlydata$localdecimaltime <- lubridate::hour(hourlydata$date) + 
                                   lubridate::minute(hourlydata$date)/60 +
                                   lubridate::second(hourlydata$date)/3600
    either.na <- is.na(hourlydata$wind_gust) | is.na(hourlydata$wind_spd)
    hourlydata$gustratio <- hourlydata$wind_gust/hourlydata$wind_spd
  }
  return(hourlydata)
}
dailysummary <- function(hourlydata) {
###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  weatherstation <- localdate <- temp <- precip <- gustratio <- rain <- snow <- hail <- NULL
  weather_daily <-  hourlydata %>%
    dplyr::group_by(weatherstation,localdate) %>%
    dplyr::summarize(min_temp=min(temp,na.rm=TRUE),
                     max_temp=max(temp,na.rm=TRUE),
                    mean_temp=mean(temp,na.rm=TRUE),
                    tot_precip=sum(precip,na.rm=TRUE),
                    any_gust=any(gustratio>1.6,na.rm=TRUE),
                    any_rain=any(rain>0,na.rm=TRUE),
                    any_snow=any(snow>0,na.rm=TRUE),
                    any_hail=any(hail>0,na.rm=TRUE))
  return(weather_daily)
}
zip_history_range <- function(location,date_start,date_end,key) {
  if (substr(location,1,3)=="ZIP") location <- substr(location,4,nchar(location))
  return(rwunderground::history_range(location=location,date_start=date_start,date_end=date_end,key=key))
}
