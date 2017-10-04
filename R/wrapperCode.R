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
#' @param ... validity limit variables for data cleaning
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
#' @seealso \code{\link{update_weatherdata}}     \code{\link{clean_hourly_data}}
#'
#' @export
rebuild_weather <- function(archive.dir,update.dir="",...) {
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
    if (is.null(weather_hourly)) {
      weather_hourly <- temp
    } else {
      weather_hourly <- dplyr::bind_rows(dplyr::anti_join(weather_hourly,temp,
                                        by=c("weatherstation","localdate","localtime")),
                                          temp)
    }
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
                                             by=c("weatherstation","localdate","localtime")),
                                         temp)
    }  
    setwd(old_wd)
  }
  weather_hourly <- clean_hourly_data(weather_hourly,...) %>%  
                    dplyr::arrange(weatherstation,date)  
weather_h <<- weather_hourly
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
#' @param ... validity limit variables for data cleaning
#'
#' @return A la list containing two dataframes.  One contains approximately hourly measures
#'   of temperature, precipitation, wind, etc, along with time and weatherstation id. 
#'   The other contains daily mins, max, means and totals for the things in the hourly 
#'   reports.  Mins and maxs are based on all daily observations plus interpolated 
#'   values for midnight (local time) at the beginning and end of the day.  Means are
#'   based on data plus the midnight interpolations, weighted by the elapsed time between
#'   them.  Precip totals are the sum of hourly precip totals over those observations 
#'   which are taken at the most common time after the hour, for many stations that is 53
#'   minutes after the hour.
#'
#' @seealso \code{\link{rebuild_weather}}     \code{\link{clean_hourly_data}}
#'
#' @export
update_weatherdata <- function(weatherpair,weatherstation,begin.date=NA,end.date=NA,
                                    update.dir="",...) {
###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  localtime <- localdate <- NULL
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
         begin.date <- gsub("-","",max(station_data$localdate))
  }
  if (end.date < begin.date) stop("end.date , begin.date")
  ndates <- ceiling(as.double(difftime(as.Date(end.date,format="%Y%m%d"),
                                       as.Date(begin.date,format="%Y%m%d"),
                                       units="days"))+1)
  if (ndates > 50) {
    cat("\ntoo many dates requested in one call to update_weatherdata")
    cat("\n", begin.date,"    ",end.date)
  }  else {
    cat("\n",ndates," calls to wunderground. 60 second sleep to avoid rate limit")
    Sys.sleep(60)
    new_data <- zip_history_range(location=weatherstation,date_start=begin.date,
                              date_end=end.date,key=rwunderground::get_api_key())
    new_data$weatherstation <- nameweatherstation
    new_data_local <- clean_hourly_data(new_data,...)
    new_data_local <- hourly_localtimes(new_data_local)
    new_daily <- dailysummary(new_data_local)
    
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
                                          by=c("weatherstation","localdate","localtime")),
                                    new_data_local,other_data)              %>%
                   dplyr::arrange(weatherstation,localtime)
    weatherdaily <- dplyr::bind_rows(dplyr::anti_join(station_daily,new_daily,
                                           by=c("weatherstation","localdate")),
                                     new_daily,other_daily)                        %>%
                    dplyr::arrange(weatherstation,localdate)
  }
  return(list(daily=weatherdaily,hourly=weatherdata))
}
#' Remove data measurements that are outside of credible ranges
#'
#' \code{clean_hourly_data} Filter out extreme and implausible values from retrieved
#'   weather data.  Most limits are user parametrizable, although precip and wind speeds
#'   are rejected if nonpositive, and visibility and humidity are restricted to be 
#'   between 0 and 100.
#'
#' @param df dataframe with the weather data.
#' @param temp.min numeric temperatures below this will be set to NA.
#' @param temp.max numeric temperatures above this will be set to NA.
#' @param wind_spd.max numeric wind speeds above this will be set to NA.
#' @param wind_gust.max numeric wind speeds above this will be set to NA.
#' @param precip.max numeric precip above this will be set to NA.
#' @param pressure.min numeric pressure below this will be set to NA.
#' @param pressure.max numeric pressure above this will be set to NA.
#' @param wind_chill.min numeric wind chill below this will be set to NA.
#' @param wind_chill.max numeric wind chill above this will be set to NA.
#' @param heat_index.min numeric heat index below this will be set to NA.
#' @param heat_index.max numeric heat index above this will be set to NA.
#'
#' @return dataframe with extreme values set to NA
#'   
#' @seealso \code{\link{update_weatherdata}} \code{\link{rebuild_weather}}
#'
#' @export
clean_hourly_data <- function(df,temp.min=-50,temp.max=130,wind_spd.max=80,wind_gust.max=120,
                              pressure.min=20,pressure.max=50,wind_chill.min=-50,wind_chill.max=70,
                              heat_index.min=50,heat_index.max=150,precip.max=2) {
  ###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  temp <- dew_pt <- hum <- wind_spd <- wind_gust <- vis <- precip <- NULL
  pressure <- wind_chill <- heat_index <- NULL
  return(df %>%
           dplyr::mutate(temp=replace(temp, which(temp < temp.min), NA),
                         temp=replace(temp, which(temp > temp.max), NA),
                         dew_pt=replace(dew_pt, which(dew_pt < 30), NA),
                         hum=replace(hum, which(hum < 0), NA),
                         hum=replace(hum, which(hum > 100), NA),
                         wind_spd=replace(wind_spd, which(wind_spd < 0), NA),
                         wind_spd=replace(wind_spd, which(wind_spd > wind_spd.max), NA),
                         wind_gust=replace(wind_gust, which(wind_gust < 0), NA),
                         wind_gust=replace(wind_gust, which(wind_gust > wind_gust.max), NA),
                         vis=replace(vis, which(vis < 0), NA),
                         vis=replace(vis, which(vis > 100), NA),
                         pressure=replace(pressure, which(pressure < pressure.min), NA),
                         pressure=replace(pressure, which(pressure > pressure.max), NA),
                         wind_chill=replace(wind_chill, which(wind_chill < wind_chill.min), NA),
                         wind_chill=replace(wind_chill, which(wind_chill > wind_chill.max), NA),
                         heat_index=replace(heat_index, which(heat_index < heat_index.min), NA),
                         heat_index=replace(heat_index, which(heat_index > heat_index.max), NA),
                         precip=replace(precip, which(precip < 0), NA),
                         precip=replace(precip, which(precip > precip.max), NA)
           )
  )
}
######## internal use 
####################################################################################
hourly_localtimes <- function(hourlydata) {
  if (!is.null(hourlydata)) {
    hourlydata$localtz <- lubridate::tz(hourlydata$date)
    hourlydata$localdate <- as.character(lubridate::date(hourlydata$date),"%Y%m%d")
    hourlydata$localtime <- as.character(hourlydata$date,"%H%M%S")
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
  weatherstation <- localdate <- temp <- precip <- gustratio <- rain <- NULL
  wind_spd <- snow <- hail <- localtime <- minutes <-NULL
  hourlyaugmented <- add_midnights(hourlydata)
  weatherdaily <- hourlydata %>% 
    dplyr::mutate(minutes=as.numeric(substr(localtime,3,4))) %>%    
    dplyr::group_by(weatherstation,localdate) %>%
    dplyr::summarize(min_temp=min(temp,na.rm=TRUE),
                     max_temp=max(temp,na.rm=TRUE),
                     mean_temp=weighted_mean(localtime,temp),
                     max_windspd=max(wind_spd,na.rm=TRUE),
                     mean_windspd=weighted_mean(localtime,wind_spd),
                     num_hourly_readings=count_on_mode_minutes(localtime),
                     tot_precip=total_on_mode_minutes(localtime,precip),
                     any_gust=any(gustratio>1.6,na.rm=TRUE),
                     any_rain=any(rain>0,na.rm=TRUE),
                     any_snow=any(snow>0,na.rm=TRUE),
                     any_hail=any(hail>0,na.rm=TRUE))

  return(weatherdaily)
}
zip_history_range <- function(location,date_start,date_end,key) {
  if (substr(location,1,3)=="ZIP") location <- substr(location,4,nchar(location))
  return(rwunderground::history_range(location=location,date_start=date_start,date_end=date_end,key=key))
}
total_on_mode_minutes <- function(localtime,quantity) {
  minutes <- as.numeric(substr(localtime,3,4))
  return(sum(quantity[minutes==mode_minutes(localtime)],na.rm=TRUE))
}
count_on_mode_minutes <- function(localtime) {
  minutes <- as.numeric(substr(localtime,3,4))
  return(sum(minutes==mode_minutes(localtime)))
}
mode_minutes <- function(localtime) {
  minutes <- as.numeric(substr(localtime,3,4))
  return(pracma::Mode(minutes))
}
add_midnights <- function(hourlydata) {
  ###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  weatherstation <- localdate <- localtime <- NULL
  
  newmidnight <- hourlydata %>% dplyr::distinct(weatherstation,localdate) %>%
                    dplyr::mutate(localtime="000000",
                                  temp=NA,
                                  wind_spd=NA) 
  stationset <- unique(newmidnight$weatherstation)
  for (x in length(stationset)) {
    newmidnightstation <- newmidnight[newmidnight$weatherstation==stationset[x],]
    newvars <- interpolate_for_station(newmidnightstation,hourlydata)
    newmidnight[newmidnight$weatherstation==stationset[x],"temp"] <- newvars[["temp"]]
    newmidnight[newmidnight$weatherstation==stationset[x],"wind_spd"] <- newvars[["wind_spd"]]
  }
    
  if (nrow(newmidnight)>1) {
    endmidnight <- newmidnight[-1,]
    endmidnight$localtime <- "240000"
    endmidnight$localdate <- as.character(as.Date(endmidnight$localdate,"%Y%m%d")-1,"%Y%m%d")
  } else {
    endmidnight <- NULL
  }
  augmented_data <- dplyr::bind_rows(hourlydata,newmidnight,endmidnight) %>%
                    dplyr::arrange(weatherstation,localdate,localtime)
  return(augmented_data)
}
interpolate_for_station <- function(newtimedf,alldatadf) {
  hourlydata <- alldatadf[alldatadf$weatherstation==newtimedf$weatherstation[1],]
  timedf <- as.POSIXct(paste0(hourlydata$localdate," ",hourlydata$localtime),
                              substr(hourlydata$localtime,1,2),":",
                              substr(hourlydata$localtime,3,4),":",
                              substr(hourlydata$localtime,5,6)),
                       format = "%Y%m%d %H%M%S")
  timenew <- as.POSIXct(paste0(newtimedf$localdate," ",newtimedf$localtime),
                               substr(newtimedf$localtime,1,2),":",
                               substr(newtimedf$localtime,3,4),":",
                               substr(newtimedf$localtime,5,6)),
                        format = "%Y%m%d %H%M%S")
  new_temp <- stats::approx(timedf, y = hourlydata$temp, 
                                 xout = timenew,rule=2)[[2]]
  new_wind <- stats::approx(timedf, y = hourlydata$wind_spd, 
                                 xout = timenew,rule=2)[[2]]
  return(list(temp=new_temp,wind_spd=new_wind))
}
weighted_mean <- function(localtime,quantity) {
  localtime <- localtime[!is.na(quantity)]
  quantity <- quantity[!is.na(quantity)]
  lagtime <- c(localtime[1],localtime[-length(localtime)])
  leadtime <- c(localtime[-1],localtime[length(localtime)])
  return(sum(elapsed_time(leadtime,lagtime)*quantity)/
           sum(elapsed_time(leadtime,lagtime)))
}
elapsed_time <- function(leadtime,lagtime) {
  return(as.numeric(substr(leadtime,1,2))+
         as.numeric(substr(leadtime,3,4))/60+
         as.numeric(substr(leadtime,5,6))/3600-
         as.numeric(substr(lagtime,1,2))-
         as.numeric(substr(lagtime,3,4))/60-
         as.numeric(substr(lagtime,5,6))/3600
  )
}
clean_hourly_data <- function(df) {
  ###  make the R checker happy with utterly irrelevant initializations of variables used by dplyr
  temp <- dew_pt <- hum <- wind_spd <- wind_gust <- vis <- precip <- NULL
  pressure <- wind_chill <- heat_index <- NULL
  return(df %>%
         dplyr::mutate(temp=replace(temp, which(temp < -50), NA),
                       dew_pt=replace(dew_pt, which(dew_pt < 30), NA),
                       hum=replace(hum, which(hum < 0), NA),
                       hum=replace(hum, which(hum > 100), NA),
                       wind_spd=replace(wind_spd, which(wind_spd < 0), NA),
                       wind_spd=replace(wind_spd, which(wind_spd > 80), NA),
                       wind_gust=replace(wind_gust, which(wind_gust < 0), NA),
                       wind_gust=replace(wind_gust, which(wind_gust > 100), NA),
                       vis=replace(vis, which(vis < 0), NA),
                       vis=replace(vis, which(vis > 50), NA),
                       pressure=replace(pressure, which(pressure < 20), NA),
                       pressure=replace(pressure, which(pressure > 50), NA),
                       wind_chill=replace(wind_chill, which(wind_chill < -50), NA),
                       wind_chill=replace(wind_chill, which(wind_chill > 70), NA),
                       heat_index=replace(heat_index, which(heat_index < 0), NA),
                       heat_index=replace(heat_index, which(heat_index > 120), NA),
                       precip=replace(precip, which(precip < 0), NA),
                       precip=replace(precip, which(precip > 2), NA)
         )
  )
}

