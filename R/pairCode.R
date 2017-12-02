#' Return a dataframe of weather variables characterizing an interval
#'
#' \code{weather_interval} Return a dataframe of weather variables 
#'   characterizing an interval.  
#'
#' If the interval begins or end in the hour where daylight saving
#' causes the hour to repeat (fall back), it will be assumed that it 
#' begins or ends on the first pass.
#'
#' @param weatherdata data frame containing weather data
#' @param weatherlocation char vector containing the station IDs (preface
#'   zip codes with ZIPxxxxx)
#' @param localdateBegin char vector containing the local date of the beginning
#'   of the interval "YYYYMMDD"
#' @param localtimeBegin char vector containing the local time of the beginning
#'   of the interval "HHMMSS"
#' @param durationHours numeric length of the interval in hours
#'
#' @return a dataframe containg the following variables:
#' 
#' @seealso \code{\link{update_weather}}     \code{\link{clean_hourly_data}}
#'
#' @export
weather_interval <- function(weatherdata,weatherlocation,
                             localdateBegin,localtimeBegin,
                             durationHours) {
  nr <- nrow(localdateBegin)
  weatherdf <- data.frame(localdate=localdateBegin,
                          localtime=localtimeBegin,
                          tempMin=NA,tempMean=NA,tempMax=NA,
                          precipMin=NA,precipMean=NA,precipMax=NA,
                          windMean=NA,gusty=NA,stringsAsFactors=FALSE)
  for (wloc in unique(weatherlocation)) {
    wlocobs <- weatherlocation == wloc  
    wlocdata <- weatherdata$weatherstation == wloc
    wloctz <- weatherdata$localtz[wlocdata][1]
    timebeg <- as.POSIXct(paste0(localdateBegin[wlocobs]," ",localtimeBegin[wlocobs]),
                          format("%Y%m%d %H%M%S"),tz=wloctz)
    timeend <- timebeg + 3600*durationHours[wlocobs]
    temps <- as.data.frame(t(
                     mapply(
                      mmfuns,timebeg,timeend,
                      MoreArgs=list(datatime=weatherdata$date[wlocdata],
                                    datameasure=weatherdata$temp[wlocdata]))))
    winds <- as.data.frame(t(
      mapply(
        mmfuns,timebeg,timeend,
        MoreArgs=list(datatime=weatherdata$date[wlocdata],
                      datameasure=weatherdata$wind_spd[wlocdata]))))
    precips <- as.data.frame(t(
      mapply(
        mmfuns,timebeg,timeend,
        MoreArgs=list(datatime=weatherdata$date[wlocdata],
                      datameasure=weatherdata$precip[wlocdata]))))
    gustratios <- as.data.frame(t(
      mapply(
        mmfuns,timebeg,timeend,
        MoreArgs=list(datatime=weatherdata$date[wlocdata],
                      datameasure=weatherdata$gustratio[wlocdata]))))
    weatherdf[wlocobs,"tempMin"] <- temps[,"min"]
    weatherdf[wlocobs,"tempMean"] <- temps[,"mean"]
    weatherdf[wlocobs,"tempMAX"] <- temps[,"max"]
    weatherdf[wlocobs,"precipMin"] <- precips[,"min"]
    weatherdf[wlocobs,"precipMean"] <- precips[,"mean"]
    weatherdf[wlocobs,"precipMAX"] <- precips[,"max"]
    weatherdf[wlocobs,"windMean"] <- winds[,"mean"]
    weatherdf[wlocobs,"gusty"] <- gustratios[,"max"] > 1.6 & 
                                      winds[,"mean"] > 5
  }
  return(weatherdf)
}
mmfuns <- function(timebeg,timeend,datatime,datameasure,
                    toofar=60,grid=10) {
  usedata <- datatime < timeend+lubridate::minutes(toofar) & 
             datatime > timebeg-lubridate::minutes(toofar)
  if (sum(!is.na(datameasure[usedata])) > 2) {
    valseq <- approx(datatime[usedata],datameasure[usedata],
                     seq(timebeg,timeend,by=paste0(grid," min")),
                     rule=2)[[2]]
    return(c("mean"=mean(valseq,na.rm=TRUE),
              "min"=min(valseq,na.rm=TRUE),
              "max"=max(valseq,na.rm=TRUE)))
  } else {
    return(c("mean"=mean(datameasure[usedata],na.rm=TRUE),
             "min"=mean(datameasure[usedata],na.rm=TRUE),
             "max"=mean(datameasure[usedata],na.rm=TRUE)))
  }
}

#' Pull together the weather archive files and stored updates
#'
#' \code{rebuild_weather} Read in all of the archived and stored updates for  
#'  weather data.  It returns a list of 2 dataframes (hourly and daily).
#'
#' Previously fetched data files are assembled and stored in a list of two
#' tibbles, one containing the data from all files, and one conatining daily
#' summary variables.
#' 
#' The daily summaries are slightly dependent on the last observation from
#' the preceding day and the first observation from the following day, as 
#' midnight values are interpolated and included in the daily measures. 
#' 
#' Before combining these dataframes from different stations, we store the
#' Timezone, and the local Time and Date as character strings reflecting the 
#' time at the site, regardless of your tz settings.  
#'
#' @param archiveDir string containing the locally-archived wunderground 
#'   data directory. Data will be loaded from this directory and its 
#'   subdirectory tree.
#' @param updateDir string containing the locally-archived wunderground 
#'   data updates directory.
#' @param oldDir string containing old locally-archived wunderground 
#'   data updates directory whose files are checked against current version.
#'   Old data is not merged in, this is for diagnostic purposes.  
#' @param ... validity limit variables for data cleaning by 
#'   \code{\link{clean_hourly_data}}
#' @param loud produce more output
#'
#' @return a list containing two dataframes.  One contains approximately 
#'   hourly measures of temperature, precipitation, wind, etc, along with time
#'   and weatherstation id. The other contains daily mins, max, means and 
#'   totals for the things in the hourly reports.  Mins and maxs are based 
#'   on all daily observations plus interpolated values for midnight (local 
#'   time) at the beginning and end of the day.  Means are based on data plus 
#'   the midnight interpolations, weighted by the elapsed time between
#'   them.  If the data is for a time and place where Daylight Savings Time 
#'   is in effect, there will be one 23 hour and one 25 hour day per year. 
#'   Precip totals are based on the average hourly rainfall rate and a 24 
#'   hour day.
#' 
#' @seealso \code{\link{update_weather}}     \code{\link{clean_hourly_data}}
#'
#' @export
rebuild_weather <- function(archiveDir,updateDir="",oldDir="",loud=FALSE,...) {
  
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  setwd(archiveDir)
  fl <- list.files(pattern=".rda",recursive=TRUE)
  weather_hourly <- NULL
  for (x in fl) {
    cat("loading data file ",x,"\n")
    load(x)
    assign("temp",eval(parse(text=gsub(".rda","",basename(x)))))
    temp <- hourly_localtimes(temp)
    if (loud) {
      if (length(unique(substr(temp$localdate,1,4)))>1) {
        cat("multiple years in file ",x,". \n",
            unique(substr(temp$localdate,1,4)),"\n")
      }
    }
    if (is.null(weather_hourly)) {
      weather_hourly <- temp
    } else {
      weather_hourly <- merge_hourly_data(weather_hourly,temp,loud)
    }
  }  
  setwd(old_wd)
  if (updateDir!="") {
    setwd(updateDir)
    fl <- list.files(pattern=".rda")
    flold <- list.files(pattern=".rdaold")
    fl <- setdiff(fl,flold)
    for (x in fl) {
      cat("loading data file ",x,"\n")
      load(x)
      #  grab local time info Before stacking data from diff timezones
      temp <- hourly_localtimes(weatherupdate)
      if (loud) {
        if (length(unique(substr(temp$localdate,1,4)))>1) {
          cat("multiple years in file ",x,". \n",
              unique(substr(temp$localdate,1,4)),"\n")
        }
      }
      weather_hourly <- merge_hourly_data(weather_hourly,temp,loud)
    }  
    setwd(old_wd)
  }
  if (oldDir!="") {
    cat("*******CHECKING OLD DATA FILES AGAINST CURRENT************\n")
    setwd(oldDir)
    fl <- list.files(pattern=".rda",recursive=TRUE)
    flold <- list.files(pattern=".rdaold",recursive=TRUE)
    fl <- setdiff(fl,flold)
    for (x in fl) {
      cat("loading data file ",x,"\n")
      load(x)
      #  grab local time info Before stacking data from diff timezones
      if (grepl("update",x)) {
        temp <- hourly_localtimes(weatherupdate)
      } else {
        assign("temp",eval(parse(text=gsub(".rda","",basename(x)))))
        temp <- hourly_localtimes(temp)
      }
      if (length(unique(substr(temp$localdate,1,4)))>1) {
          cat("multiple years in file ",x,". \n",
              unique(substr(temp$localdate,1,4)),"\n")
      }
      junk <- merge_hourly_data(weather_hourly,temp,loud=TRUE)
    }  
    setwd(old_wd)
    
  }
  weather_hourly <- clean_hourly_data(weather_hourly,...) %>%  
                    dplyr::arrange(weatherstation,date)  
  weather_daily <-  dailysummary(weather_hourly) %>%
                    dplyr::arrange(weatherstation,localdate) 
  cat(nrow(weather_hourly),"\n obs on ",nrow(weather_daily)," station-days\n")
  report_weather(list(daily=weather_daily,hourly=weather_hourly))
  return(list(daily=weather_daily,hourly=weather_hourly))
}
#' Update weather data with new data from wunderground
#'
#' \code{update_weatherdata} Fetch the necessary data from wunderground,
#'   archive it, process it and update the hourly detail and the daily 
#'   summary data frames.
#'
#' The daily summaries are slightly dependent on the last observation from
#' the preceding day and the first observation from the following day, as 
#' midnight values are interpolated and included in the daily measures. The
#' daily measures for the last day observed are cumulative, and change as more
#' observations are included.
#'
#' @param weatherpair a 2 item list containing the hourly and daily dataframes
#' @param weatherstation string containing the station ID (preface zip codes 
#'   with ZIPxxxxx)
#' @param beginDate a string "YYYYMMDD" identifying the first date to fetch in
#'   the interval.  If not specified, it chooses the last date specied for the
#'   weatherstation in th weatherpair dataframes.  If there is no such date,
#'   the beginDate is set to the same value as endDate.
#' @param endDate a string "YYYMMDD" identifying the last date to fetch in the 
#'   interval.  If not specified, the current Sys.Date() is used.
#' @param updateDir string containing the locally-archived wunderground data 
#'   updates directory, so that data fetched can be re-used.
#' @param newfileok if no existing update file found, create one
#' @param nofetchlimit do not retrieve more than 50 unless true
#' @param ... validity limit variables for data cleaning
#'
#' @return a list containing two dataframes.  One contains approximately hourly
#'   measures of temperature, precipitation, wind, etc, along with time and 
#'   weatherstation id. The other contains daily mins, max, means and totals 
#'   for the things in the hourly reports.  Mins and maxs are based on all 
#'   daily observations plus interpolated values for midnight (local time) at 
#'   the beginning and end of the day.  Means are based on data plus the 
#'   midnight interpolations, weighted by the elapsed time between them.  If 
#'   the data is for a time and place where Daylight Savings Time is in effect, 
#'   there will be one 23 hour and one 25 hour day per year.  Precip totals are
#'    based on the average hourly rainfall rate and a 24 hour day.
#'
#' @seealso \code{\link{rebuild_weather}} \code{\link{clean_hourly_data}}
#'
#' @export
update_weather <- function(weatherpair,weatherstation,beginDate=NA,endDate=NA,
                           updateDir="",newfileok=FALSE,nofetchlimit=FALSE,...) {
  nameweatherstation <- gsub("pws:","",weatherstation)
  wdata <- weatherpair[["hourly"]]
  wdaily <- weatherpair[["daily"]]
  station_data <- wdata[wdata$weatherstation==nameweatherstation,]
  station_daily <- wdaily[wdaily$weatherstation==nameweatherstation,]
  other_data <- wdata[wdata$weatherstation!=nameweatherstation,]
  other_daily <- wdaily[wdaily$weatherstation!=nameweatherstation,]
  
  if (missing(endDate)|is.na(endDate)) 
    endDate <- as.character(format(Sys.Date(),"%Y%m%d"))
  
  if (nrow(station_data)==0) {
    if (missing(beginDate)|is.na(beginDate)) beginDate <- endDate
  } else {
    if (missing(beginDate)|is.na(beginDate)) 
      beginDate <- max(station_data$localdate)
  }
  if (endDate < beginDate) 
    stop(paste0("endDate ",endDate," before beginDate ",beginDate))

  if (nrow(station_daily)>=2) {
    daybefore <- as.character(as.Date(beginDate,"%Y%m%d")-1,"%Y%m%d")
    hourlybefore <- station_data[station_data[["localdate"]]==daybefore,]
  } else {
    daybefore <- NA
    hourlybefore <- NULL
  }
  new_data <- update_weather_data(weatherstation,
                                  beginDate=beginDate,endDate=endDate,
                                  updateDir=updateDir,
                                  newfileok=newfileok,
                                  nofetchlimit=nofetchlimit)
  new_data_local <- hourly_localtimes(new_data)
  new_data_local <- clean_hourly_data(new_data_local,...)
  new_daily <- dailysummary(dplyr::bind_rows(hourlybefore,new_data_local))
  if (!is.na(daybefore)) 
    new_daily <- new_daily[new_daily$localdate != daybefore,]
  
  weatherdata <- merge_hourly_data(dfbig=wdata,dfoneloc=new_data_local)   
  weatherdaily <- dplyr::bind_rows(
                      dplyr::anti_join(station_daily,new_daily,
                                       by=c("weatherstation","localdate")),
                      new_daily,other_daily)   %>%
      dplyr::arrange(weatherstation,localdate)
  cat(nrow(new_data_local)," new observations on  ",nrow(new_daily)," days\n")
  return(list(daily=weatherdaily,hourly=weatherdata))
}


#' Remove data measurements that are outside of credible ranges
#'
#' \code{clean_hourly_data} Filter out extreme and implausible values from retrieved
#'   weather data.  Most limits are user parametrizable, although precip and wind speeds
#'   are rejected if nonpositive, and visibility and humidity are restricted to be 
#'   between 0 and 100.
#'
#' @param fast use first of duplicate station-time observations rather than mean()
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
#' @seealso \code{\link{update_weather}} \code{\link{rebuild_weather}}
#'
#' @export
clean_hourly_data <- function(df,fast=FALSE,temp.min=-50,temp.max=130,
                              wind_spd.max=80,wind_gust.max=120,
                              pressure.min=20,pressure.max=50,
                              wind_chill.min=-50,wind_chill.max=70,
                              heat_index.min=50,heat_index.max=150,precip.max=2) {

  vars_expect <- c(varnames_keys(),varnames_mean(),varnames_max(),
                   varnames_indicator(),varnames_first(),varnames_concat())
  needNAs <- setdiff(vars_expect, names(df))
  df[needNAs] <- NA
  
  temp <- unique(df) %>%
    #  override NAs in precip if there is a temperature reading
  dplyr::mutate(precip=replace(precip, which(is.na(precip)&!is.na(temp)), 0)) %>%
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
                precip=replace(precip, which(precip > precip.max), NA),
                precip_rate=replace(precip, which(precip_rate < 0), NA),
                precip_total=replace(precip, which(precip_total < 0), NA)
  )
  if (fast) {
    # 3s timing on sample dataset
    return(temp %>% dplyr::distinct(weatherstation,date,.keep_all=TRUE)%>%
                    dplyr::arrange(weatherstation,date,localdate,localtime))
  } else { 
    singles <-  temp %>% 
      dplyr::group_by(weatherstation,date) %>% 
      dplyr::filter(n()==1)
    return(temp %>% 
             dplyr::group_by(weatherstation,date) %>% 
             dplyr::filter(n()>1) %>%
             dplyr::do(collapse_dups(.)) %>%
             dplyr::ungroup() %>%
             dplyr::mutate(temp=replace(temp, is.nan(temp), NA),
                           hum=replace(hum, is.nan(hum), NA),
                           wind_spd=replace(wind_spd, is.nan(wind_spd), NA),
                           vis=replace(vis, is.nan(vis), NA),
                           pressure=replace(pressure, is.nan(pressure), NA),
                           wind_chill=replace(wind_chill, is.nan(wind_chill), NA),
                           heat_index=replace(heat_index, is.nan(heat_index), NA),
                           precip=replace(precip, is.nan(precip), NA),
                           precip_rate=replace(precip_rate, is.nan(precip_rate), NA),
                           precip_total=replace(precip_total, is.nan(precip_total), NA)
             ) %>%
             dplyr::bind_rows(singles) %>%
             dplyr::arrange(weatherstation,date,localdate,localtime)
           
    )
  }
}

#' Summarize a weatherdata pair
#'
#' \code{report_weather} Summarize the number of dates in the weather data archive, 
#'      and share of dates covered for each station.
#'
#' @param weatherpair list of periodic and daily weather data, as produced by 
#'   \code{\link{rebuild_weather}} 
#'
#' @return NULL
#'   
#' @seealso \code{\link{update_weather}} \code{\link{rebuild_weather}}
#'   
#'
#' @export
report_weather <- function(weatherpair) {
  stationset <- unique(weatherpair[["hourly"]][["weatherstation"]])
  dateset <- unique(weatherpair[["hourly"]][["localdate"]])
  cat("Periodic Data for ",length(dateset)," dates \n")
  station_name <- NULL
  distinct_dates <- NULL
  total_obs <- NULL
  for (x in stationset){
    station_name <- c(station_name,x)
    distinct_dates <- c(distinct_dates,
                        nrow(unique(weatherpair[["hourly"]]
                                    [weatherpair[["hourly"]]$weatherstation==x,"localdate"])))
    total_obs <- c(total_obs,
                   nrow(weatherpair[["hourly"]]
                         [weatherpair[["hourly"]]$weatherstation==x,]))
  }  
  print(data.frame(station_name,distinct_dates,total_obs,obs_per_day=total_obs/distinct_dates),right=FALSE,row.names=FALSE)
  return(NULL)
}

