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
#'   Data will be loaded from this directory and its subdirectory tree.
#' @param update.dir string containing the locally-archived wunderground data updates directory.
#' @param ... validity limit variables for data cleaning by \code{\link{clean_hourly_data}}
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
#' @seealso \code{\link{update_weatherdata}}     \code{\link{clean_hourly_data}}
#'
#' @export
rebuild_weather <- function(archive.dir,update.dir="",...) {
  
  old_wd <- getwd()
  on.exit(setwd(old_wd))
  setwd(archive.dir)
  fl <- list.files(pattern=".rda",recursive=TRUE)
  weather_hourly <- NULL
  for (x in fl) {
    cat("loading data file ",x,"\n")
    load(x)
    assign("temp",eval(parse(text=gsub(".rda","",basename(x)))))
    temp <- hourly_localtimes(temp)
    if (is.null(weather_hourly)) {
      weather_hourly <- temp
    } else {
      weather_hourly <- dplyr::bind_rows(dplyr::anti_join(weather_hourly,temp,
                                               #by=c("weatherstation","localdate","localtime")),
                                               by=c("weatherstation","date")),
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
      cat("loading data file ",x,"\n")
      load(x)
      #  grab local time info Before stacking data from diff timezones
      temp <- hourly_localtimes(weatherupdate)
      weather_hourly <- dplyr::bind_rows(dplyr::anti_join(weather_hourly,temp,
                                             by=c("weatherstation","date")),
                                         temp)
    }  
    setwd(old_wd)
  }
  weather_hourly <- clean_hourly_data(weather_hourly,...) %>%  
                    dplyr::arrange(weatherstation,date)  
  weather_daily <-  dailysummary(weather_hourly) %>%
                    dplyr::arrange(weatherstation,localdate) 
  cat(nrow(weather_hourly),"\n obs on ",nrow(weather_daily)," station-days\n")
  weather_report(list(daily=weather_daily,hourly=weather_hourly))
  return(list(daily=weather_daily,hourly=weather_hourly))
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
#' @seealso \code{\link{update_weatherdata}} \code{\link{rebuild_weather}}
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
#             dplyr::summarize(temp=mean(temp,na.rm=TRUE),
#                              dew_pt=mean(dew_pt,na.rm=TRUE),
#                              hum=mean(hum,na.rm=TRUE),
#                              wind_spd=mean(wind_spd,na.rm=TRUE),
#                              wind_gust=max(max(wind_gust,na.rm=TRUE),0),
#                              vis=mean(vis,na.rm=TRUE),
#                              pressure=mean(pressure,na.rm=TRUE),
#                              wind_chill=mean(wind_chill,na.rm=TRUE),
#                              heat_index=mean(heat_index,na.rm=TRUE),
#                              precip=mean(precip,na.rm=TRUE),
#                              fog=max(max(fog,na.rm=TRUE),0),
#                              rain=max(max(rain,na.rm=TRUE),0),
#                              snow=max(max(snow,na.rm=TRUE),0),
#                              hail=max(max(hail,na.rm=TRUE),0),
#                              thunder=max(max(thunder,na.rm=TRUE),0),
#                              tornado=max(max(tornado,na.rm=TRUE),0),
#                              localdecimaltime <- first(localdecimaltime),
#                              localtime=dplyr::first(localtime),
#                              localdate=dplyr::first(localdate),
#                              localtz=dplyr::first(localtz)
#             ) %>%
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
#' \code{weather_report} Summarize the number of dates in the weather data archive, 
#'      and share of dates covered for each station.
#'
#' @param weatherpair list of periodic and daily weather data, as produced by 
#'   \code{\link{rebuild_weather}} 
#'
#' @return NULL
#'   
#' @seealso \code{\link{update_weatherdata}} \code{\link{rebuild_weather}}
#'   
#'
#' @export
weather_report <- function(weatherpair) {
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
