#' wunderWrapper: A package to generate historic data hourly requests to the package 
#'   rwunderground and save the results in .rda files.  
#'   
#' wunderWrapper offers a few functions to retrieve, save and format historical weather
#'   data from wunderground.com.  You will need a key from wunderground.com, which 
#'   has limits on request volume and rate.  See rwunderground package documentation 
#'   if you want to do more.
#'
#'
#' @section fetch data from wunderground.com:
#'   \link{update_weatherdata}, \link{nocap_build_archive}
#'
#' @section rebuild hourly/daily dataframe pair from archive data:
#'   \link{rebuild_weather}
#'
#' @importFrom rwunderground history_range get_api_key
#' @importFrom lubridate second minute hour tz date with_tz days ymd
#' @importFrom dplyr bind_rows anti_join arrange group_by distinct first funs lag n do
#' @importFrom magrittr %>%
#' @importFrom pracma Mode
#' @importFrom stats approx
#' @name wunderWrapper
NULL

###  make the R checker happy
tedious <- utils::globalVariables(c(".","weatherstation","weatherupdate","localtime","localdate","utctime",
                                    "temp","dew_pt","hum","wind_spd","wind_gust","vis","precip",
                                    "pressure","wind_chill","heat_index","gustratio","snow","hail",
                                    "minutes","cond","fog","rain","thunder","tornado","time","localtz",
                                    "precip_total","precip_rate","min_temp","max_temp"))

