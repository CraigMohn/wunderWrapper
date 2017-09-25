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
#' @importFrom lubridate second minute hour tz date
#' @importFrom dplyr bind_rows anti_join arrange group_by distinct
#' @importFrom magrittr %>%
#' @importFrom pracma Mode
#' @importFrom stats approx
#' @name wunderWrapper
NULL
