##  lists of variables processed by code
##    Do not export, avoid namespace pollution!
##    "date" contains a POSIXct time-date
varnames_keys <- function(){
  return(c("weatherstation","date"))
}
varnames_mean <- function() {
  return(c("temp","dew_pt","hum","wind_spd","vis","pressure","wind_chill",
           "heat_index","precip","precip_rate","wind_dir","gust_ratio"))
}
varnames_max <- function() {
  return(c("wind_gust","precip_total"))
}
varnames_indicator <- function() {
  return(c("fog","rain","snow","hail","thunder","tornado"))
}
varnames_first <- function() {
  return(c("localdate","localtime","localtz"))
}
varnames_concat <- function() {
  return(c("cond"))
}
