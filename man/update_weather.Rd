% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pairCode.R
\name{update_weather}
\alias{update_weather}
\title{Update weather data with new data from wunderground}
\usage{
update_weather(weatherpair, weatherstation, beginDate = NA, endDate = NA,
  updateDir = "", newfileok = FALSE, nofetchlimit = FALSE, ...)
}
\arguments{
\item{weatherpair}{a 2 item list containing the hourly and daily dataframes}

\item{weatherstation}{string containing the station ID (preface zip codes 
with ZIPxxxxx)}

\item{beginDate}{a string "YYYYMMDD" identifying the first date to fetch in
the interval.  If not specified, it chooses the last date specied for the
weatherstation in th weatherpair dataframes.  If there is no such date,
the beginDate is set to the same value as endDate.}

\item{endDate}{a string "YYYMMDD" identifying the last date to fetch in the 
interval.  If not specified, the current Sys.Date() is used.}

\item{updateDir}{string containing the locally-archived wunderground data 
updates directory, so that data fetched can be re-used.}

\item{newfileok}{if no existing update file found, create one}

\item{nofetchlimit}{do not retrieve more than 50 unless true}

\item{...}{validity limit variables for data cleaning}
}
\value{
a list containing two dataframes.  One contains approximately hourly
  measures of temperature, precipitation, wind, etc, along with time and 
  weatherstation id. The other contains daily mins, max, means and totals 
  for the things in the hourly reports.  Mins and maxs are based on all 
  daily observations plus interpolated values for midnight (local time) at 
  the beginning and end of the day.  Means are based on data plus the 
  midnight interpolations, weighted by the elapsed time between them.  If 
  the data is for a time and place where Daylight Savings Time is in effect, 
  there will be one 23 hour and one 25 hour day per year.  Precip totals are
   based on the average hourly rainfall rate and a 24 hour day.
}
\description{
\code{update_weatherdata} Fetch the necessary data from wunderground,
  archive it, process it and update the hourly detail and the daily 
  summary data frames.
}
\details{
The daily summaries are slightly dependent on the last observation from
the preceding day and the first observation from the following day, as 
midnight values are interpolated and included in the daily measures. The
daily measures for the last day observed are cumulative, and change as more
observations are included.
}
\seealso{
\code{\link{rebuild_weather}} \code{\link{clean_hourly_data}}
}
