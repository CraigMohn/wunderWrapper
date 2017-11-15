# wunderWrapper
Wrapper for the rwunderground package, fetches and saves historical weather data

These functions are wrappers for the rwunderground package, and allow users with a wunderground.com API key to retrieve historical data for a specified weather station in date range chunks and store it in a collection of .rda files.  These files can be assembled into a single dataframe, and a dataframe of daily summary measures can be calculated.  The api keys are free, but have daily and minutely query rate limitations.  Check the wunderground.com terms of service to ensure that your non-comercial use of the data is in compliance.

There are two categories of functions: weather data fetching and local storage, and stored data compilation and processing into
daily summary variables.  
