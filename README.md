# wunderWrapper
Wrapper for the rwunderground package, fetches and saves historical weather data

These functions are wrappers for the rwunderground package, and allow users with a wunderground.com API key to retrieve historical data for a specified weather station in date range chunks and store it in a collection of .rda files.  These files can be assembled into a single dataframe, and a dataframe of daily summary measures can be calculated.  This is a quick patch to use the API interface after the website ceased providing historical data in csv format.  The api keys are free, but have daily and minutely query rate limitations.  Check the wunderground.com terms of service to ensure that your non-comercial use of the data is in compliance.

Because it is a quick patch for my specific needs, the documentation is at best sparse, and probably doesn't answer your questions.  The daily summary probably doesn't contain the exact variable you want.  The code is pretty simple.  

High priority issue - need to do better weighted averaging when more than 24 observations at a station in a day for mean and total quantities.
