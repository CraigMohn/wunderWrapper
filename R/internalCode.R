hourly_localtimes <- function(hourlydata) {
  if (!is.null(hourlydata)) {
    hourlydata$localtz <- lubridate::tz(hourlydata$date)
    hourlydata$localdate <- as.character(lubridate::date(hourlydata$date),"%Y%m%d")
    hourlydata$localtime <- as.character(hourlydata$date,"%H%M%S")
    hourlydata$gustratio <- hourlydata$wind_gust/hourlydata$wind_spd
    hourlydata$gustratio[is.na(hourlydata$gustratio)] <- 1
    hourlydata$wind_dir <- dir_numeric(hourlydata$dir)
    hourlydata$dir <- NULL
  }
  return(hourlydata)
}
dailysummary <- function(hourlydata) {
  hourlyaugmented <- add_midnights(hourlydata)
  weatherdaily <- hourlyaugmented %>% 
    dplyr::mutate(minutes=as.numeric(substr(localtime,3,4))) %>%    
    dplyr::group_by(weatherstation,localdate) %>%
    # use the data that's there - min, max, weighted_mean_rate*24
    dplyr::summarize(min_temp=min(temp,na.rm=TRUE),
                     max_temp=max(temp,na.rm=TRUE),
                     mean_temp=weighted_mean(date,temp),
                     max_windspd=max(max(wind_spd,na.rm=TRUE),0),
                     mean_windspd=weighted_mean(date,wind_spd),
                     num_hourly_readings=count_on_mode_minutes(localtime),
                     tot_precip=weighted_mean(date,precip)*24,
                     any_gust=any(gustratio>1.6,na.rm=TRUE),
                     any_rain=any(rain>0,na.rm=TRUE),
                     any_snow=any(snow>0,na.rm=TRUE),
                     any_hail=any(hail>0,na.rm=TRUE)) %>%
    dplyr::mutate(min_temp=replace(min_temp,min_temp > 200,NA),
                  max_temp=replace(max_temp,max_temp < -100,NA) )
  return(weatherdaily)
}
zip_history_range <- function(location,date_start,date_end,key) {
  if (substr(location,1,3)=="ZIP") location <- substr(location,4,nchar(location))
  df <- rwunderground::history_range(location=location,date_start=date_start,
                                     date_end=date_end,key=key)
  df$fetchtime <- Sys.time()
  return(df)
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
  #  for each station+date combo, create (local) midnight entry at time 000000 with missing values
  newmidnight <- hourlydata %>% dplyr::distinct(weatherstation,localdate,.keep_all=TRUE) %>%
                    dplyr::select(weatherstation,localdate,localtz) %>%
                    dplyr::mutate(localtime="000000",
                                  temp=NA,wind_spd=NA,hum=NA,pressure=NA,
                                  precip=NA,dew_pt=NA,gustratio=NA,
                                  rain=NA,snow=NA,hail=NA,
                                  date=Sys.time()
                                  ) 
  #  work station by station
  stationset <- unique(newmidnight$weatherstation)
  for (x in stationset) {
    #  new midnight obs for this station - set POSIXct time based on station tz
    newmidnightstation <- newmidnight[newmidnight$weatherstation==x,]
    newmidnightstation$date <- as.POSIXct(paste0(newmidnightstation$localdate,
                                                 newmidnightstation$localtime),
                                   format="%Y%m%d%H%M%S",tz=newmidnightstation$localtz[1])
    #  pass all current station hourly data
    newvars <- interpolate_for_station(newmidnightstation,
                                       hourlydata[hourlydata$weatherstation==x,])
    for (y in c("temp","wind_spd","hum","pressure","precip","dew_pt")) {
       newmidnight[newmidnight$weatherstation==x,y] <- newvars[[y]]
    }
    newmidnight[newmidnight$weatherstation==x,"date"] <- newmidnightstation[["date"]]
  }
  newmidnight <- dplyr::arrange(newmidnight,weatherstation,date,localdate,localtime)
  if (nrow(newmidnight)>1) {
    #  midnight 000000 for any day which follows another is midnight 240000 needed for 
    #   averaging and max/min for the previous day.
    #   days which precede a day in the dataset can get a midnight 240000 
    #     interpolated value from the following day's values. 
    #   other days will have midnight 240000 values interpolated to NA since following 
    #     values are NA for 24 hours, so just skip them...
    has_day_preceding <- newmidnight[["weatherstation"]] == 
                            dplyr::lag(newmidnight[["weatherstation"]]) &
                         lubridate::ymd(newmidnight[["localdate"]]) == 
                          dplyr::lag(lubridate::ymd(newmidnight[["localdate"]]))+
                                     lubridate::days(1)
    has_day_preceding[is.na(has_day_preceding)]<-FALSE
    endmidnight <- newmidnight[has_day_preceding,]
    endmidnight$localtime <- "240000"
    endmidnight$localdate <- as.character(as.Date(endmidnight$localdate,"%Y%m%d")-1,"%Y%m%d")
  } else {
    endmidnight <- NULL
  }
  augmented_data <- dplyr::bind_rows(hourlydata,newmidnight,endmidnight) %>%
                    dplyr::arrange(weatherstation,date,localdate,localtime)
  return(augmented_data)
}
interpolate_for_station <- function(newtimedf,hourlydata) {
  listret <- NULL
  varnames <- c("temp","wind_spd","hum","vis","pressure","precip","dew_pt")
  for (y in varnames) {
    if (sum(!is.na(hourlydata[[y]]))>=2) {
      #  boundaries get NA
      new_vals <- stats::approx(hourlydata$date, y = hourlydata[[y]], 
                                 xout = newtimedf$date,rule=1)[[2]]
    } else {
      new_vals <- NA
    }
    listret <- c(listret,list(new_vals))
  }
  names(listret) <- varnames
  return(listret)
}
weighted_mean <- function(timevar,quantity) {
  #  assumes that duplicate times have been removed
  timevar <- timevar[is.finite(quantity)]
  quantity <- quantity[is.finite(quantity)]
  tvorder <- order(timevar)
  timevar <- timevar[tvorder]
  quantity <- quantity[tvorder]
  #  these work right on the boundary, dplyr lead/lag don't do what is needed.
  lagtime <- c(timevar[1],timevar[-length(timevar)])
  leadtime <- c(timevar[-1],timevar[length(timevar)])
  return(sum(as.numeric(difftime(leadtime,lagtime),units=c("secs"))*quantity)/
           sum(as.numeric(difftime(leadtime,lagtime),units=c("secs"))))
}

dir_numeric <- function(dirword) {
  dn <- rep(NA,length(dirword))
  dn[dirword=="North"] <- 0
  dn[dirword=="NNE"] <- 0.0625
  dn[dirword=="NE"] <- 0.1250
  dn[dirword=="ENE"] <- 0.1875
  dn[dirword=="East"] <- 0.2500
  dn[dirword=="ESE"] <- 0.3125
  dn[dirword=="SE"] <- 0.3750
  dn[dirword=="SSE"] <- 0.4375
  dn[dirword=="South"] <- 0.5000
  dn[dirword=="SSW"] <- 0.5625
  dn[dirword=="SW"] <- 0.6250
  dn[dirword=="WSW"] <- 0.6875
  dn[dirword=="West"] <- 0.7500
  dn[dirword=="WNW"] <- 0.8125
  dn[dirword=="NW"] <- 0.8750
  dn[dirword=="NNW"] <- 0.9375
  return(dn)
}

collapse_dups <- function(df) {
  outdf <- df[1,]
  for (v in intersect(names(df),varnames_mean())) {
    outdf[1,v] <- mean(df[[v]],na.rm=TRUE)
  }
  for (v in intersect(names(df),varnames_max())) {
    if (all(is.na(df[[v]]))) {
      outdf[1,v] <- NA
    } else {
      outdf[1,v] <- max(df[[v]],na.rm=TRUE)
    }
  }
  for (v in intersect(names(df),varnames_indicator())) {
    if (all(is.na(df[[v]]))) {
      outdf[1,v] <- NA
    } else {
      outdf[1,v] <- max(max(df[[v]],na.rm=TRUE),0)
    }
  }
  for (v in intersect(names(df),varnames_concat())) {
    if (all(is.na(df[[v]]))) {
      outdf[1,v] <- NA
    } else {
      outdf[1,v] <- paste0(unique(gsub(" ","",df[[v]])),
                         collapse='',sep=' ')
    }
  }
  #this loop doesn't do anything because outdf initialized to df[1,]
  #for (v in intersect(names(df),varnames_first())) {
  #  outdf[1,v] <- df[1,v]
  #}
  return(outdf)
}

merge_hourly_data <- function(dfbig,dfoneloc,loud=FALSE) {

   loc <- unique(dfoneloc$weatherstation)
   if (length(loc) > 1) {
     print(unique(dfoneloc$weatherstation))
     stop("multiple locations in merged dataset")
   }
   sameloc <- dfbig[dfbig$weatherstation==loc,]
   otherloc <- dfbig[dfbig$weatherstation!=loc,]
   if (nrow(sameloc) == 0) {
     allsameloc <- dfoneloc
   } else {
     allsameloc <- merge_location_data(sameloc,dfoneloc,byvar="date",
                                       rankvar="fetchtime",sortvar="date",
                                       loud=loud) 
   } 
   allsameloc <- force_POSIX_dfvar(allsameloc,"fetchtime")
   if (nrow(otherloc)>0) otherloc <- force_POSIX_dfvar(otherloc,"fetchtime")
   return(
      allsameloc %>%
      dplyr::bind_rows(otherloc) %>%
      dplyr::arrange(weatherstation,date)
   ) 
}  

force_POSIX_dfvar <- function(df,varname,earlydate="1960-01-01")  {
  if (lubridate::is.POSIXct(df[[varname]])) {
    df[[varname]][is.na(df[[varname]])] <- as.POSIXct(earlydate)
  } else {
    tt <- try(as.POSIXct(df[[varname]]),silent=TRUE)
    df[[varname]] <- NULL
    if (!inherits(tt, "try-error")) {
      df[[varname]] <- tt
      df[[varname]][is.na(df[[varname]])] <- as.POSIXct(earlydate)
    } else {
      df[[varname]] <- as.POSIXct(earlydate)
    }
  }
  return(df)
}


