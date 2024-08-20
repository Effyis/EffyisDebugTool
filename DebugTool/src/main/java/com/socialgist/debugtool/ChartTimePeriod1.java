package com.socialgist.debugtool;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public enum ChartTimePeriod1 {

	FOREVER("All time",      "cfForever", 30*24*60*60, 60, "HH:mm"),
	LAST_YEAR("Last Year",   "cfYear", 7*24*60*60, 53, "HH:mm"),
	LAST_MONTH("Last Month", "cfMonth", 12*60*60, 60, "HH:mm"),
	LAST_WEEK("Last Week",   "cfWeek", 3*60*60, 60, "HH:mm"),
	LAST_DAY("Last Day",     "cfDay", 30*60, 50, "HH:mm"),
	LAST_HOUR("Last Hour",   "cf3h", 5 * 60, 12, "HH:mm");  // 12 items every 5 minutes
	

	/*
	    LAST_YEAR("Last Year", "cfYear", 7*24*60*60, 53, "HH:mm"),
	    LAST_MONTH("Last Month", "cfMonth", 12*60*60, 60, "HH:mm"),
	    LAST_WEEK("Last Week", "cfWeek", 3*60*60, 60, "HH:mm"),
	    LAST_DAY("Last Day", "cfDay", 30*60, 50, "HH:mm"),
	    LAST_HOUR("Last Hour", "stats_5m", 5 * 60, 60, "HH:mm");
//	    LAST_HOUR("Last Hour", "stats_5m", 5 * 60, 12, "HH:mm");
*/
	    private final String title;
	    private final String cfName;
	    private final int periodInSeconds;
	    private final int numberOfPeriods;
	    private final DateTimeFormatter formatter;

	    ChartTimePeriod1(String title, String cfName, int periodInSeconds, int numberOfPeriods, String pattern) {
	        this.title = title;
	        this.cfName = cfName;
	        this.periodInSeconds = periodInSeconds;
	        this.numberOfPeriods = numberOfPeriods;
	        formatter = DateTimeFormatter.ofPattern(pattern);
	    }

		public String getTitle() {
			return title;
		}

		public String getCfName() {
			return cfName;
		}

		public int getPeriodInSeconds() {
			return periodInSeconds;
		}

		public int getNumberOfPeriods() {
			return numberOfPeriods;
		}
		
		public String formatTime(long unixTimestamp) {
			Instant instant = Instant.ofEpochSecond(unixTimestamp);
	        ZoneId zoneId = ZoneId.systemDefault();
	        LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
	        return localDateTime.format(formatter);
		}
	
}
