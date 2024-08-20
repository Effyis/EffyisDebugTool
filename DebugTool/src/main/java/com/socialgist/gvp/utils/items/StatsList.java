package com.socialgist.gvp.utils.items;

import java.util.TreeMap;

public class StatsList {
	
	String title;
	
//	  {NAME => 'cfForever', TTL => FOREVER}, \
//	  {NAME => 'cfYear', TTL => 31536000},  # 365 days in seconds
//	  {NAME => 'cfMonth', TTL => 2678400},  # 31 days in seconds
//	  {NAME => 'cfWeek', TTL => 604800},   # 7 days in seconds
//	  {NAME => 'cfDay', TTL => 86400},    # 1 day in seconds
//	  {NAME => 'cf3h', TTL => 10800}     # 3 hours in seconds
	
	private final TreeMap<String, String> ttlForeverMap = new TreeMap<String, String>();
	private final TreeMap<String, String> ttlYearMap = new TreeMap<String, String>();
	private final TreeMap<String, String> ttlMonthMap = new TreeMap<String, String>();
	private final TreeMap<String, String> ttlWeekMap = new TreeMap<String, String>();
	private final TreeMap<String, String> ttlDayMap = new TreeMap<String, String>();
	private final TreeMap<String, String> ttlHourMap = new TreeMap<String, String>();

	public StatsList(String title) {
		this.title = title;
    }

	public String getStatsItem(String key) {
		if (ttlHourMap.containsKey(key)) return ttlHourMap.get(key); 
		if (ttlDayMap.containsKey(key)) return ttlDayMap.get(key); 
		if (ttlWeekMap.containsKey(key)) return ttlWeekMap.get(key); 
		if (ttlMonthMap.containsKey(key)) return ttlMonthMap.get(key); 
		if (ttlYearMap.containsKey(key)) return ttlYearMap.get(key);
		return ttlForeverMap.getOrDefault(key, "");		
	}
	
	public void putStatsItem(StatsTimePeriod stp, String key, String value) {
		TreeMap<String, String> map = getMapByPeriod(stp);
		map.put(key, value);
	}

	public void putStatsItem(StatsTimePeriod stp, String key, Long value) {
		TreeMap<String, String> map = getMapByPeriod(stp);
		map.put(key, value.toString());
	}
	
	public TreeMap<String, String> getMapByPeriod(StatsTimePeriod stp) {
		TreeMap<String, String> map = null;
		switch (stp) {
           case FOREVER:
        	   map = ttlForeverMap;
               break;
           case LAST_YEAR:
        	   map = ttlYearMap;
               break;
           case LAST_MONTH:
        	   map = ttlMonthMap;
               break;
           case LAST_WEEK:
        	   map = ttlWeekMap;
               break;
           case LAST_DAY:
        	   map = ttlDayMap;
               break;
           case LAST_HOUR:
        	   map = ttlHourMap;
               break;
       }		
		return map;
	}

	public String getTitle() {
		return title;
	}

	public void setLastUpdateTime() {
		
		
	    for (StatsTimePeriod stp : StatsTimePeriod.values()) {
	    	TreeMap<String, String> map = getMapByPeriod(stp);
	    	if (!map.isEmpty()) {
	    		putStatsItem(stp, "_lastUpdateUTC", GvpUtils.getCurrentISOTimeUTC());	
	    		putStatsItem(stp, "_lastUpdateLocal", GvpUtils.getCurrentISOTimeLocal());	
	    		break; // put this item to bigger ttl only;
	    	}
        }		
	}
	
}