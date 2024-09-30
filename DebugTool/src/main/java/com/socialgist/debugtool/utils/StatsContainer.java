package com.socialgist.debugtool.utils;

import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.socialgist.debugtool.utils.items.StatsCounter;
import com.socialgist.debugtool.utils.items.StatsList;
import com.socialgist.debugtool.utils.items.StatsTimePeriod;
import com.socialgist.debugtool.utils.items.StatsType;

public class StatsContainer {

	@Value("${stats.prefix:stats}")
	public String stats_prefix;
	
	@Autowired
	HBaseContainer hbaseContainer;
	
	public void init() {
	}

	public synchronized TreeMap<String, String> hBase_getStats(String hbase_index) {
		
		TreeMap<String, String> map = new TreeMap<String, String>();
		
		Result result = hbaseContainer.hBase_getStats(hbase_index);
		if ((result != null) && !result.isEmpty()) { // STATS FOUND IN HBASE
		    for (StatsTimePeriod stp : StatsTimePeriod.values()) {
		    	// Extract values from the Result instance and populate the HashMap
		    	for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes(stp.getCfName())).keySet()) {
		    		byte[] valueBytes = result.getValue(Bytes.toBytes(stp.getCfName()), qualifier);
		    		String value = Bytes.toString(valueBytes);
		    		String qualifierStr = Bytes.toString(qualifier);
		    		map.put(qualifierStr, value);
		    	} 
		    }
		}
		return map;
	}	

	public synchronized void saveStatsList(StatsList stats) {
		String hbase_index = stats_prefix + "." + stats.getTitle();
		stats.setLastUpdateTime();
		hbaseContainer.hBase_putStats(hbase_index, stats); 
	}
	
	public synchronized void saveStatsCounter(StatsCounter statsCounter) {

	    // Access the errorCounts map
        EnumMap<StatsType, Long> statsCounts = statsCounter.getStatsCounts();
        // Iterate over the stats counts
        for (Entry<StatsType, Long> e : statsCounts.entrySet()) {
        	if (e.getValue() == 0) continue;
    		String hbase_index = stats_prefix + "." + statsCounter.getTitle() + "." + e.getKey().name();
            hbaseContainer.hBase_incrementTimeStats(hbase_index, e.getValue());
//            long count = p.statsCounter.getStatsCount(statsType);
//            System.out.println(statsType.name() + ": " + count);
        }
	}
	
	public synchronized void incrementStatsCount(StatsType statsType, long value) {
		String hbase_index = stats_prefix + "." + statsType.name();
        hbaseContainer.hBase_incrementTimeStats(hbase_index, value);
	}
	
}
