package com.socialgist.debugtool.utils.items;

import java.util.EnumMap;

public class StatsCounter {

	String title;
	
    // Create an EnumMap to store stats counts
    private final EnumMap<StatsType, Long> statsCounts = new EnumMap<>(StatsType.class);

	public StatsCounter(String title) {
		this.title = title;
        // Initialize stats counts to 0
        for (StatsType statsType : StatsType.values()) {
            statsCounts.put(statsType, 0L);
        }
    }

    // Method to increment the stats count for a specific type
    public void incrementStatsCount(StatsType statsType) {
        long currentCount = statsCounts.getOrDefault(statsType, 0L);
        statsCounts.put(statsType, currentCount + 1);
    }

    // Method to get the stats count for a specific type
    public long getStatsCount(StatsType statsType) {
        return statsCounts.getOrDefault(statsType, 0L);
    }

    public EnumMap<StatsType, Long> getStatsCounts() {
		return statsCounts;
	}

	public String getTitle() {
		return title;
	}
    
}