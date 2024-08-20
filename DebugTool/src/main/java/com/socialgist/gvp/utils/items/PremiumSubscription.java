package com.socialgist.gvp.utils.items;

import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.support.rowset.SqlRowSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PremiumSubscription {

 	public static final int SUBSCRPTION_TYPE_VIDEO 		= 0;
	public static final int SUBSCRPTION_TYPE_CHANNEL   	= 1;
	public static final int SUBSCRPTION_TYPE_KEYWORD   	= 2;
	
	public int id;
	public int ds_id;
	public int type;

	public String rule_name;
//	public String rule_url;
	public String source_id;
	public String outputkafkatopic; 

	public String hbase_index;
	public String playlist_id;
	
	public int channel_video_days	    = 100;
//	public int channel_video_since_days = 100;
	
	public long last_start_sec;
	public long new_scheduled_time_sec;
	
	public long all_messages    = 0;
	public long new_messages    = 0;
	public long new_comments    = 0;
	
	public long reviewed_videos = 0;	
	public long reviewed_comments    = 0; 
	public long stats_sent    = 0; 

	public boolean checkCompleted = true; 
	
	Map<String, String> outputsMap = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
	
	public PremiumSubscription(){
		super();
	}
	
	public void setData(SqlRowSet result, long premium_recheck_start_hrs_utc) {	
		id 	= result.getInt("id");
		ds_id 	= result.getInt("stream_id");

		hbase_index = result.getString("hbase_index");
		playlist_id = result.getString("playlist_id");
	
		type = result.getInt("type");
		rule_name 	= result.getString("rule_name");
		
		source_id  = result.getString("youtube_id").trim();
		last_start_sec = result.getLong("last_start");
		
		outputkafkatopic = result.getString("topic");
		
		int recheckhours = result.getInt("recheck_hours");
		if (recheckhours == 0) recheckhours=1;
		new_scheduled_time_sec = (calculateNextScheduledTime(recheckhours)/1000L) + (premium_recheck_start_hrs_utc * 60 * 60);
	}
	
	public long calculateNextScheduledTime(int recheckhours) {	
		// Define the interval in milliseconds
		long intervalMillis = recheckhours * 60 * 60 * 1000; 
		// Get the current Unix timestamp (milliseconds since epoch)
		long currentTimestamp = System.currentTimeMillis();
		// Calculate the next closest time mark
		return currentTimestamp + (intervalMillis - (currentTimestamp % intervalMillis));
	}
	
	public void clearMonitroVariables() {	
		all_messages    = 0;
		new_messages    = 0;
		new_comments    = 0;
	
	}

	public JsonNode getMatchInfo(String ds_id) {
		JsonNode result = null;
		String json = outputsMap.get(ds_id);
		try {
			if (json != null) {
				JsonNode node = mapper.readTree(json);
				result = node.get("match_info");
			}	
		} catch (JsonProcessingException e) {}
		return result;	
	}
	
	public String getOutputTopic(String ds_id) {
		String result = null;
		String json = outputsMap.get(ds_id);
		try {
			if (json != null) {
				JsonNode node = mapper.readTree(json);
				result = node.get("outputkafkatopic").asText();
			}	
		} catch (JsonProcessingException e) {}
		return result;	
	}
	
}
