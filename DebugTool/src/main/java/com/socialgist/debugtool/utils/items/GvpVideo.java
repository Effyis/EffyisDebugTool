package com.socialgist.debugtool.utils.items;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class GvpVideo {
    
	public String video_id;

	@JsonIgnore public String hbase_index;
	@JsonIgnore	public String prefix;
	public String channel_id;

	@JsonIgnore	public long created_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime created;
	
	public long deleted = 0;
	
	// Stats 
	public long commentsCount;
	public long commentsSent;
	
	@JsonIgnore	public long lastComment_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime lastComment;
	@JsonIgnore	public long lastCheck_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime lastCheck;
	
	@JsonIgnore	 public long new_commentsCount;
	@JsonIgnore	 public long new_commentsSent;
	@JsonIgnore	 public long new_lastComment_ts;
	@JsonIgnore	 public long new_lastCheck_ts;
	@JsonIgnore	 public long new_deleted;

	@JsonIgnore	public long stats_comments_api_calls;
	@JsonIgnore	public long stats_reply_api_calls;
	@JsonIgnore	public long stats_comments_collected;
	@JsonIgnore	public String stats_border_reached = "not reached";
	@JsonIgnore	public long stats_start_ts;
	@JsonIgnore	public long stats_duration;
	
	@JsonIgnore	public int existed_comments_checked_for_replies = 0;
	@JsonIgnore	public long new_comment_updated_ts = 0;	
    public enum PremiumStatus {
        UNDEFINED, 
        PREMIUM, 
        NO_PREMIUM
    }    
    @JsonIgnore	public PremiumStatus premiumStatus;
    @JsonIgnore	public Map<String, String> outputsMap;

    @JsonIgnore	public long video_border_ts;
    @JsonIgnore	public boolean clear_flag = false;
    
	public GvpVideo(String video_id) {
		this.video_id = video_id;
		premiumStatus = PremiumStatus.UNDEFINED;
	}    
    
	public GvpVideo(String hbase_index, long created_ts) {
		
		this.hbase_index = hbase_index; 
		prefix = hbase_index.split("\\.")[0];
		video_id = hbase_index.split("\\.")[3];
		this.created_ts  = created_ts;
		premiumStatus = PremiumStatus.UNDEFINED; 
    }
	
	public GvpVideo(Result result) {
		
		hbase_index = Bytes.toString(result.getRow()); 
		prefix = hbase_index.split("\\.")[0];
		video_id = hbase_index.split("\\.")[3];

		byte[] hb_created_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"));
		
		byte[] hb_commentsCount   = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("commentsCount_int"));
		byte[] hb_commentsSent   = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("commentsSent_int"));
		byte[] hb_lastComment_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastComment_ts"));
		byte[] hb_lastCheck_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"));
		byte[] hb_deleted    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("deleted"));
		
//		byte[] hb_checkedForGUID  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("checkedForGUID"));
//    	byte[] hb_guidvalue       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("guid"));
		
    	byte[] hb_channel_id       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("channel_id"));
    	channel_id 	   = (hb_channel_id!=null)?Bytes.toString(hb_channel_id):"";
		created_ts = (hb_created_ts!=null)?Bytes.toLong(hb_created_ts):0;				
		commentsCount = (hb_commentsCount!=null)?Bytes.toLong(hb_commentsCount):0;				
		commentsSent  = (hb_commentsSent!=null)?Bytes.toLong(hb_commentsSent):0;				
		lastComment_ts 	  = (hb_lastComment_ts!=null)?Bytes.toLong(hb_lastComment_ts):0;				
		lastCheck_ts = (hb_lastCheck_ts!=null)?Bytes.toLong(hb_lastCheck_ts):0;
		
//*****************************************		
    	int size = 0; 
    	if (hb_deleted != null) { // long 
    		size = hb_deleted.length;
    	}
    	if (size == 8) { // long 
			deleted = (hb_deleted!=null)?Bytes.toLong(hb_deleted):0;  //  LONG
    	}
    	if (size == 4) { // long 
			deleted = (hb_deleted!=null)?Bytes.toInt(hb_deleted):0;  //   INT
    	}
//*****************************************		
//		checkedForGUID = (hb_checkedForGUID!=null)?Bytes.toLong(hb_checkedForGUID):0;
//		index_GUID 	   = (hb_guidvalue!=null)?Bytes.toString(hb_guidvalue):"";
		premiumStatus = PremiumStatus.UNDEFINED;
		
		created = epochSecondsToLocalDateTimeUTC(created_ts);		
		lastCheck = epochSecondsToLocalDateTimeUTC(lastCheck_ts);		
		lastComment = epochSecondsToLocalDateTimeUTC(lastComment_ts);		
		
    }

	public void fixPubDate(String hbase_index, long created_ts) {

		this.hbase_index = hbase_index; 
		prefix = hbase_index.split("\\.")[0];
		video_id = hbase_index.split("\\.")[3];
		this.created_ts  = created_ts;
		
	}
	
	 public static LocalDateTime epochSecondsToLocalDateTimeUTC(long epochSeconds) {
	        Instant instant = Instant.ofEpochSecond(epochSeconds);
	        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
	 }	
	
	
}