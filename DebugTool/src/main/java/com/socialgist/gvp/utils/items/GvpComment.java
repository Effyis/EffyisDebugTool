package com.socialgist.gvp.utils.items;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.socialgist.gvp.utils.items.GvpVideo.PremiumStatus;

public class GvpComment {
    
	public String comment_id;

	public String video_id;
	public String channel_id;
	public String authorDisplayName;
	
	public String publishedAt;
	public long created_ts;
	public long collected_ts;

	public String hbase_index;
	@JsonIgnore	public String prefix;
	
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime created;

	public GvpComment() {
	}
	
	public GvpComment(String comment_id, String video_id, String channel_id, long created_ts) {
		this.comment_id = comment_id;
		this.channel_id = channel_id;
		this.video_id = video_id;
		this.created_ts = created_ts;
	}
	
	public GvpComment(Result result) {
		hbase_index = Bytes.toString(result.getRow()); 
		byte[] hb_collected_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("collected"));
    	collected_ts = (hb_collected_ts!=null)?Bytes.toLong(hb_collected_ts):0;				
    }

	public static LocalDateTime epochSecondsToLocalDateTimeUTC(long epochSeconds) {
	        Instant instant = Instant.ofEpochSecond(epochSeconds);
	        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
	 }	
	
	
}