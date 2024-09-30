package com.socialgist.debugtool.utils.items;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class GvpChannel {

	public String channel_id;
	public String playlist_id;
	@JsonIgnore	public String hbase_index;
	@JsonIgnore	public String prefix;
	public long thread;

	@JsonIgnore	public long created_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime created;
	@JsonIgnore	public long lastCheck_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime lastCheck;
	@JsonIgnore	public long scheduled_ts;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
	public LocalDateTime scheduled;

	public long deleted = 0;
	
	@JsonIgnore	public long new_scheduled_ts;
	
	@JsonIgnore	public PremiumSubscription ps;

	@JsonIgnore	public boolean clear_flag = false;

	public GvpChannel() {
	}
	
	public GvpChannel(String channel_id) {
		super();
		this.channel_id = channel_id;
	}

	public GvpChannel(PremiumSubscription ps) {
		super();
		this.ps = ps;
		this.channel_id = ps.source_id;
	}

	public GvpChannel(String hbase_index, String playlist_id) {
		this.hbase_index = hbase_index;
		prefix     = hbase_index.split("\\.")[0];
		thread     = Long.parseLong(hbase_index.split("\\.")[1]);
		created_ts     = Long.parseLong(hbase_index.split("\\.")[2]);
		channel_id = hbase_index.split("\\.")[3];
		this. playlist_id = playlist_id;
	}
	
	public GvpChannel(Result result) {

		hbase_index = Bytes.toString(result.getRow());
		prefix     = hbase_index.split("\\.")[0];
		thread     = Long.parseLong(hbase_index.split("\\.")[1]);
		created_ts     = Long.parseLong(hbase_index.split("\\.")[2]);
		channel_id = hbase_index.split("\\.")[3];

//		byte[] hb_created_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"));
		
		byte[] hb_deleted    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("deleted"));
		byte[] hb_playlist_id  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("playlist_id"));
		byte[] hb_lastCheck_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"));
		byte[] hb_scheduled_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("scheduled_ts"));
		
//		created_ts = (hb_created_ts!=null)?Bytes.toLong(hb_created_ts):0;				
		lastCheck_ts = (hb_lastCheck_ts!=null)?Bytes.toLong(hb_lastCheck_ts):0;
		scheduled_ts = (hb_scheduled_ts!=null)?Bytes.toLong(hb_scheduled_ts):0;

		
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
		
		
		playlist_id = (hb_playlist_id!=null)?Bytes.toString(hb_playlist_id):"no_play_list";
		
		
		created = epochSecondsToLocalDateTimeUTC(created_ts);		
		lastCheck = epochSecondsToLocalDateTimeUTC(lastCheck_ts);		
		scheduled = epochSecondsToLocalDateTimeUTC(scheduled_ts);		
		
    }

	public boolean isScheduledTimeReached() {
		if (scheduled_ts < System.currentTimeMillis() / 1000L) 
			return true;
		else 
			return false;
	}

	 public static LocalDateTime epochSecondsToLocalDateTimeUTC(long epochSeconds) {
	        Instant instant = Instant.ofEpochSecond(epochSeconds);
	        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
	 }	
	
}
