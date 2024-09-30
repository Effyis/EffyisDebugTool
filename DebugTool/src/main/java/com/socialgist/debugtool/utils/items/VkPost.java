package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class VkPost {
	
	public String hbase_index = null;

	public String composite_id;
	public String post_id;
	public String owner_id;
	public long created_ts;
	
	// Stats
	public long api_commentsCount;
	// HBase Stats
	public long hbase_commentsCount;
	public long hbase_commentsSent;
	public long hbase_lastComment_ts;
	public long hbase_lastCheck_ts;
	
	public VkPost() {
	}
	
	public VkPost(Result result) {

		hbase_index = Bytes.toString(result.getRow());
		composite_id = hbase_index.split("\\.")[1];
		composite_id = hbase_index.split("\\.")[1];
		owner_id = composite_id.split("\\_")[0]; 
		post_id = composite_id.split("\\_")[1]; 

		byte[] hb_created_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"));
		
		byte[] hb_commentsCount   = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("commentsCount"));
		byte[] hb_commentsSent   = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("commentsSent"));
		byte[] hb_lastComment_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastComment_ts"));
		byte[] hb_lastCheck_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"));
    	byte[] hb_owner_id       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("owner_id"));

    	owner_id 	   = (hb_owner_id!=null)?Bytes.toString(hb_owner_id):"";
		created_ts = (hb_created_ts!=null)?Bytes.toLong(hb_created_ts):0;				
		hbase_commentsCount = (hb_commentsCount!=null)?Bytes.toLong(hb_commentsCount):0;				
		hbase_commentsSent  = (hb_commentsSent!=null)?Bytes.toLong(hb_commentsSent):0;				
		hbase_lastComment_ts 	  = (hb_lastComment_ts!=null)?Bytes.toLong(hb_lastComment_ts):0;				
		hbase_lastCheck_ts = (hb_lastCheck_ts!=null)?Bytes.toLong(hb_lastCheck_ts):0;
	}

	public VkPost(String post_id, String hbase_index, long date) {
		this.post_id = post_id;
		this.hbase_index = hbase_index;
		this.created_ts = date;
		composite_id = hbase_index.split("\\.")[1];
		owner_id = composite_id.split("\\_")[0]; 
	}
	
}
