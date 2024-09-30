package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class VkComment {
	
	public String hbase_index = null;

	public String comment_id;
	public long created_ts;
	public long collected_ts;

	public String authorDisplayName;
		
	public VkComment() {
	}
	
	public VkComment(Result result) {

		hbase_index = Bytes.toString(result.getRow());

		byte[] hb_created_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"));
		byte[] hb_collected_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("collected_ts"));

		created_ts = (hb_created_ts!=null)?Bytes.toLong(hb_created_ts):0;				
		collected_ts = (hb_collected_ts!=null)?Bytes.toLong(hb_collected_ts):0;				
	}

/*	public VkComment(String post_id, String hbase_index, long date) {
		this.post_id = post_id;
		this.hbase_index = hbase_index;
		this.created_ts = date;
		composite_id = hbase_index.split("\\.")[1];
		owner_id = composite_id.split("\\_")[0]; 
	}
*/	
}
