package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class VkWall {

	
	public String wall_id = null;
	public String hbase_index = null;
	public String hbase_ref_index = null;
	
	public long lastCheck_ts;
	public long deleted = 0;
	public long hidden = 0;
	
	public VkWall() {
	}
	
	public VkWall(Result result) {
		hbase_index = Bytes.toString(result.getRow());
		wall_id = hbase_index.split("\\.")[1];

		byte[] hb_lastCheck_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"));
		byte[] hb_deleted         = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("deleted"));
		byte[] hb_hidden          = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("hidden"));
		byte[] hb_ref_index       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("hbase_ref_id"));
		
		lastCheck_ts 	= (hb_lastCheck_ts!=null)?Bytes.toLong(hb_lastCheck_ts):0;
		deleted 	 	= (hb_deleted!=null)?Bytes.toLong(hb_deleted):0; 
		hidden 	 	 	= (hb_hidden!=null)?Bytes.toLong(hb_hidden):0;
		hbase_ref_index = (hb_ref_index!=null)?Bytes.toString(hb_ref_index):null;		
	}
	
}
