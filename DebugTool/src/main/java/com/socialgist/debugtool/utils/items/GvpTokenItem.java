package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GvpTokenItem {

	public String id = "";
	public String token = "";
	public long used_quota = 0;
	public long used_quota_dt = 0; 
	public long total_quota;
	public long lastuse;
	public boolean isActive;
	public boolean isQuotaExceeded;
	
	public GvpTokenItem(String token) {
		this.token = token;
		if (token == null)
			this.token = "";
		this.isActive = true;
	}

	public GvpTokenItem(String id, String token, long quota) {

		this.id = id;
		this.token = token;
		if (token == null)
			this.token = "";
		this.total_quota = quota;
		this.isActive = true;
	}

	public GvpTokenItem(Result result) {

		String hbase_index = Bytes.toString(result.getRow());
		token     = hbase_index.split("\\.")[0];
		used_quota_dt     = Long.parseLong(hbase_index.split("\\.")[1]);
 	   
    	byte[] hb_lastuse_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"));
      	byte[] hb_quota_used       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"));
		byte[] hb_quota_exceeded = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"));
      			
  		lastuse = (hb_lastuse_ts!=null)?Bytes.toLong(hb_lastuse_ts):0;				
  		used_quota = (hb_quota_used!=null)?Bytes.toLong(hb_quota_used):0;
  		isQuotaExceeded = ((hb_quota_exceeded != null) && (Bytes.toLong(hb_quota_exceeded)==1))  ? true : false;
	}

	public long getFreeQuota() {
		return total_quota - used_quota; 
	}
	
}
