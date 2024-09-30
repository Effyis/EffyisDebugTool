package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GvpDictionaryItem {

/* hbase part */
	public String hbase_index_prefix;

	public String keyword;
	public String name_md5;

	public String hbase_index;
	public String  prev_pubBefore;
	
	public long  prev_pubBefore_sec;
	public long  prev_search_timeframe_sec;

	public long scheduled_ts;
	public long lastCheck_ts;
	
//	public boolean is_waiting;
	public long lastRequest_ts;
	
/* hbase part */
	
	public long delay_sec;
	
	
	public GvpDictionaryItem(String hbase_index_prefix, String keyword)
	{
		this.keyword = keyword;
		this.name_md5 = GvpUtils.getMD5(keyword);
		this.hbase_index = hbase_index_prefix + "." + name_md5;
	}
	
/*	public GvpDictionaryItem(	String keyword,	long delay_sec,	long lastcheck_ts, long scheduled_ts )
	{
		this.keyword = keyword;
		this.delay_sec = delay_sec;
		this.scheduled_ts = scheduled_ts;
	}
*/	
	
	public GvpDictionaryItem(Result result) {
		syncFromHBase(result);
	}

		
	public void syncFromHBase(Result result){
    		
		hbase_index = Bytes.toString(result.getRow());

		hbase_index_prefix     = hbase_index.split("\\.")[0];
		name_md5   = hbase_index.split("\\.")[1];
		
//		byte[] hb_created_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"));
		
		byte[] hb_keyword               = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("keyword"));
		byte[] hb_prev_pubBefore        = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_pubBefore"));
		byte[] hb_prev_pubBefore_sec    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_pubBefore_sec"));
		byte[] hb_prev_search_timeframe_sec  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_search_timeframe_sec"));

		byte[] hb_scheduled_ts  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("scheduled_ts"));
		byte[] hb_lastCheck_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"));

		keyword         = (hb_keyword!=null)?Bytes.toString(hb_keyword):null;
		prev_pubBefore  = (hb_prev_pubBefore!=null)?Bytes.toString(hb_prev_pubBefore):null;

		prev_pubBefore_sec = (hb_prev_pubBefore_sec!=null)?Bytes.toLong(hb_prev_pubBefore_sec):0;  //  LONG
		prev_search_timeframe_sec = (hb_prev_search_timeframe_sec!=null)?Bytes.toLong(hb_prev_search_timeframe_sec):0;  //  LONG
		scheduled_ts = (hb_scheduled_ts!=null)?Bytes.toLong(hb_scheduled_ts):0;
		lastCheck_ts = (hb_lastCheck_ts!=null)?Bytes.toLong(hb_lastCheck_ts):0;
		
    }
	
	
}
