package com.socialgist.gvp.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GvpServer {

	public String hbase_index;
	public String process_uuid;
	
	public String name = "";
	public String token = "";
	
	public GvpServer(String name, String token) {
		this.name = name;
		this.token = token;
	}

	public GvpServer(Result result) {
		hbase_index = Bytes.toString(result.getRow()); 
		process_uuid = hbase_index.split("\\|")[1];
		name = hbase_index.split("\\|")[2];
		byte[] hb_token  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("token"));
		token = (hb_token!=null)?Bytes.toString(hb_token):"";
		
	}	
}
