package com.socialgist.debugtool.utils.items;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class VkWallRef {

	public String wall_id = null;
	public String hbase_index = null;
	
	public VkWallRef() {
	}
	
	public VkWallRef(Result result) {
		hbase_index = Bytes.toString(result.getRow());
		wall_id = hbase_index.split("\\.")[2];
	}
	
}
