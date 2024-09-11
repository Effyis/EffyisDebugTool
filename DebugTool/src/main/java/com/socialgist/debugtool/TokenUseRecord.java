package com.socialgist.debugtool;

public class TokenUseRecord {

	public String rowKey;

	public String token = "";
	public String proxy;
	public long day_of_use;
	
	public String id = "";
	public String type = "";
	public String apiproject = "";
	public String description = "";
	public long total_quota;
	public int isactive = 0;
	

	public long lastuse;
	public long quota_used;
	public boolean quota_exceeded;

	
	


	public TokenUseRecord() {
	}

}
