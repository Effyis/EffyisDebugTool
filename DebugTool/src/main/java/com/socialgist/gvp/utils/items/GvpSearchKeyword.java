package com.socialgist.gvp.utils.items;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class GvpSearchKeyword {

	public long gvp_id;
	public String name;
	public String encodedName = "";
	
	public long new_videos;
	public long total_videos;
	public int  last_start;
	
	public GvpSearchKeyword(String name, long gvp_id)  
						 {
		super();
		this.gvp_id 	= gvp_id;
		this.name 		= name.trim();

		try {
			encodedName = URLEncoder.encode(this.name, "UTF-8");
		} catch (UnsupportedEncodingException e2) {
		}
		
	}
}
