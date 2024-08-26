package com.socialgist.debugtool.items;

public class DtTokenItem {

    public enum DtTokenType {
        VK_TOKEN, 
        YOUTUBE_TOKEN, 
        NOT_DEFINED
    }    
    public DtTokenType type;
	public String token = "";
	public String proxy = "";
	
	public DtTokenItem(DtTokenType type, String token, String proxy) {
		super();
		this.type = type;
		this.token = token;
		this.proxy = proxy;
	} 

}
