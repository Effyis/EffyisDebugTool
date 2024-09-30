package com.socialgist.debugtool.utils.items;

import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;


public class GvpError {
	
	public int code;
//	public int subcode;

	public String message = "";
	public String type = "";
	public String reason = "";
	
    public boolean canRecheck = true;				// return if api call can be run again
    public boolean skipAlerting = false;
	
	
	public GvpError(JsonNode errorNode) {

		code = errorNode.path("error").path("code").asInt(0);
		message = errorNode.path("error").path("message").asText();
			
		JsonNode n = errorNode.path("error").withArray("errors");
		Iterator<JsonNode> itItems = n.elements();
		while (itItems.hasNext()) {
			 JsonNode item = itItems.next();
			 reason = item.path("reason").asText();
		}
	}
	
	public static boolean isError(JsonNode errorNode) {
		if (errorNode.path("error").isMissingNode()) return false;
		return true;
	}

	
	public boolean isDailyLimitExceeded() {
//		{ "error": {  "errors": [   {    "domain": "usageLimits",    "reason": "dailyLimitExceeded",    "message": "Daily Limit Exceeded. The quota will be reset at midnight Pacific Time (PT). You may monitor your quota usage and adjust limits in the API Console: https://console.developers.google.com/apis/api/youtube.googleapis.com/quotas?project=1055093737724",    "extendedHelp": "https://console.developers.google.com/apis/api/youtube.googleapis.com/quotas?project=1055093737724"   }  ],  "code": 403,  "message": "Daily Limit Exceeded. The quota will be reset at midnight Pacific Time (PT). You may monitor your quota usage and adjust limits in the API Console: https://console.developers.google.com/apis/api/youtube.googleapis.com/quotas?project=1055093737724" }}Daily Limit Exceeded. The quota will be reset at midnight Pacific Time (PT). You may monitor your quota usage and adjust limits in the API Console: https://console.developers.google.com/apis/api/youtube.googleapis.com/quotas?project=1055093737724	
		if ((code == 403)&&reason.equalsIgnoreCase("dailyLimitExceeded"))  return true;
		if ((code == 403)&&reason.equalsIgnoreCase("quotaExceeded"))       return true;

		return false;
	}

	
	public boolean isRateLimitExceeded() {
//		{ "error": {  "errors": [   {    "domain": "usageLimits",    "reason": "rateLimitExceeded",    "message": "Rate Limit Exceeded"   }  ],  "code": 429,  "message": "Rate Limit Exceeded" }}
//		Rate Limit Exceeded	
		if ((code == 429)&&reason.equalsIgnoreCase("rateLimitExceeded")) return true;
		else return false; 
	}
	
	public boolean isPostDeleted() {
		return false;
	}
	
	public boolean isChannelDeleted() {
		if (code == 404) return true;
//		{"error":{"errors":[{"domain":"youtube.playlistItem","reason":"playlistNotFound","message":"The playlist identified with the requests <code>playlistId</code> parameter cannot be found.","locationType":"parameter","location":"playlistId"}],"code":404,"message":"The playlist identified with the requests <code>playlistId</code> parameter cannot be found."}}		
		return false;
	}
	
	public void delay100() {
		try {
			Thread.sleep(100);   
		} catch (InterruptedException e) {}
	}
	
	public void delay500() {
		try {
			Thread.sleep(500);   
		} catch (InterruptedException e) {}
	}
}
