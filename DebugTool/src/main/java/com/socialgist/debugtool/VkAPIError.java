package com.socialgist.debugtool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class VkAPIError {

	public int code;
	public String message = "";

	public boolean skipAlerting = false;
	
	public VkAPIError(JsonNode errorNode) {

		code = errorNode.path("error").path("error_code").asInt(0);
		message = errorNode.path("error").path("error_msg").asText();
		
		if ((code == 15)) { // Access denied
			skipAlerting = true;
		}
		else if ((code == 6)) { // 6 - Too many requests per second
			skipAlerting = true;
		}
		else if ((code == 9)) { // 9 - Flood control
			skipAlerting = true;
		}
		else if ((code == 29)) { // 29 - Rate limit reached
			skipAlerting = true;
		}
		else { 
//	        statsCounter.incrementStatsCount(StatsType.UNKNOWN_PROVIDER_ERROR);
		}
	}
	
	public static boolean isError(JsonNode errorNode) {
		if (errorNode.path("error").isMissingNode()) return false;
		else return true;
	}

	
	// Error:{"error":{"error_code":15,"error_msg":"Access denied: user hid his wall from accessing from outside","request_params":[{"key":"owner_id","value":"1750864"},{"key":"v","value":"5.131extended=1"},{"key":"count","value":"10"},{"key":"filter","value":"all"},{"key":"method","value":"wall.get"},{"key":"oauth","value":"1"},{"key":"domain","value":"1750864"}],"errors":[]}}	
	public boolean isPermissionDenied() {
		if (code == 15)  return true;
		return false; 
	}
	
	public boolean isRateLimitExceeded() {
		if ((code == 6)||(code == 9)||(code == 29)) return true;
		else return false; 
	}

	public static boolean isError(String s) {
		if (s.startsWith("{\"error\"")) 
			return true;
		else 
			return false;
	}	
	
	public static boolean isRetryableError(String s) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode errorNode;
		try {
			errorNode = mapper.readTree(s);
		} catch (JsonProcessingException e) {
			return false;
		}

		int code = errorNode.path("error").path("error_code").asInt(0);
		
		if ((code == 15)) { // Access denied
			return false;
		}
		else if ((code == 6)) { // 6 - Too many requests per second
			return true;
		}
		else if ((code == 9)) { // 9 - Flood control
			return true;
		}
		else if ((code == 29)) { // 29 - Rate limit reached
			return false;
		}
		if ((code == 5)) { // Access denied
			return false;
		}
		return false;
	}
	
	public static boolean isLoggableError(String s) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode errorNode;
		try {
			errorNode = mapper.readTree(s);
		} catch (JsonProcessingException e) {
			return false;
		}
		int code = errorNode.path("error").path("error_code").asInt(0);
		
		if ((code == 15)) { // Access denied
			return false;
		}
		return false;
	}

	
	
	
/*
 VK API provides a list of error codes:

1 - Unknown error occurred
2 - Application is disabled
3 - Unknown method passed
4 - Incorrect signature
5 - User authorization failed
6 - Too many requests per second
7 - Permission to perform this action is denied
8 - Invalid request
9 - Flood control
10 - Internal server error
11 - Application should be disabled
14 - Captcha needed
15 - Access denied
17 - Validation required
18 - User was deleted or banned
20 - Permission to perform this action is denied for non-standalone applications
21 - Permission to perform this action is allowed only for standalone and OpenAPI applications
23 - This method was disabled
27 - Group authorization failed
28 - Application authorization failed
29 - Rate limit reached
30 - This profile is private
100 - One of the parameters specified was missing or invalid
200 - Access to album denied
201 - Access to audio denied
203 - Access to group denied
300 - This album is full
*/	
	
}
