package com.socialgist.debugtool.utils.items;

public enum StatsType {
	
	// Common stats 
	CONTROLLER_REGUEST(1, "Controller requests"),
	// Network stats
    HTTP_CALLS(101, "Htpp calls"),
    HTTP_200_CALLS(102, "200 Success htpp calls"),
	// Youtube stats
	SENT_VIDEOS(201, ""),
	SENT_COMMENTS(202, ""),
	SENT_STATS(203, ""),
	// Youtube network stats
    API_SEARCH_CALLS(200, "API Search Requests"),
    API_CHANNELS_CALLS(201, "API Channels Requests"),
    API_PLAYLISTITEMS_CALLS(202, "API PlayListItem Requests"),
    API_VIDEO_CALLS(20, "API Video Requests"),
    API_COMMENTTHREADS_CALLS(20, "API CommentThreads Requests"),
    API_COMMENTS_CALLS(20, "API Comments Requests"),
    API_VIDEOCATEGORIES_CALLS(20, "API VideoCategories Requests"),
    
	// Common errors 
    UNKNOWN_ERROR(1000, "Unknown Error"),
    ERROR_HTTP(1001, "HTTP Error"),
    ERROR_BAD_JSON(1002, "Bad Json Error"),
	// Provider errors
    UNKNOWN_PROVIDER_ERROR(1100, "Unknown Provider Error"),
    ERROR_BACKEND(1101, "500/503 Backend Error"),
    ERROR_EMPTY_RESPONSE(1102, "500 Empty Response"),
    ERROR_PROCESSING_FAILURE(1103, "400 Processing Failure"),
    ERROR_RATE_LIMIT_EXCEEDED(1104, "429 Rate Limit Exceeded"),
    ERROR_DAILY_LIMIT_EXCEEDED(1105, "403 Daily Limit Exceeded"),
    ERROR_QUOTA_EXCEEDED(1106, "403 Quota Exceeded");

    private final int id;
    private final String title;

    StatsType(int id, String title) {
        this.id = id;
        this.title = title;
    }

    public int getId() {
        return id;
    }

	public String getTitle() {
		return title;
	}
    
    
}