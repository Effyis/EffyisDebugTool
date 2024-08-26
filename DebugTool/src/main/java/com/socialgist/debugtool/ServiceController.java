package com.socialgist.debugtool;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.socialgist.debugtool.items.DtHtmlTable;
import com.socialgist.debugtool.items.DtHtmlYoutubeApiResult;
import com.socialgist.debugtool.model.Cell;
import com.socialgist.debugtool.model.JsonData;
import com.socialgist.debugtool.model.Row;
import com.socialgist.gvp.utils.HBaseContainer;
import com.socialgist.gvp.utils.StatsContainer;
import com.socialgist.gvp.utils.items.GvpChannel;
import com.socialgist.gvp.utils.items.GvpComment;
import com.socialgist.gvp.utils.items.GvpVideo;
import com.socialgist.gvp.utils.items.PremiumSubscription;
import com.socialgist.gvp.utils.items.StatsTimePeriod;


@RestController
public class ServiceController {

	@Autowired
	HBaseContainer hbaseContainer;
	@Autowired
	HBaseUtilityContainer hbaseUtilityContainer;
	@Autowired
	MySQLUtilsContainer mySQLUtilsContainer;
	@Autowired
	YoutubeContainer youtubeContainer;
	@Autowired
	StatsContainer statsContainer;	
	
    @GetMapping("/bbb")
    public String bbb() {
        return "bbb";
    }	

    @GetMapping("/all_tokens")
    public String tokens(@RequestParam(name = "token", required = false) String token) throws IOException {
//    public String tokens() throws IOException {

    	 StringBuilder htmlTable = new StringBuilder();
         
         htmlTable.append("<table border=1>");
         htmlTable.append("<tr><th>Index</th><th>Last Use</th><th>Ouota Used</th></tr>");
         
         Map<String, String> map = hbaseUtilityContainer.getAllTokens1();
         for (Map.Entry<String, String> entry : map.entrySet()) {
             String key = entry.getKey();
             String data = entry.getValue();
             
             if (!data.contains(".")) continue;
             String hb_token = key.split("\\.")[0];
             if ((token != null) && !token.equalsIgnoreCase(hb_token))  continue;;
            	                 
             String lastuse = data.split("\\.")[0];
             String quotaused = data.split("\\.")[1];
             String quota_exceeded = data.split("\\.")[2];
              
             htmlTable.append("<tr>");
             htmlTable.append("<td>").append(key).append("</td>");
//             htmlTable.append("<td>").append(lastuse).append("</td>");
             htmlTable.append("<td>").append(Instant.ofEpochSecond(Long.parseLong(lastuse)).toString()).append("</td>");
             htmlTable.append("<td>").append(quotaused).append("</td>");
             htmlTable.append("<td>").append(quota_exceeded).append("</td>");
             htmlTable.append("</tr>");
         }
         
         htmlTable.append("</table>");
         return htmlTable.toString();
    }


    @GetMapping("/all_premium")
    public String all_premium10() throws IOException {
    	
    	 StringBuilder htmlTable = new StringBuilder();
         
         htmlTable.append("<table>");
         htmlTable.append("<tr><th>Index</th><th>Last Use</th><th>Ouota Used</th></tr>");
         
         Map<String, PremiumSubscription> map = mySQLUtilsContainer.readRules(10, 0);
         for (Entry<String, PremiumSubscription> entry : map.entrySet()) {
             String key = entry.getKey();
             PremiumSubscription ps = entry.getValue();
             
             htmlTable.append("<tr>");
             htmlTable.append("<td>").append(key).append("</td>");
             htmlTable.append("<td>").append(ps.outputkafkatopic).append("</td>");
             htmlTable.append("<td>").append(ps.last_start_sec).append("</td>");
             htmlTable.append("</tr>");
         }
         
         htmlTable.append("</table>");
         return htmlTable.toString();
    }

    @GetMapping("/api_channel")
 	public String api_channel(@RequestParam(name = "channel_id", required = false ) String channel_id) throws Exception {

    	DtHtmlYoutubeApiResult apiResult = youtubeContainer.readChannelHtml(channel_id);
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Channels API</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Channel:</td>");
 			htmlTable.append("<td><b>" + channel_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("api_channel?channel_id=", "", "Enter channel_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td><a target='_blank' href=\"channel_list?channel_id=" + channel_id + "\">Channel Debug View</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist?channel_id=" + channel_id + "\">Playlists API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist_items?playlist_id=" + apiResult.playlist_id + "\">PlaylistItems API</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }
    
    @GetMapping("/api_playlist")
 	public String api_playlist(@RequestParam(name = "channel_id", required = false ) String channel_id) throws Exception {
    	DtHtmlYoutubeApiResult apiResult = youtubeContainer.readPlaylistHtml(channel_id);
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Playlists API</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Channel:</td>");
 			htmlTable.append("<td><b>" + channel_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("api_playlist?channel_id=", "", "Enter channel_id", "Search") + "</td>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td width=\"10%\"></td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_channel?channel_id=" + channel_id + "\">Channel API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"channel_list?channel_id=" + channel_id + "\">Channel Debug View</a></td>");
 		}
 		htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }

    @GetMapping("/api_playlist_items")
 	public String api_playlist_items(@RequestParam(name = "playlist_id", required = false ) String playlist_id) throws Exception {

    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube PlaylistItems API</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
   	 	htmlTable.append("<td>Playlist:</td>");
   	 	htmlTable.append("<td><b>" + playlist_id + "</b></td>");
   	 	htmlTable.append("<td width=\"20%\"></td>");
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("api_playlist_items?playlist_id=", "", "Enter playlist_id", "Search") + "</td>");
   	 	
   	 	htmlTable.append("</tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append(youtubeContainer.readPlaylistItemsHtml(playlist_id));
    	return htmlTable.toString();
    }
    
    @GetMapping("/api_video")
 	public String video(@RequestParam(name = "video_id", required = false ) String video_id) throws Exception {
    	
    	DtHtmlYoutubeApiResult apiResult = youtubeContainer.readVideoHtml(video_id);
    	
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Videos API</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0 ><tr>");
   	 	htmlTable.append("<td>Video_id:</td>");
   	 	htmlTable.append("<td><b>" + video_id + "</b></td>");
   	 	htmlTable.append("<td width=\"20%\"></td>");
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("api_video?video_id=", "", "Enter video_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"video_list?video_id=" + video_id + "\">Video Debug View</a></td>");
   	 	htmlTable.append("<td align=center width=\"20\">|</td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"https://www.youtube.com/watch?v=" + video_id + "\">Video URL</a></td>");
 		if ((apiResult.channel_id!=null)) {
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_channel?channel_id=" + apiResult.channel_id + "\">Channel API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"channel_list?channel_id=" + apiResult.channel_id + "\">Channel Debug View</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append(apiResult.htmlText);
    	return htmlTable.toString();
    }

    @GetMapping("/rules_list")
 	public String rules_list(@RequestParam(name = "rule_id") int rule_id, @RequestParam(name = "limit") int limit) throws Exception {
    	
    	int prevPage = rule_id - limit;
    	if (prevPage < 0) prevPage=0;
    	int nextPage = rule_id + limit;
    	
    	DtHtmlTable subscrptionTable = mySQLUtilsContainer.get_subscriptions("id >="+rule_id, 0, limit, 1, "asc");   	 	
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Videos</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
    	if (rule_id > 0) { 
    		htmlTable.append("<td>&nbsp;<a href=\"rules_list?rule_id=" + prevPage + "&limit="+limit+ "\">" + " << Prev Page " + "</a></td>");
    	}
    	else {
    		htmlTable.append("<td>&nbsp; << Prev Page </td>");
    	}
	 		htmlTable.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>");
    	if (subscrptionTable.rowCount >= limit) 
   	 		htmlTable.append("<td>&nbsp;<a href=\"rules_list?rule_id=" + nextPage + "&limit="+limit+"\">" + " Next Page >>  " + "</a></td>");
   	 	htmlTable.append("<td width=\"20%\"></td>");
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("rules_list?rule_id=", "&limit="+limit, "Enter rule id", "Search") + "</td>");
   	 	htmlTable.append("<td>&nbsp;&nbsp;&nbsp;Limit " + limit + " records.</td>");
   	 	htmlTable.append("</tr></table>");
   	 	htmlTable.append("<br>");
   	 	htmlTable.append(subscrptionTable.htmlTable);
   	 	return htmlTable.toString();
 	}
    
    @GetMapping("/rules_names")
 	public String rules_names(@RequestParam(name = "rule_name") String rule_name, @RequestParam(name = "offset") int offset, @RequestParam(name = "limit") int limit) throws Exception {

    	int prevOffset = offset - limit;
    	if (prevOffset < 0) prevOffset=0;
    	int nextOffset = offset + limit;
    	DtHtmlTable subscrptionTable = mySQLUtilsContainer.get_subscriptions("rule_name like '%"+rule_name+"%'", offset, limit, 3, "asc");   	 	
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Videos</h2></td></tr></table>");
   	 	htmlTable.append("<p>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
   	 	htmlTable.append("<td>Filtered by rule_name:</td>");
   	 	htmlTable.append("<td><b>" + rule_name + "</b></td>");
   	 	htmlTable.append("<td width=\"20%\"></td>");
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("rules_names?rule_name=", "&offset=0&limit="+limit, "Enter rule name", "Search") + "</td>");
   	 	htmlTable.append("<td>&nbsp;&nbsp;&nbsp;Limit " + limit + " records.</td>");
   	 	htmlTable.append("</tr></table>");
   	 	htmlTable.append("<br>");
    	if (offset > 0) 
    		htmlTable.append("&nbsp;&nbsp;<a href=\"rules_names?rule_name=" + rule_name + "&offset="+prevOffset+"&limit="+limit+ "\">" + " << Prev Page " + "</a> &nbsp;&nbsp;");
    	if (subscrptionTable.rowCount >= limit) {
   	 		htmlTable.append("&nbsp;<a href=\"rules_names?rule_name=" + rule_name + "&offset="+nextOffset+"&limit="+limit+"\">" + " Next Page >>  " + "</a>");
   	 		htmlTable.append("<br>");
   	   	 	htmlTable.append("<br>");
    	}
   	 	htmlTable.append(subscrptionTable.htmlTable);
   	 	return htmlTable.toString();
    } 	


    @GetMapping("/rules_created")
 	public String rules_created(@RequestParam(name = "sort") String sort, @RequestParam(name = "offset") int offset, @RequestParam(name = "limit") int limit) throws Exception {

    	int prevOffset = offset - limit;
    	if (prevOffset < 0) prevOffset=0;
    	int nextOffset = offset + limit;
    	DtHtmlTable subscrptionTable = mySQLUtilsContainer.get_subscriptions("id>0", offset, limit, 11, sort);   	 	
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Videos</h2></td></tr></table>");
   	 	htmlTable.append("<p>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
    	if (offset > 0) { 
    		htmlTable.append("<td>&nbsp;<a href=\"rules_created?sort=" + sort + "&offset="+prevOffset+"&limit="+limit+ "\">" + " << Prev Page " + "</a></td>");
    		htmlTable.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;</td>");
    	}
    	if (subscrptionTable.rowCount >= limit) 
   	 		htmlTable.append("<td>&nbsp;<a href=\"rules_created?sort=" + sort + "&offset="+nextOffset+"&limit="+limit+"\">" + " Next Page >>  " + "</a></td>");
   	 	htmlTable.append("</tr></table>");
   	 	htmlTable.append("<p>");
   	 	htmlTable.append(subscrptionTable.htmlTable);
   	 	return htmlTable.toString();
    } 	
    
    
    @GetMapping("/channel_list")
// 	public String channel(@RequestParam(name = "channel_id") String channel_id, Model model
 	public String channel_list(@RequestParam(name = "channel_id") String channel_id) throws Exception {

    	//qCyufu4Ep0Q
    	GvpChannel ghc=null;
 		if ((channel_id!=null) && (!channel_id.isBlank()))	{
 			ghc = hbaseContainer.checkChannelForExistence(channel_id);
 		}
 		
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Channel Debug View</h2></td></tr></table>");
   	 	
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((ghc!=null)) {
 			htmlTable.append("<td>channel_id:</td>");
 			htmlTable.append("<td><b>" + channel_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
 		else {
 	 		if ((channel_id!=null) && (!channel_id.isBlank()))	{
 	 			htmlTable.append("<td>Channel with channel_id of '"+channel_id+"' not found in HBase.</td>");
 	 			htmlTable.append("<td width=\"20%\"></td>");
 	 		}
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("channel_list?channel_id=", "", "Enter channel_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
 		if ((ghc!=null)) {
 	   	 	htmlTable.append("<td><a target='_blank' href=\"api_channel?channel_id=" + channel_id + "\">Channel API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist?channel_id=" + channel_id + "\">Playlists API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist_items?playlist_id=" + ghc.playlist_id + "\">PlaylistItems API</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");   	 	
   	 	htmlTable.append("<hr>");

 		if ((ghc==null)) return htmlTable.toString();
 		
   	 	htmlTable.append("<b>Subscription rules:</b>");
   	 	htmlTable.append("<p>");
   	 	
   	 	String where = "youtube_id like '" + ghc.channel_id + "'";
   	 	DtHtmlTable subscrptionTable = mySQLUtilsContainer.get_subscriptions(where, 0, 10, 1, "asc");   	 	
   	 	htmlTable.append(subscrptionTable.htmlTable);
   	 	htmlTable.append("<p>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<b>Video list (limit 90 days or 200 latest videos):</b>");
   	 	htmlTable.append("<p>");
   	 	
    	TreeMap<Long, GvpVideo> videos = youtubeContainer.readVideosForChannel(ghc.playlist_id);
   	 	
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>Video Id</th><th>URL</th><th>Created</th><th>Last Check</th><th>Comments</th><th>Comments Sent</th><th>lastComment_ts</th></tr>");
     
   	 	for (Entry<Long, GvpVideo> entry : videos.entrySet()) {
   	 		Long video_id = entry.getKey();
   	 		GvpVideo v1 = entry.getValue();
   	 		GvpVideo v2 = hbaseContainer.checkVideoForExistence(v1.video_id);
   	 		
   	 		htmlTable.append("<tr>");
   	 	    String video_href = String.format("<a target='_blank' href='/video_list?video_id=%s'>%s</a>", v1.video_id, v1.video_id);
   	 		htmlTable.append("<td>").append(video_href).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://www.youtube.com/watch?v=%s'>URL</a>", v1.video_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		
   	 		
   	 		htmlTable.append("<td>").append(Instant.ofEpochSecond(v1.created_ts)).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v2==null)?"---" : Instant.ofEpochSecond(v2.lastCheck_ts)).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v2==null)?"---" : v2.commentsCount).append("</td>");
   	 		htmlTable.append("<td  align='right'>").append((v2==null)?"---" : v2.commentsSent).append("</td>");
   	 		htmlTable.append("<td  align='right'>").append((v2==null)?"---" : Instant.ofEpochSecond(v2.lastComment_ts)).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
     
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }
    
    @GetMapping("/video_list")
// 	public String channel(@RequestParam(name = "channel_id") String channel_id, Model model
 	public String video_list(@RequestParam(name = "video_id") String video_id) throws Exception {

   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Video Debug View</h2></td></tr></table>");
   	 	htmlTable.append("<p>");
   	 	
    	//qCyufu4Ep0Q
 		GvpVideo ghp = null;
 		if ((video_id!=null) && (!video_id.isBlank()))	{
 			ghp = hbaseContainer.checkVideoForExistence(video_id);
 		}

   	 	htmlTable.append("<table border=0><tr>");
 		if ((ghp!=null)) {
 			htmlTable.append("<td>video_id:</td>");
 			htmlTable.append("<td><b>" + video_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
 		else {
 	 		if ((video_id!=null) && (!video_id.isBlank()))	{
 	 			htmlTable.append("<td>Video with video_id of '"+video_id+"' not found in HBase.</td>");
 	 			htmlTable.append("<td width=\"20%\"></td>");
 	 		}
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildRedirectionForm("video_list?video_id=", "", "Enter video_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"api_video?video_id=" + video_id + "\">Video API</a></td>");
 		if ((ghp!=null)) {
			htmlTable.append("<td align=center width=\"20\">|</td>");
 	   	 	htmlTable.append("<td><a target='_blank' href=\"api_channel?channel_id=" + ghp.channel_id + "\">Channel API</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");   	 	
 		
 		if (ghp==null) {
 			return htmlTable.toString();
 		}
 		
 		
 		if ((ghp!=null)) {
 	   	 	htmlTable.append("<table border=0>");
 	   	 	htmlTable.append("<tr><td>channel:</td><td>" + ghp.channel_id + "</td></tr>");
 	   	 	htmlTable.append("<tr><td>last check:</td><td>" + Instant.ofEpochSecond(ghp.lastCheck_ts) + "</td></tr>");
 	   	 	htmlTable.append("<tr><td>comments sent:</td><td>" + ghp.commentsSent + "</td></tr>");
 	   	 	htmlTable.append("<tr><td>last comment:</td><td>" + Instant.ofEpochSecond(ghp.lastComment_ts) + "</td></tr></table>");
 		}
 		
 		
   	 	htmlTable.append("<hr>");
   	 	
   	 	
 		TreeMap<Long, GvpComment> comments = youtubeContainer.readCommentsForVideo(ghp);
   	 	
   	 	htmlTable.append("<b>200 latest comments:</b>");
   	 	htmlTable.append("<p>");
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>Comment Id</th><th>URL</th><th>author</th><th>published</th><th alignr=center>collected</th></tr>");
     
   	 	for (Entry<Long, GvpComment> entry : comments.entrySet()) {
   	 		long id = entry.getKey();
   	 		GvpComment comment = entry.getValue();
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(comment.comment_id).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://www.youtube.com/watch?v=%s&lc=%s'>URL</a>", ghp.video_id, comment.comment_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		htmlTable.append("<td>").append(comment.authorDisplayName).append("</td>");
   	 		htmlTable.append("<td>").append(comment.publishedAt).append("</td>");
//   	 		htmlTable.append("<td>&nbsp;&nbsp;").append(GvpComment.epochSecondsToLocalDateTimeUTC(comment.created_ts)).append("</td>");
   	 		if (comment.collected_ts == 0) 
   	 			htmlTable.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;").append("---").append("</td>");
   	 		else
   	 			htmlTable.append("<td>&nbsp;&nbsp;").append(GvpComment.epochSecondsToLocalDateTimeUTC(comment.collected_ts)).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
     
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }
    
    @GetMapping("/chart123")
 	public Map<String, Long> chart123(@RequestParam(name = "stats_id") String stats_id) throws Exception {

		Map<String, Long> retrievedHashMap = new HashMap<>();
 		
	    	Result result = hbaseContainer.hBase_getStats(stats_id);
	 		if (result==null) return null;
			if ((result != null) && !result.isEmpty()) { // SUBSCRIPTION FOUND IN HBASE
				// Extract values from the Result instance and populate the HashMap
				for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes("stats_5m")).keySet()) {
					byte[] valueBytes = result.getValue(Bytes.toBytes("stats_5m"), qualifier);
					Long value = Bytes.toLong(valueBytes);
					String qualifierStr = Bytes.toString(qualifier);
					retrievedHashMap.put(qualifierStr, value);
				}
			}
			return retrievedHashMap;
//			scheduled_ts = (hb_scheduled_ts != null) ? Bytes.toLong(hb_scheduled_ts) : 0;
    }
    
    @GetMapping("/stats_list")
// 	public String channel(@RequestParam(name = "channel_id") String channel_id, Model model
 	public String stats_list(@RequestParam(name = "id") String hbase_index) throws Exception {

    	
    	TreeMap<String, String> map = statsContainer.hBase_getStats(hbase_index);
 		if (map==null) return null;

 		StringBuilder htmlTable = new StringBuilder();
     
   	 	htmlTable.append("<table>");
   	 	htmlTable.append("<tr><th>key</th><th>value</th></tr>");
     
   	 	for (Entry<String, String> entry : map.entrySet()) {
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(entry.getKey()).append("</td>");
   	 		htmlTable.append("<td>").append(entry.getValue()).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }
    

    
    @GetMapping("/threads_stats")
    public String threads_stats(@RequestParam(name = "prefix", required = true) String prefix, @RequestParam(name = "stats_ids", required = true) String stats_ids) throws IOException {

    	
    	// Convert the comma-separated string to a list
        List<String> statsList = Arrays.asList(stats_ids.split(","));    	
    	
    	Map<Integer, Map<String, String>> map = hbaseUtilityContainer.getThreadsStats(prefix);
    	
        StringBuilder htmlTable = new StringBuilder();

        // Open the table tag
        htmlTable.append("<table border='1'>");
        
        
        // Open the table header row
        htmlTable.append("<tr>");

        // Add an empty cell for the row key in the header
        htmlTable.append("<th>Thread Id</th>");

        // Iterate over the first row's inner map entries (column keys)
        Map<String, String> firstRowInnerMap = map.isEmpty() ? new HashMap<>() : map.values().iterator().next();
        for (String columnKey : firstRowInnerMap.keySet()) {
            if (!statsList.contains(columnKey)) continue;
            // Add a th cell for each column key
            htmlTable.append("<th>").append(columnKey).append("</th>");
        }

        // Close the table header row
        htmlTable.append("</tr>");
        

        // Iterate over outer map entries (rows)
        for (Entry<Integer, Map<String, String>> rowEntry : map.entrySet()) {
            Integer rowKey = rowEntry.getKey();
            Map<String, String> innerMap = rowEntry.getValue();

            // Open a new table row
            htmlTable.append("<tr>");

            // Add the row key as the first cell in the row
            htmlTable.append("<td>").append(rowKey).append("</td>");

            // Iterate over inner map entries (columns)
            for (Map.Entry<String, String> columnEntry : innerMap.entrySet()) {
                String columnKey = columnEntry.getKey();
                
                if (!statsList.contains(columnKey)) continue;
                
                String columnValue = columnEntry.getValue();

                // Add a cell for each column
                htmlTable.append("<td>").append(columnValue).append("</td>");
            }

            // Close the table row
            htmlTable.append("</tr>");
        }

        // Close the table tag
        htmlTable.append("</table>");

        return htmlTable.toString();    	
    }
    

    @GetMapping("/chart")
    public String chart(@RequestParam(name = "stats_ids") String stats_ids, @RequestParam(name = "period") String period) throws IOException {

    	StatsTimePeriod stp = StatsTimePeriod.valueOf(period);
    	
        JsonData jsonData = new JsonData();
//        jsonData.buildTimeCollection(StatsTimePeriod.LAST_HOUR);
        jsonData.buildTimeCollection(stp);

        
        String[] charts = stats_ids.split(",");
        
        for (String s : charts) {
//          Map<String, Long> map = hbaseUtilityContainer.getStatsMap(s, StatsTimePeriod.LAST_HOUR);
          Map<String, Long> map = hbaseUtilityContainer.getStatsMap(s, stp);
          jsonData.addMap(s, map);
          System.out.println(s);
        }
        
        
//        Map<String, Long> map1 = hbaseUtilityContainer.getStatsMap("test111", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test111", map1);
//        Map<String, Long> map2 = hbaseUtilityContainer.getStatsMap("test222", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test222", map2);
//        Map<String, Long> map3 = hbaseUtilityContainer.getStatsMap("test333", StatsTimePeriod.LAST_HOUR);
//        jsonData.addMap("test333", map3);
        
        
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonMessage ="{}";
        try {
            jsonMessage = objectMapper.writeValueAsString(jsonData);
            System.out.println(jsonMessage);
//            model.addAttribute("message", jsonMessage);        
//            model.addAttribute("title", StatsTimePeriod.LAST_HOUR.getTitle());        
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }    	
//        model.addAttribute("message", message);
        
        return jsonMessage;
    }
    
    @GetMapping("/day_tokens1")
    public String day_tokens1(@RequestParam(name = "type") String type) throws IOException {

        JsonData jsonData = new JsonData();
        jsonData.buildCollections();
    	
    	Map<String, TokenUseRecord> tokens = hbaseUtilityContainer.getAllTokens();
    	SqlRowSet result = mySQLUtilsContainer.db_tokens(type);

//	   	 StringBuilder htmlTable = new StringBuilder();
	   	 
//	   	 jsonData.addColumn("id", "string");
	   	 jsonData.addColumn("id", "number");
//	   	 jsonData.addColumn("day", "string");    
	   	 jsonData.addColumn("token(check)", "string");         
	   	 jsonData.addColumn("proxy(check)", "string");
	   	 jsonData.addColumn("apiproject", "string");    
	   	 jsonData.addColumn("description", "string");   
	   	 jsonData.addColumn("quota used", "string");    
	   	 jsonData.addColumn("total quota", "string");   
	   	 jsonData.addColumn("lastuse", "string");       
	   	 jsonData.addColumn("is quota exceeded", "string");
    	
	    while (result.next()) {

	    	String token = result.getString("clientkey");
	    	String token_type = result.getString("type");
	    	
	    	 for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
	    		 TokenUseRecord tr = entry.getValue();
	    		 if (token.equalsIgnoreCase(tr.token)) {
    		    	 tr.id = result.getString("id");
	    			 tr.total_quota = result.getInt("quota");
	    			 tr.apiproject = result.getString("apiproject");
	    			 tr.description = result.getString("description");
	    			 tr.usagepattern = result.getString("usagepattern");
	    			 tr.type = result.getString("type");
	    		 }
	    	 }
			}
	    
	    long i = 0;
	    for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
   		 	TokenUseRecord tr = entry.getValue();

   		 	if (!tr.type.equalsIgnoreCase(type)) continue; 
   		 	
            Row row = jsonData.addRow(0L, tr.id);
//            row.getC().add(new Cell(tr.day_of_use+""));

    		String[] proxy_parts = tr.usagepattern.split(":");
    		String proxy_ip = proxy_parts[0];
    		int proxy_port = Integer.parseInt(proxy_parts[1]);
            
            String tokenLink = "<a href=\"check_proxy_token?token=" + tr.token + "&proxyHost=" +  proxy_ip + "&proxyPort=" + proxy_port + "\" target=\"_blank\">(x)</a>";
            row.getC().add(new Cell(tr.token+tokenLink));
            
            String proxyLink = "<a href=\"check_proxy?proxyHost=" +  proxy_ip + "&proxyPort=" + proxy_port + "\" target=\"_blank\">(x)</a>";
            
            row.getC().add(new Cell(tr.usagepattern+proxyLink));
            
//            row.getC().add(new Cell(test_call(tr.token, tr.usagepattern)));
            
            row.getC().add(new Cell(tr.apiproject+""));
            row.getC().add(new Cell(tr.description+""));
            row.getC().add(new Cell(tr.quota_used+""));
            row.getC().add(new Cell(tr.total_quota+""));

            Instant instant = Instant.ofEpochSecond(tr.lastuse);
   	        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("America/New_York"));
   	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss");
            
            String formattedDate = zonedDateTime.format(formatter);
            
            row.getC().add(new Cell(formattedDate));
            row.getC().add(new Cell(tr.quota_exceeded+""));
	    }

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonMessage ="{}";
        try {
            jsonMessage = objectMapper.writeValueAsString(jsonData);
            System.out.println(jsonMessage);
//            model.addAttribute("message", jsonMessage);        
//            model.addAttribute("title", StatsTimePeriod.LAST_HOUR.getTitle());        
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }    	
//        model.addAttribute("message", message);
        
        return jsonMessage;
	    
	    
	    
	    
    }

//    @GetMapping("/token")
//    public String token(@RequestParam(name = "token") String token) throws IOException {
//    	return mySQLUtilsContainer.token(token);
//    }
    
    
    @GetMapping("/day_tokens")
    public String day_tokens(@RequestParam(name = "type") String type) throws IOException {

    	Map<String, TokenUseRecord> tokens = hbaseUtilityContainer.getAllTokens();
    	SqlRowSet result = mySQLUtilsContainer.db_tokens(type);

	    while (result.next()) {

	    	String token = result.getString("clientkey");
	    	String token_type = result.getString("type");
	    	
	    	 for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
	    		 TokenUseRecord tr = entry.getValue();
	    		 if (token.equalsIgnoreCase(tr.token)) {
    		    	 tr.id = result.getString("id");
	    			 tr.total_quota = result.getInt("quota");
	    			 tr.apiproject = result.getString("apiproject");
	    			 tr.description = result.getString("description");
	    		 }
	    	 }
			}


	    
	   	 StringBuilder htmlTable = new StringBuilder();
	   	 DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");	   	 
	     
	     htmlTable.append("<table border=1>");
	     htmlTable.append("<tr><th>db_id</th>");
	     htmlTable.append("<th>day_of_use</th>");
	     htmlTable.append("<th>day_of_use</th>");
	     htmlTable.append("<th>token</th>");
	     htmlTable.append("<th>apiproject</th>");
	     htmlTable.append("<th>description</th>");
	     htmlTable.append("<th>Ouota Used</th>");
	     htmlTable.append("<th>total_quota</th>");
	     htmlTable.append("<th>lastuse</th>");
	     htmlTable.append("<th>quota_exceeded</th>");
	     htmlTable.append("</tr>");
	    
	    for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
   		 	TokenUseRecord tr = entry.getValue();

   		    Instant instant = Instant.ofEpochSecond(tr.day_of_use);
            LocalDate localDate = instant.atZone(ZoneId.of("America/Los_Angeles")).toLocalDate();   		 	
            String formattedDate = localDate.format(formatter);	
   		 	
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(tr.id).append("</td>");
   	 		htmlTable.append("<td>").append(tr.day_of_use).append("</td>");
   	 		htmlTable.append("<td>").append(formattedDate).append("</td>");
   	 		htmlTable.append("<td>").append(tr.token).append("</td>");
   	 		htmlTable.append("<td>").append(tr.apiproject).append("</td>");
   	 		htmlTable.append("<td>").append(tr.description).append("</td>");
   	 		htmlTable.append("<td>").append(tr.quota_used).append("</td>");
   	 		htmlTable.append("<td>").append(tr.total_quota).append("</td>");
   	 		htmlTable.append("<td>").append(Instant.ofEpochSecond(tr.lastuse)).append("</td>");
   	 		htmlTable.append("<td>").append(tr.quota_exceeded).append("</td>");
   	 		htmlTable.append("</tr>");
   	 }
	    
	    htmlTable.append("</table>");
	    return htmlTable.toString();
    }

//    @GetMapping("/token")
//    public String token(@RequestParam(name = "token") String token) throws IOException {
//    	return mySQLUtilsContainer.token(token);
//    }

    @GetMapping("/most_quota_used")
    public String most_quota_used(@RequestParam(name = "limit") String limit) throws IOException {
    	return mySQLUtilsContainer.most_quota_used(limit);
    }

    @GetMapping("/rules_started")
    public String rules_started() throws IOException {
    	return mySQLUtilsContainer.rules_started();
    	
    }

    @GetMapping("/dbviews")
    public String dbviews(@RequestParam(name = "view") String view, @RequestParam(name = "params") String params) throws IOException {
    	
    	if (view.equalsIgnoreCase("MQU")) 
    		return mySQLUtilsContainer.most_quota_used(params);
    	if (view.equalsIgnoreCase("RS")) 
    		return mySQLUtilsContainer.rules_started();
    	if (view.equalsIgnoreCase("PS")) 
    		return mySQLUtilsContainer.premium_status();
    	
		return null;
    }

    @GetMapping("/hbaseviews")
    public String hbaseviews(@RequestParam(name = "view") String view, @RequestParam(name = "params") String params) throws IOException {
    	
    	if (view.equalsIgnoreCase("CH_LIST")) 
    		return channelsViewJson(params);
    	if (view.equalsIgnoreCase("V_LIST")) 
    		return videosViewJson(params);
//    	if (view.equalsIgnoreCase("PS")) 
//    		return mySQLUtilsContainer.premium_status();
    	
		return null;
    }
    
    
    @GetMapping("/check_proxy")
    public String check_proxy(@RequestParam(name = "proxyHost", required = false) String proxyHost, @RequestParam(name = "proxyPort", required = false, defaultValue = "0") int proxyPort) throws IOException {
    	
    	 if ((proxyHost == null) || (proxyPort == 0)) {
             return "provide proxy IP and port";
         }    	
    	
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)));
        // Create a RestTemplate with the configured requestFactory
        RestTemplate restTemplate = new RestTemplate(requestFactory);
        String url = "http://api.ipify.org";

        String response = "Proxy used:<br><br>" + proxyHost + ":" + proxyPort;
        response = response + "<br><br>" + "api.ipify.org response: <hr>";
        
        try {
           response = response + "<br>" + restTemplate.getForObject(url, String.class);
        } catch (RestClientResponseException e) {  // (HttpClientErrorException | HttpServerErrorException | UnknownHttpStatusCodeException e)
           response = response + "<br>" + "Error Code:" + e.getStatusCode();
           response = response + "<br>" + e.getResponseBodyAsString();
        } catch (Exception e) {
        	response = response + "<br>" + e.getMessage();
        }
        System.out.println(response);
		return response;
    }

    @GetMapping("/check_proxy_token")
    public String check_proxy(@RequestParam(name = "token", required = false) String token,
    						  @RequestParam(name = "proxyHost", required = false) String proxyHost,
    						  @RequestParam(name = "proxyPort", required = false, defaultValue = "0") int proxyPort) throws IOException {
    	
   	 if ((token == null) || (proxyHost == null) || (proxyPort == 0)) {
         return "provide token, proxy IP and port";
     }
   	 
     SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
     requestFactory.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)));
     // Create a RestTemplate with the configured requestFactory
     RestTemplate restTemplate = new RestTemplate(requestFactory);

     String url = "http://api.ipify.org";

     String response = "Proxy used:<br><br>" + proxyHost + ":" + proxyPort + "<br><hr>";
     response = response + "" + "api.ipify.org response:<br>";
     
     try {
        response = response + "<br>" + restTemplate.getForObject(url, String.class);
     } catch (RestClientResponseException e) {  // (HttpClientErrorException | HttpServerErrorException | UnknownHttpStatusCodeException e)
        response = response + "<br>" + "Error Code:" + e.getStatusCode();
        response = response + "<br>" + e.getResponseBodyAsString();
     } catch (Exception e) {
     	response = response + "<br>" + e.getMessage();
     }
     
     response = response + "<hr>";
     url = "https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&id=rTawvzH0MQ4&key=" + token;
     response = response + "" + "youtube.googleapis.com response:<br>";
     try {
        response = response + "<br>" + restTemplate.getForObject(url, String.class);
     } catch (RestClientResponseException e) {  // (HttpClientErrorException | HttpServerErrorException | UnknownHttpStatusCodeException e)
        response = response + "<br>" + "Error Code:" + e.getStatusCode();
        response = response + "<br>" + e.getResponseBodyAsString();
     } catch (Exception e) {
     	response = response + "<br>" + e.getMessage();
     }
     
     System.out.println(response);
     return convertJsonToHtml(response);
     
    }

    
    public String connectWithoutProxy(String url) throws IOException {
    	
    String response = "";    	
     // Create a RestTemplate with the configured requestFactory
     RestTemplate restTemplate = new RestTemplate();
     try {
        response = restTemplate.getForObject(url, String.class);
     } catch (RestClientResponseException e) {  // (HttpClientErrorException | HttpServerErrorException | UnknownHttpStatusCodeException e)
        response = response + "<br>" + "Error Code:" + e.getStatusCode();
        response = response + "<br>" + e.getResponseBodyAsString();
     } catch (Exception e) {
     	response = response + "<br>" + e.getMessage();
     }
     System.out.println(response);
     return convertJsonToHtml(response);
    }
    
    public String convertJsonToHtml(String jsonString) {
        if (jsonString == null) {
            return null;
        }
        // Replace newline characters with <br> tags
        String htmlString = jsonString.replace("\n", "<br>");
        // Optionally, replace spaces with non-breaking spaces
        htmlString = htmlString.replace(" ", "&nbsp;");
        return htmlString;
    }     
    
    @GetMapping("/test_tokens")
    public String test_tokens(@RequestParam(name = "type") String type) throws IOException {

        JsonData jsonData = new JsonData();
        jsonData.buildCollections();
    	
    	Map<String, TokenUseRecord> tokens = hbaseUtilityContainer.getAllTokens();
    	SqlRowSet result = mySQLUtilsContainer.db_tokens(type);

//	   	 StringBuilder htmlTable = new StringBuilder();
	   	 
//	   	 jsonData.addColumn("id", "string");
	   	 jsonData.addColumn("id", "number");
//	   	 jsonData.addColumn("day", "string");    
	   	 jsonData.addColumn("token(check)", "string");         
	   	 jsonData.addColumn("proxy(check)", "string");         
	   	 jsonData.addColumn("apiproject", "string");    
	   	 jsonData.addColumn("description", "string");   
	   	 jsonData.addColumn("quota used", "string");    
	   	 jsonData.addColumn("total quota", "string");   
	   	 jsonData.addColumn("lastuse", "string");       
	   	 jsonData.addColumn("is quota exceeded", "string");
    	
	    while (result.next()) {

	    	String token = result.getString("clientkey");
	    	String token_type = result.getString("type");
	    	
	    	 for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
	    		 TokenUseRecord tr = entry.getValue();
	    		 if (token.equalsIgnoreCase(tr.token)) {
    		    	 tr.id = result.getString("id");
	    			 tr.total_quota = result.getInt("quota");
	    			 tr.apiproject = result.getString("apiproject");
	    			 tr.description = result.getString("description");
	    			 tr.usagepattern = result.getString("usagepattern");
	    			 tr.type = result.getString("type");
	    		 }
	    	 }
			}
	    
	    long i = 0;
	    for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
   		 	TokenUseRecord tr = entry.getValue();

   		 	if (!tr.type.equalsIgnoreCase(type)) continue; 
   		 	
            Row row = jsonData.addRow(0L, tr.id);
//            row.getC().add(new Cell(tr.day_of_use+""));

    		String[] proxy_parts = tr.usagepattern.split(":");
    		String proxy_ip = proxy_parts[0];
    		int proxy_port = Integer.parseInt(proxy_parts[1]);
            
            String tokenLink = "<a href=\"check_proxy_token?token=" + tr.token + "&proxyHost=" +  proxy_ip + "&proxyPort=" + proxy_port + "\" target=\"_blank\">(x)</a>";
            row.getC().add(new Cell(tr.token+tokenLink));
            
            String proxyLink = "<a href=\"check_proxy?proxyHost=" +  proxy_ip + "&proxyPort=" + proxy_port + "\" target=\"_blank\">(x)</a>";
            
            row.getC().add(new Cell(tr.usagepattern+proxyLink));
            row.getC().add(new Cell(tr.apiproject+""));
            row.getC().add(new Cell(tr.description+""));
            row.getC().add(new Cell(tr.quota_used+""));
            row.getC().add(new Cell(tr.total_quota+""));

            Instant instant = Instant.ofEpochSecond(tr.lastuse);
   	        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("America/New_York"));
   	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss");
            
            String formattedDate = zonedDateTime.format(formatter);
            
            row.getC().add(new Cell(formattedDate));
            row.getC().add(new Cell(tr.quota_exceeded+""));
	    }

        ObjectMapper objectMapper = new ObjectMapper();
        String jsonMessage ="{}";
        try {
            jsonMessage = objectMapper.writeValueAsString(jsonData);
            System.out.println(jsonMessage);
//            model.addAttribute("message", jsonMessage);        
//            model.addAttribute("title", StatsTimePeriod.LAST_HOUR.getTitle());        
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }    	
//        model.addAttribute("message", message);
        
        return jsonMessage;
	    
    }
    
    
    public String test_call(String token, String proxy) throws IOException {

		String[] proxy_parts = proxy.split(":");
		String proxy_ip = proxy_parts[0];
		int proxy_port = Integer.parseInt(proxy_parts[1]);
    	
    	SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    	requestFactory.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxy_ip, proxy_port)));
    	// Create a RestTemplate with the configured requestFactory
    	RestTemplate restTemplate = new RestTemplate(requestFactory);
    	// Use RestTemplate to make requests
    	// For example:
    	String url = "https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&id=rTawvzH0MQ4&key=" + token;

    	String result = "ERROR";
    	try {
    		String response = restTemplate.getForObject(url, String.class);
    		if (response.contains("contentDetails")) result = "OK";       
    	} catch (RestClientResponseException e) {  // (HttpClientErrorException | HttpServerErrorException | UnknownHttpStatusCodeException e)
    		//    		response = response + "<br>" + "Error Code:" + e.getStatusCode();
    		//    		response = response + "<br>" + e.getResponseBodyAsString();
    	} catch (Exception e) {
    		//    		response = response + "<br>" + e.getMessage();
    	}
    	return result;
    }

    @GetMapping("/channelsView")
    public String channelsView(@RequestParam(name = "start") String start) throws IOException {

    	Map<String, GvpChannel> channels = hbaseUtilityContainer.getChannels(start);    	
     	ObjectMapper objectMapper = new ObjectMapper();
    	String response = objectMapper.writeValueAsString(channels);
        return response;    
    }
    
//    @GetMapping("/channelsViewJson")
//    public String channelsViewJson(@RequestParam(name = "start") String start) throws IOException {

    public String channelsViewJson(String start) throws IOException {
    
    	Map<String, GvpChannel> channels = hbaseUtilityContainer.getChannels(start);    	
     	ObjectMapper objectMapper = new ObjectMapper();
     	// support Java 8 date time apis
     	objectMapper.registerModule(new JavaTimeModule());    	
     	objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);    	
     	
    	GvpChannel gvpChannel = new GvpChannel("headers");
    	gvpChannel.playlist_id = "playlist";
    	gvpChannel.hbase_index = "hbase_index";
    	gvpChannel.created = LocalDateTime.now();
    	gvpChannel.lastCheck = LocalDateTime.now();
    	gvpChannel.scheduled = LocalDateTime.now();
    	String headers = objectMapper.writeValueAsString(gvpChannel);
    	JsonNode rootNode = objectMapper.readTree(headers);
    	
    	
        JsonData jsonData = new JsonData();
        jsonData.buildCollections();
    	
        // Iterate over each field in the JSON object
        rootNode.fields().forEachRemaining(field -> {
        		String fieldName = field.getKey();
        		JsonNode fieldValue = field.getValue();
                String fieldType = getJsonNodeType(fieldValue);
        		jsonData.addColumn(fieldName, fieldType);
//        		System.out.println("Field Name: " + fieldName + ", Field Value: " + fieldValue);
           });    	
    	

    	String response = objectMapper.writeValueAsString(channels);
        rootNode = objectMapper.readTree(response);
        // Iterate over each entry in the root node (which is a JSON object)
        rootNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode channelNode = entry.getValue();
//            System.out.println("Key: " + key);
            Row row = jsonData.addRow();
            
            // Iterate over each field in the JSON object
            channelNode.fields().forEachRemaining(field -> {
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                row.getC().add(new Cell(fieldValue));
//                System.out.println("Field Name: " + fieldName + ", Field Value: " + fieldValue);
            });
        });    	
        
        String jsonMessage = objectMapper.writeValueAsString(jsonData);
        System.out.println(jsonMessage);
        return jsonMessage;
//        return response;    
    }

    private static String getJsonNodeType(JsonNode node) {
    	
        if (node.isTextual()) {
        	return "string";
        } else if (node.isInt()) {
            return "number";
        } else if (node.isLong()) {
            return "number";
        } else if (node.isDouble()) {
            return "number";
        } else if (node.isBoolean()) {
            return "boolean";
        } else if (node.isArray()) {
            return "Array";
        } else if (node.isObject()) {
            return "Object";
        } else {
            return "Unknown";
        }    
    }        

    
    public String videosViewJson(String start) throws IOException {
        
    	Map<String, GvpVideo> videos = hbaseUtilityContainer.getVideos(start);    	
     	ObjectMapper objectMapper = new ObjectMapper();
     	// support Java 8 date time apis
     	objectMapper.registerModule(new JavaTimeModule());    	
     	objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);    	

     	GvpVideo gvpVideo = new GvpVideo("headers");
    	gvpVideo.channel_id = "playlist";
    	
    	gvpVideo.created = LocalDateTime.now();
    	gvpVideo.lastCheck = LocalDateTime.now();
    	gvpVideo.lastComment = LocalDateTime.now();
    	
    	String headers = objectMapper.writeValueAsString(gvpVideo);
    	JsonNode rootNode = objectMapper.readTree(headers);
    	
    	
        JsonData jsonData = new JsonData();
        jsonData.buildCollections();
    	
        // Iterate over each field in the JSON object
        rootNode.fields().forEachRemaining(field -> {
        		String fieldName = field.getKey();
        		JsonNode fieldValue = field.getValue();
                String fieldType = getJsonNodeType(fieldValue);
        		jsonData.addColumn(fieldName, fieldType);
//        		System.out.println("Field Name: " + fieldName + ", Field Value: " + fieldValue);
           });    	
    	

    	String response = objectMapper.writeValueAsString(videos);
        rootNode = objectMapper.readTree(response);
        // Iterate over each entry in the root node (which is a JSON object)
        rootNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            JsonNode videoNode = entry.getValue();
//            System.out.println("Key: " + key);
            Row row = jsonData.addRow();
            
            // Iterate over each field in the JSON object
            videoNode.fields().forEachRemaining(field -> {
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                row.getC().add(new Cell(fieldValue));
            });
        });    	
        
        String jsonMessage = objectMapper.writeValueAsString(jsonData);
        System.out.println(jsonMessage);
        return jsonMessage;
//        return response;    
    }

    
    
    
    public static String formatForGoogleCharts(LocalDateTime dateTime) {
    	int year = dateTime.getYear();
        int month = dateTime.getMonthValue() - 1; // Google Charts uses 0-based month
        int day = dateTime.getDayOfMonth();
        int hour = dateTime.getHour();
        int minute = dateTime.getMinute();
        int second = dateTime.getSecond();

        return String.format("Date(%d, %d, %d, %d, %d, %d)", 
                year, month, day, hour, minute, second);
    }

    
    public String buildRedirectionForm(String url1, String url2, String placeholder, String button) {
    	String s = ""
    		 + "<input type=\"text\" id=\"in\" placeholder=\" " + placeholder + "   \">"
    		 + "<button type=\"button\" onclick=\"window.location.href="
    		 + "'"+url1+"'+document.getElementById('in').value+'"+url2+"'\"> " + button + " </button>"
    		 + "";
		return s;
    }
    
    
    
}
