package com.socialgist.debugtool;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.socialgist.debugtool.items.DtHtmlTable;
import com.socialgist.debugtool.items.DtHtmlVkApiResult;
import com.socialgist.debugtool.items.DtHtmlYoutubeApiResult;
import com.socialgist.debugtool.model.Cell;
import com.socialgist.debugtool.model.JsonData;
import com.socialgist.debugtool.model.Row;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.StatsContainer;
import com.socialgist.debugtool.utils.items.GvpChannel;
import com.socialgist.debugtool.utils.items.GvpComment;
import com.socialgist.debugtool.utils.items.GvpVideo;
import com.socialgist.debugtool.utils.items.VkComment;
import com.socialgist.debugtool.utils.items.VkPost;
import com.socialgist.debugtool.utils.items.VkWall;

@RestController
public class VK_ServiceController {
	
	@Autowired
	HBaseContainer hbaseContainer;
	@Autowired
	HBaseUtilityContainer hbaseUtilityContainer;
	@Autowired
	MySQLUtilsContainer mySQLUtilsContainer;
	@Autowired
	VkContainer vkContainer;
	@Autowired
	StatsContainer statsContainer;	
	
    @GetMapping("/api_wall")
 	public String api_wall(@RequestParam(name = "wall_id", required = false ) String wall_id) throws Exception {

    	DtHtmlVkApiResult apiResult = vkContainer.readWallHtml(wall_id);
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK API - groups.getById() || users.get()</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Wall:</td>");
 			htmlTable.append("<td><b>" + wall_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("api_wall?wall_id=", "", "Enter wall_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
/* 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td><a target='_blank' href=\"channel_list?channel_id=" + wall_id + "\">Channel Debug View</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist?channel_id=" + wall_id + "\">Playlists API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist_items?playlist_id=" + apiResult.playlist_id + "\">PlaylistItems API</a></td>");
 		}
*/ 		
   	 	htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }

    @GetMapping("/api_wall_posts")
 	public String api_wall_posts(@RequestParam(name = "wall_id", required = false ) String wall_id) throws Exception {

    	DtHtmlVkApiResult apiResult = vkContainer.readWallPostsHtml(wall_id);
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK API - wall.get()</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Wall:</td>");
 			htmlTable.append("<td><b>" + wall_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("api_wall?wall_id=", "", "Enter wall_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
/* 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td><a target='_blank' href=\"channel_list?channel_id=" + wall_id + "\">Channel Debug View</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist?channel_id=" + wall_id + "\">Playlists API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_playlist_items?playlist_id=" + apiResult.playlist_id + "\">PlaylistItems API</a></td>");
 		}
*/ 		
   	 	htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }
    
    @GetMapping("/api_post")
 	public String api_post(@RequestParam(name = "post_id", required = false ) String post_id) throws Exception {
    	
    	DtHtmlVkApiResult apiResult = vkContainer.readPostHtml(post_id);
    	
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK API - wall.getById()</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0 ><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Post_id:</td>");
 			htmlTable.append("<td><b>" + post_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("api_post?post_id=", "", "Enter post_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"post_list?post_id=" + post_id + "\">Post Debug View</a></td>");
   	 	htmlTable.append("<td align=center width=\"20\">|</td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"https://vk.com/wall" + post_id + "\">Post URL</a></td>");
 		if ((apiResult.wall_id!=null)) {
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_wall?wall_id=" + apiResult.wall_id + "\">Wall API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"wall_list?wall_id=" + apiResult.wall_id + "\">Wall Debug View</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }

    
    @GetMapping("/api_comments")
 	public String api_comments(@RequestParam(name = "post_id", required = false ) String post_id) throws Exception {
    	
    	DtHtmlVkApiResult apiResult = vkContainer.readCommentsHtml(post_id);
    	
    	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK API - wall.getComments()</h2></td></tr></table>");
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0 ><tr>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<td>Post_id:</td>");
 			htmlTable.append("<td><b>" + post_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("api_comments?post_id=", "", "Enter post_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"post_list?post_id=" + post_id + "\">Post Debug View</a></td>");
   	 	htmlTable.append("<td align=center width=\"20\">|</td>");
   	 	htmlTable.append("<td><a target='_blank' href=\"https://vk.com/wall" + post_id + "\">Post URL</a></td>");
 		if ((apiResult.wall_id!=null)) {
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_wall?wall_id=" + apiResult.wall_id + "\">Wall API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"wall_list?wall_id=" + apiResult.wall_id + "\">Wall Debug View</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");
 		if ((apiResult.htmlText!=null)) {
 			htmlTable.append("<hr>");
 			htmlTable.append(apiResult.htmlText);
 		}
    	return htmlTable.toString();
    }
    
    public String buildVkRedirectionForm(String url1, String url2, String placeholder, String button) {
    	String s = ""
    		 + "<input type=\"text\" id=\"in\" placeholder=\" " + placeholder + "   \">"
    		 + "<button type=\"button\" onclick=\"window.location.href="
    		 + "'"+url1+"'+document.getElementById('in').value+'"+url2+"'\"> " + button + " </button>"
    		 + "";
		return s;
    }
    
    @GetMapping("/wall_list")
 	public String wall_list(@RequestParam(name = "wall_id") String wall_id) throws Exception {

    	VkWall ghc=null;
 		if ((wall_id!=null) && (!wall_id.isBlank()))	{
 			ghc = hbaseContainer.checkWallForExistence(wall_id);
 		}
 		
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK Wall Debug View</h2></td></tr></table>");
   	 	
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((ghc!=null)) {
 			htmlTable.append("<td>wall_id:</td>");
 			htmlTable.append("<td><b>" + wall_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
 		else {
 	 		if ((wall_id!=null) && (!wall_id.isBlank()))	{
 	 			htmlTable.append("<td>Wall with wall_id of '"+wall_id+"' not found in HBase.</td>");
 	 			htmlTable.append("<td width=\"20%\"></td>");
 	 		}
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("wall_list?wall_id=", "", "Enter wall_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
 		if ((ghc!=null)) {
 	   	 	htmlTable.append("<td><a target='_blank' href=\"api_wall?wall_id=" + wall_id + "\">wall API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"api_wall_posts?wall_id=" + wall_id + "\">wall posts API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"https://www.vk.com/wall" + wall_id + "\">Wall URL</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");   	 	
   	 	htmlTable.append("<hr>");

 		if ((ghc==null)) return htmlTable.toString();
 		
   	 	
   	 	TreeMap<Long, VkPost> posts = null;
   	 	try {
   	 		posts = vkContainer.readPostsForWall(wall_id);
   	 	}
   	 	catch(Exception e) { 
   	   	 	htmlTable.append("<p>");
   	   	 	htmlTable.append("<b>Error: " + e.getMessage() + "</b>");
   	   	 	return htmlTable.toString();
   	 	}

   	 	htmlTable.append("<b>Actual posts list from API (limit 90 days or 200 latest videos):</b>");
   	 	htmlTable.append("<p>");
   	 	
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>post id</th><th>debug</th><th>url</th><th>created</th><th>last check</th><th>api comments</th><th>hb comments</th><th>comments cent</th><th>last comment</th></tr>");
     
   	 	for (Entry<Long, VkPost> entry : posts.entrySet()) {
   	 		Long video_id = entry.getKey();
   	 		VkPost v1 = entry.getValue();
   	 		VkPost v2 = hbaseContainer.hBase_getPost("post." + v1.post_id);
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(v1.composite_id).append("</td>");
   	 	    String post_href = String.format("<a target='_blank' href='/post_list?post_id=%s'>debug</a>", v1.composite_id);
   	 		htmlTable.append("<td>").append(post_href).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://www.vk.com/wall%s'>url</a>", v1.composite_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		
   	 		
   	 		htmlTable.append("<td>").append(Instant.ofEpochSecond(v1.created_ts)).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v2==null)?"---" : Instant.ofEpochSecond(v2.hbase_lastCheck_ts)).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v2==null)?"---" : v1.api_commentsCount).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v2==null)?"---" : v2.hbase_commentsCount).append("</td>");
   	 		htmlTable.append("<td  align='right'>").append((v2==null)?"---" : v2.hbase_commentsSent).append("</td>");
   	 		htmlTable.append("<td  align='right'>").append(((v2==null)||(v2.hbase_lastComment_ts==0))?"---" : Instant.ofEpochSecond(v2.hbase_lastComment_ts)).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
     
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }

    @GetMapping("/post_list")
 	public String post_list(@RequestParam(name = "post_id") String post_id) throws Exception {

    	VkPost vkPost=null;
 		if ((post_id!=null) && (!post_id.isBlank()))	{
 			vkPost = hbaseContainer.checkPostForExistence(post_id);
 		}
 		
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK Post Debug View</h2></td></tr></table>");
   	 	
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append("<table border=0><tr>");
 		if ((vkPost!=null)) {
 			htmlTable.append("<td>post_id:</td>");
 			htmlTable.append("<td><b>" + post_id + "</b></td>");
 			htmlTable.append("<td width=\"20%\"></td>");
 		}
 		else {
 	 		if ((post_id!=null) && (!post_id.isBlank()))	{
 	 			htmlTable.append("<td>Post with post_id of '"+post_id+"' not found in HBase.</td>");
 	 			htmlTable.append("<td width=\"20%\"></td>");
 	 		}
 		}
   	 	htmlTable.append("<td valign='bottom'>" + buildVkRedirectionForm("post_list?post_id=", "", "Enter post_id", "Search") + "</td>");
   	 	htmlTable.append("<td width=\"10%\"></td>");
 		if ((vkPost!=null)) {
 	   	 	htmlTable.append("<td><a target='_blank' href=\"api_post?post_id=" + post_id + "\">Post API</a></td>");
 			htmlTable.append("<td align=center width=\"20\">|</td>");
 			htmlTable.append("<td><a target='_blank' href=\"https://www.vk.com/wall" + post_id + "\">Post URL</a></td>");
 		}
   	 	htmlTable.append("</tr></table>");   	 	
   	 	htmlTable.append("<hr>");

 		if ((vkPost==null)) return htmlTable.toString();
 		
   	 	htmlTable.append("<b> HBase Data(collected):</b>");
   	 	htmlTable.append("<table border=0>");
   	 	htmlTable.append("<tr><td>hbase index:</td><td>" + vkPost.hbase_index + "</td><td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td></tr>");
   	 	htmlTable.append("<tr><td>video created:</td><td>" + Instant.ofEpochSecond(vkPost.created_ts) + "</td><td>&nbsp;</td></tr>");
   	 	htmlTable.append("<tr><td>last API check:</td><td>" + Instant.ofEpochSecond(vkPost.hbase_lastCheck_ts) + "<td>&nbsp;</td></td>");
   	 	htmlTable.append("<td>comments count:</td><td>" + vkPost.hbase_commentsCount + "</td><td>&nbsp;</td></tr><tr>");

 		if (vkPost.hbase_lastComment_ts == 0) 
   	 		htmlTable.append("<td>last comment:</td><td>&nbsp;&nbsp;&nbsp;&nbsp;").append("---").append("</td><td>&nbsp;</td>");
 		else
   	 		htmlTable.append("<td>last comment:</td><td>" + Instant.ofEpochSecond(vkPost.hbase_lastComment_ts) + "<td>&nbsp;</td></td>");

   	 	htmlTable.append("<td>comments sent:</td><td>" + vkPost.hbase_commentsSent + "</td></tr>");
   	 	htmlTable.append("</table>");
 		
 		htmlTable.append("<hr>");
   	 	htmlTable.append("<b>Actual comments list from API (limit 90 days or 200 latest comments):</b>");
   	 	htmlTable.append("<p>");
   	 	
    	TreeMap<Long, VkComment> comments = vkContainer.readCommentsForPost(vkPost);
   	 	
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>comment id</th><th>url</th><th>author</th><th>published</th><th alignr=center>collected</th></tr>");
     
   	 	for (Entry<Long, VkComment> entry : comments.entrySet()) {
   	 		long id = entry.getKey();
   	 		VkComment comment = entry.getValue();
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(comment.comment_id).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://vk.com/wall%s?w=wall%s_r%s'>url</a>", vkPost.composite_id, vkPost.composite_id,  comment.comment_id, comment.comment_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		htmlTable.append("<td>").append(comment.authorDisplayName).append("</td>");
   	 		if (comment.created_ts == 0) 
   	 			htmlTable.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;").append("---").append("</td>");
   	 		else
   	 			htmlTable.append("<td>&nbsp;&nbsp;").append(GvpComment.epochSecondsToLocalDateTimeUTC(comment.created_ts)).append("</td>");
   	 		if (comment.collected_ts == 0) 
   	 			htmlTable.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;").append("---").append("</td>");
   	 		else
   	 			htmlTable.append("<td>&nbsp;&nbsp;").append(GvpComment.epochSecondsToLocalDateTimeUTC(comment.collected_ts)).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
     
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }

    @GetMapping("/vk_wall_hbaseview")
   	public String wall_hbaseview(@RequestParam(name = "start", required = false ) String start) throws Exception {
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK Walls HBase</h2></td></tr></table>");
   	 	
    	Map<String, VkWall> walls = hbaseUtilityContainer.getHBaseWalls(start);
    	Map.Entry<String, VkWall> lastEntry = null;
        for (Map.Entry<String, VkWall> entry : walls.entrySet()) {
               lastEntry = entry;  // Overwrite on each iteration
        }    	
        String nextPage =  lastEntry.getKey();
    	
 		htmlTable.append("<hr>");
   	 	htmlTable.append("<p>");
 		htmlTable.append("&nbsp;<a href=\"vk_wall_hbaseview?start=" + nextPage + "\">" + " Next 100 >>  " + "</a>");
   	 	htmlTable.append("<p>");
 		
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>wall id</th><th>debug</th><th>url</th><th>hbase_ref</th><th>last check</th><th>hidden</th></tr>");
     
   	 	for (Entry<String, VkWall> entry : walls.entrySet()) {
   	 		String wall_id = entry.getKey();
   	 		VkWall v1 = entry.getValue();
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(v1.wall_id).append("</td>");
   	 	    String post_href = String.format("<a target='_blank' href='/wall_list?wall_id=%s'>debug</a>", v1.wall_id);
   	 		htmlTable.append("<td>").append(post_href).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://www.vk.com/wall%s'>url</a>", v1.wall_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		htmlTable.append("<td>").append(v1.hbase_ref_index).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v1==null)?"---" : Instant.ofEpochSecond(v1.lastCheck_ts)).append("</td>");
   	 		htmlTable.append("<td>").append(v1.hidden).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }

    @GetMapping("/vk_wall_ref_hbaseview")
   	public String wall_ref_hbaseview(@RequestParam(name = "start", required = false ) String start) throws Exception {
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK Walls HBase</h2></td></tr></table>");
   	 	
    	Map<String, VkWall> walls = hbaseUtilityContainer.getHBaseWallsRef(start);
    	Map.Entry<String, VkWall> lastEntry = null;
        for (Map.Entry<String, VkWall> entry : walls.entrySet()) {
               lastEntry = entry;  // Overwrite on each iteration
        }    	
        String nextPage =  lastEntry.getKey();
    	
 		htmlTable.append("<hr>");
   	 	htmlTable.append("<p>");
 		htmlTable.append("&nbsp;<a href=\"vk_wall_ref_hbaseview?start=" + nextPage + "\">" + " Next 100 >>  " + "</a>");
   	 	htmlTable.append("<p>");
 		
   	 	htmlTable.append("<table border=1 cellspacing=0 cellpadding=5>");
   	 	htmlTable.append("<tr><th>wall id</th><th>debug</th><th>url</th><th>hbase_ref</th><th>last check</th><th>hidden</th></tr>");
     
   	 	for (Entry<String, VkWall> entry : walls.entrySet()) {
   	 		String wall_id = entry.getKey();
   	 		VkWall v1 = entry.getValue();
   	 		
   	 		htmlTable.append("<tr>");
   	 		htmlTable.append("<td>").append(v1.wall_id).append("</td>");
   	 	    String post_href = String.format("<a target='_blank' href='/wall_list?wall_id=%s'>debug</a>", v1.wall_id);
   	 		htmlTable.append("<td>").append(post_href).append("</td>");
   	 	    String url_href = String.format("<a target='_blank' href='https://www.vk.com/wall%s'>url</a>", v1.wall_id);
   	 		htmlTable.append("<td>").append(url_href).append("</td>");
   	 		htmlTable.append("<td>").append(v1.hbase_ref_index).append("</td>");
   	 		htmlTable.append("<td align='right'>").append((v1==null)?"---" : Instant.ofEpochSecond(v1.lastCheck_ts)).append("</td>");
   	 		htmlTable.append("<td>").append(v1.hidden).append("</td>");
   	 		htmlTable.append("</tr>");
   	 	}
   	 	htmlTable.append("</table>");
   	 	return htmlTable.toString();
    }
    
    
}
