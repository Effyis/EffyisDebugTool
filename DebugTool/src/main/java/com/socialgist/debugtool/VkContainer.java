package com.socialgist.debugtool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.socialgist.debugtool.items.DtHtmlVkApiResult;
import com.socialgist.debugtool.items.DtHtmlYoutubeApiResult;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.MySQLContainer;
import com.socialgist.debugtool.utils.items.GvpChannel;
import com.socialgist.debugtool.utils.items.GvpComment;
import com.socialgist.debugtool.utils.items.GvpUtils;
import com.socialgist.debugtool.utils.items.GvpVideo;
import com.socialgist.debugtool.utils.items.PremiumSubscription;
import com.socialgist.debugtool.utils.items.VkComment;
import com.socialgist.debugtool.utils.items.VkPost;
import com.socialgist.debugtool.utils.items.VkWall;

import jakarta.xml.bind.DatatypeConverter;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;


@Component
public class VkContainer  {

	@Autowired
	MySQLContainer mySQLContainer;
	@Autowired
	HBaseContainer hbaseContainer;
	@Autowired
	HttpContainer httpContainer;
	@Autowired
	CnRepository repository;
	
	ObjectMapper mapper = new ObjectMapper();
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	private static Logger LOGGER = LoggerFactory.getLogger(VkContainer.class);
	
	public void init() {
	}
	
	public DtHtmlVkApiResult readWallHtml(String wall_id) throws Exception {
 		if ((wall_id==null) || (wall_id.isBlank())) return new DtHtmlVkApiResult();
		
    	StringBuilder htmlTable = new StringBuilder();
		String url;
		
		if (wall_id.charAt(0) == '-') {		
			url = "https://api.vk.com/method/groups.getById?v=5.131&group_id=" + wall_id + "&access_token=" + repository.vk_token;
		}
		else {
			url = "https://api.vk.com/method/users.get?v=5.131&user_ids=" + wall_id + "&access_token=" + repository.vk_token;		
		}
		
		String json = httpContainer.connectToRest_Json(url);
   	 	htmlTable.append(url);
   	 	htmlTable.append("<hr>");
   	 	JsonNode rootNode = mapper.readTree(json);
   	 	String s = rootNode.toPrettyString();
   	 	htmlTable.append(convertJsonToHtml(rootNode.toPrettyString()));
   	 	DtHtmlVkApiResult result = new DtHtmlVkApiResult();
   	    result.htmlText = htmlTable.toString();
//   	    if (rootNode.path("items").get(0)!=null )
//   	    	result.playlist_id = rootNode.path("items").get(0).path("contentDetails").path("relatedPlaylists").path("uploads").asText();
		return result;
	}

	public DtHtmlVkApiResult readWallPostsHtml(String wall_id) throws Exception {
 		if ((wall_id==null) || (wall_id.isBlank())) return new DtHtmlVkApiResult();
		
    	StringBuilder htmlTable = new StringBuilder();
		String url = "https://api.vk.com/method/wall.get?extended=1&count=10&v=5.81&owner_id="
				+ wall_id + "&access_token=" + repository.vk_token;
		String json = httpContainer.connectToRest_Json(url);
   	 	htmlTable.append(url);
   	 	htmlTable.append("<hr>");
   	 	JsonNode rootNode = mapper.readTree(json);
   	 	String s = rootNode.toPrettyString();
   	 	htmlTable.append(convertJsonToHtml(rootNode.toPrettyString()));
   	 	DtHtmlVkApiResult result = new DtHtmlVkApiResult();
   	    result.htmlText = htmlTable.toString();
//   	    if (rootNode.path("items").get(0)!=null )
//   	    	result.playlist_id = rootNode.path("items").get(0).path("contentDetails").path("relatedPlaylists").path("uploads").asText();
		return result;
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

public DtHtmlVkApiResult readPostHtml(String post_id) throws Exception {

 		if ((post_id==null) || (post_id.isBlank())) return new DtHtmlVkApiResult();
		
    	StringBuilder htmlTable = new StringBuilder();
		String url = "https://api.vk.com/method/wall.getById?v=5.131&posts="
				+ post_id + "&access_token=" + repository.vk_token;
		String json = httpContainer.connectToRest_Json(url);
   	 	htmlTable.append(url);
   	 	htmlTable.append("<hr>");
   	 	JsonNode rootNode = mapper.readTree(json);
   	 	String s = rootNode.toPrettyString();
   	 	htmlTable.append(convertJsonToHtml(rootNode.toPrettyString()));
   	 	DtHtmlVkApiResult result = new DtHtmlVkApiResult();
   	    result.htmlText = htmlTable.toString();
//   	    if (rootNode.path("items").get(0)!=null )
//   	    	result.playlist_id = rootNode.path("items").get(0).path("contentDetails").path("relatedPlaylists").path("uploads").asText();
		return result;
	
	
	//https://api.vk.com/method/wall.getById?posts={owner_id}_{post_id}&access_token={your_access_token}&v=5.131	
	// TODO Auto-generated method stub
//	return null;
}

public DtHtmlVkApiResult readCommentsHtml(String post_id) throws Exception {
 		if ((post_id==null) || (post_id.isBlank())) return new DtHtmlVkApiResult();

        String[] parts = post_id.split("_");
        // Parse the owner ID and post ID
        int ownerId = Integer.parseInt(parts[0]);
        int postId = Integer.parseInt(parts[1]); 		
 		
    	StringBuilder htmlTable = new StringBuilder();
		String url = "https://api.vk.com/method/wall.getComments?extended=1&v=5.81&count=100&sort=desc&preview_length=0&post_id="
				+ postId + "&owner_id=" + ownerId + "&access_token=" + repository.vk_token;
		String json = httpContainer.connectToRest_Json(url);
   	 	htmlTable.append(url);
   	 	htmlTable.append("<hr>");
   	 	JsonNode rootNode = mapper.readTree(json);
   	 	String s = rootNode.toPrettyString();
   	 	htmlTable.append(convertJsonToHtml(rootNode.toPrettyString()));
   	 	DtHtmlVkApiResult result = new DtHtmlVkApiResult();
   	    result.htmlText = htmlTable.toString();
//   	    if (rootNode.path("items").get(0)!=null )
//   	    	result.playlist_id = rootNode.path("items").get(0).path("contentDetails").path("relatedPlaylists").path("uploads").asText();
		return result;
}

public TreeMap<Long, VkPost> readPostsForWall(String wall_id) throws Exception {
	
	TreeMap<Long, VkPost> posts =  new TreeMap<>(Collections.reverseOrder());
//	return mapper.readTree(json);

	boolean isStop = false;
	long start_time = System.currentTimeMillis() / 1000L;
	long time_max = 0; // latest video in the channel
	long total_count = 0;
	
	int offset = 0;
	

	long post_border = GvpUtils.getUnixTime(LocalDate.now().minusDays(90).toString());

	String url = "https://api.vk.com/method/wall.get?owner_id=" + wall_id + "&&v=5.131extended=1&count=100&filter=all&access_token=" + repository.vk_token;
	String next_url = url;
	while (true) {
		if (total_count>200) { 
			LOGGER.info("************ total_count reached: {}", total_count);
			break;
		}
		if (next_url == null) {
			isStop = true;
		}
		if (isStop) {
			break;
		}
		String json = httpContainer.connectToRest_Json(next_url);
		JsonNode rootNode = mapper.readTree(json);
		if (rootNode == null)
			return posts;
		if (VkAPIError.isError(rootNode)) {
			VkAPIError error = new VkAPIError(rootNode);
			if (error.isPermissionDenied()) {
				throw new Exception(error.message);
			}
			break;
		}
		offset = offset + 100;
		
		if (offset > rootNode.path("response").path("count").asInt(0)) 
			next_url = null;
		else {
			next_url = url + "&offset=" + offset; 
		}

		JsonNode nodeItems = rootNode.path("response").path("items");
		Iterator<JsonNode> itItems = nodeItems.elements();
		if (!itItems.hasNext()) break;

		//*******************************					
		//Read Items array to collection					
		//*******************************					
		while (itItems.hasNext()) {
				JsonNode item_node = itItems.next();
				long id = item_node.path("id").asLong(0);
				long owner_id = item_node.path("owner_id").asLong(0);
				String post_type = item_node.path("post_type").asText();
				long date = 0L;
				if (!item_node.path("date").isMissingNode()) {
						date = item_node.path("date").asLong();
				}
				int is_pinned = item_node.path("is_pinned").asInt(0);
				
				if ((is_pinned == 0) && (date < post_border)) {  
//						LOGGER.info("TIMEBORDER_FOUND - {}, {}, {} ", VkUtils.formatTime(date));
						isStop = true;
						break;
				}
				String post_id = "" + owner_id + "_" + id;
				
				String hbase_post_id = buildHBasePostId(post_id);
				VkPost vkPost = new VkPost(post_id, hbase_post_id, date);
				vkPost.api_commentsCount = item_node.path("comments").path("count").asLong(0);
				posts.put(date, vkPost);
				total_count++;
		}
	} // while (true)
	return posts;
}

public TreeMap<Long, VkComment> readCommentsForPost(VkPost vkPost) throws Exception {
	TreeMap<Long,VkComment> comments =  new TreeMap<>();
	
	boolean isStop = false;
	int offset = 0;
	
	String url = "https://api.vk.com/method/wall.getComments?owner_id=" + vkPost.owner_id + "&post_id=" + vkPost.post_id + "&extended=1&v=5.81&count=100&sort=desc&preview_length=0&access_token=" + repository.vk_token;
	String next_url = url;
	int i=0;
	while (true) {
		i++;
		if (i>1000) { 
			LOGGER.info("************checkForComments() Too big offset: {}", next_url);
			break;
		}
		if (next_url == null) {
			isStop = true;
		}
		if (isStop) {
			break;
		}
		String json = httpContainer.connectToRest_Json(next_url);
		JsonNode rootNode = mapper.readTree(json);
		if (rootNode == null)
			return comments;
		if (VkAPIError.isError(rootNode)) {
			VkAPIError error = new VkAPIError(rootNode);
			if (error.isPermissionDenied()) {
				return comments;
			}
			break;
		}
		offset = offset + 100;
		if (offset > rootNode.path("response").path("count").asInt(0)) 
			next_url = null;
		else {
			next_url = url + "&offset=" + offset; 
		}
		
        JsonNode profilesItems = rootNode.path("response").path("profiles");        
        Map<Long, String> profiles = new HashMap<>();
        for (JsonNode profileNode : profilesItems) {
            profiles.put(profileNode.path("id").asLong(), profileNode.path("first_name").asText() + " " + profileNode.path("last_name").asText());
        }		
		JsonNode nodeItems = rootNode.path("response").path("items");
		Iterator<JsonNode> itItems = nodeItems.elements();
		if (!itItems.hasNext()) break;
//		c_mapItems.clear();  
//*******************************					
//Read Items array to collection					
//*******************************	
		long comment_border = GvpUtils.getUnixTime(LocalDate.now().minusDays(90).toString());
		while (itItems.hasNext()) {
			JsonNode itemNode = itItems.next();
			long comment_id = itemNode.path("id").asLong(0);
			long date = 0L;
			if (!itemNode.path("date").isMissingNode()) {
				date = itemNode.path("date").asLong();
			} 
			if (date <= comment_border){   
					isStop = true;
					break;
			}

			String hbase_index = buildHBaseCommentId(vkPost.owner_id, vkPost.post_id, comment_id);
			VkComment gvc = hbaseContainer.checkVkCommentForExistence(hbase_index);
			if (gvc == null) { 
				gvc = new VkComment();
				gvc.hbase_index = hbase_index;
				gvc.comment_id = itemNode.path("id").asText(); 
				gvc.created_ts = date;
				gvc.authorDisplayName = profiles.get(itemNode.path("from_id").asLong(0)); 				
			}
			comments.put(date, gvc);
		} // items loop
	} // while requests loop 
	return comments;
}

public String buildHBaseWallId(String wall_id) {
	return "wall." + wall_id;     
}

public String buildHBasePostId(String post_id) {
	return "post." + post_id;     
}

public String buildHBaseCommentId(String owner_id, String post_id, long comment_id) {
	return "comment." + owner_id + "_" + post_id + "." + comment_id;     
}




}
