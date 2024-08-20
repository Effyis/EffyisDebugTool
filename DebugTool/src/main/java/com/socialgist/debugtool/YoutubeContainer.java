package com.socialgist.debugtool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.socialgist.gvp.utils.HBaseContainer;
import com.socialgist.gvp.utils.MySQLContainer;
import com.socialgist.gvp.utils.items.GvpChannel;
import com.socialgist.gvp.utils.items.GvpComment;
import com.socialgist.gvp.utils.items.GvpUtils;
import com.socialgist.gvp.utils.items.GvpVideo;
import com.socialgist.gvp.utils.items.PremiumSubscription;

import jakarta.xml.bind.DatatypeConverter;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.TreeMap;


@Component
public class YoutubeContainer  {

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

	private static Logger LOGGER = LoggerFactory.getLogger(YoutubeContainer.class);
	
	public void init() {
	}
	
	public JsonNode readChannelJson(String channel_id) throws Exception {
/*		
		auditDetails: Audit details for the channel, if applicable.		
		brandingSettings: Settings related to the channel's branding, including the channel's profile image and banner.
		contentDetails: Details about the channel's content, such as its uploaded videos and playlists.
		contentOwnerDetails
		id
		localizations: Localized metadata for the channel, such as its title and description in different languages.
		snippet: Basic information about the channel, including its title, description, and thumbnails.
		statistics: Channel statistics, including the number of subscribers, video views, and comments.
		status: Information about the channel's status, such as privacy status and country.
		topicDetails: Information about topics associated with the channel.
*/		
		String url = "https://www.googleapis.com/youtube/v3/channels?part=id,snippet,contentDetails,contentOwnerDetails,statistics,topicDetails,brandingSettings,status,localizations&maxResults=50&id="
				+ channel_id + "&key=" + repository.youtube_token;
		String json = httpContainer.connectToRest_Json(url);
		return mapper.readTree(json);
	}

	public JsonNode readPlaylistJson(String channel_id) throws Exception {
/*
		id
		contentDetails: Details about the content of the playlist, including the videos it contains and their order.
		localizations
		player
		snippet: Basic information about the playlist, including its title, description, and thumbnails.
		status: Information about the playlist's status, such as privacy status and the number of videos in the playlist.
*/
		String url = "https://www.googleapis.com/youtube/v3/playlists?part=id,contentDetails,localizations,player,snippet,status&maxResults=50&channelId="
				+ channel_id + "&key=" + repository.youtube_token;
		String json = httpContainer.connectToRest_Json(url);
		return mapper.readTree(json);
	}

/*
	public JsonNode readPlaylistJson(String playlist_id) throws Exception {
		id
		snippet: Basic information about the playlist, including its title, description, and thumbnails.
		status: Information about the playlist's status, such as privacy status and the number of videos in the playlist.
		contentDetails: Details about the content of the playlist, including the videos it contains and their order.
		String url = "https://www.googleapis.com/youtube/v3/playlistItems?part=id,snippet,status,contentDetails&maxResults=50&playlistId="
				+ playlist_id + "&key=" + repository.youtube_token;
		String json = httpContainer.connectToRest_Json(url);
		return mapper.readTree(json);
	}
*/
	
	
	public JsonNode readVideoJson(String video_id) throws Exception {
/*		
		id
		contentDetails: Details about the video's content, such as its duration and dimension.
		fileDetails - owner only
		liveStreamingDetails
		localizations: Localized metadata for the video, such as its title and description in different languages.
		player
		processingDetails - owner only
		recordingDetails: Information about the video's recording settings and location.
		snippet: Basic information about the video, including its title, description, thumbnails, and tags.
		statistics: Statistics related to the video, such as view count, like count, and comment count.
		status: Information about the video's status, including privacy status and embeddable status.
		suggestions - owner only
		topicDetails: Information about topics associated with the video.
*/		
		
//		String url = "https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails&maxResults=50&key="
//		+ p.gvpToken.token_encoded + "&id=" + videoIds;
//		id,contentDetails,fileDetails,liveStreamingDetails,localizations,player,processingDetails,recordingDetails,snippet,statistics,status,suggestions,topicDetails
		
		String url = "https://www.googleapis.com/youtube/v3/videos?part=id,contentDetails,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails&maxResults=50&id="
				+ video_id + "&key=" + repository.youtube_token;
		String json = httpContainer.connectToRest_Json(url);
		return mapper.readTree(json);
	}
	
//	public HashMap<String, GvpVideo> readChannelContent(String channel_id) {
//	HashMap<String, PremiumSubscription> premiumSubscriptions = new HashMap<>();

	public TreeMap<Long, GvpVideo> readVideosForChannel(String playlist_id) throws Exception {

			TreeMap<Long, GvpVideo> videos =  new TreeMap<>(Collections.reverseOrder());
//			return mapper.readTree(json);
		
			boolean isStop = false;
			long start_time = System.currentTimeMillis() / 1000L;
			long time_max = 0; // latest video in the channel
			long total_count = 0;

			long video_border;
			video_border = getUnixTime(LocalDate.now().minusDays(90).toString());

			String url = "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&maxResults=50&playlistId="
					+ playlist_id + "&key=" + repository.youtube_token;
			String next_url = url;
			while (true) {
				if (next_url == null) {
					isStop = true;
				}
				if (isStop) {
					break;
				}
//				JsonNode rootNode = ro.getJsonObject(p, p.gvpToken, next_url, "check channel", 1);
				String json = httpContainer.connectToRest_Json(next_url);
				JsonNode rootNode = mapper.readTree(json);
				
				if (rootNode == null)
					return videos;
				if (YouTubeAPIError.isError(rootNode)) {
					YouTubeAPIError error = new YouTubeAPIError(rootNode);
					if (error.isChannelDeleted()) {
						return videos;
						//ghc.deleted = 1;
					}
					break;
				}
				JsonNode nodeData = rootNode.path("items");
				Iterator<JsonNode> itItems = nodeData.elements();

				if (rootNode.path("nextPageToken").isMissingNode())
					next_url = null;
				else {
					try {
						next_url = url + "&pageToken="
								+ URLEncoder.encode(rootNode.path("nextPageToken").asText(), "UTF-8");
					} catch (Exception e) {
						next_url = null;
					}
				}

				if (!itItems.hasNext()) {
					isStop = true;
					continue;
				}

				int in_range_count = 0;
				while (itItems.hasNext()) {
					JsonNode item = itItems.next();

					total_count++;
					long ctime = convertTimeToUnixTime(item.path("snippet").path("publishedAt").asText());
					String video_id = item.path("snippet").path("resourceId").path("videoId").asText();
					if (ctime > (time_max))
						time_max = ctime;
					if (ctime < (video_border)) { // in case if first time
						isStop = true;
						continue;
					}
					/*
					 * else if (ctime < ghc.lastCheck_ts) { isStop = true; continue; }
					 */
					else {
						in_range_count++;
					}
					
//					long post_created_time = convertTimeToUnixTime(nodePost.path("snippet").path("publishedAt").asText());
//					GvpVideo ghr = ro.repository.hbaseContainer.checkVideoForExistence(video_id);

//					if (ghr == null) {
						String hbase_post_id = buildHBaseVideoId(video_id, ctime);
						GvpVideo ghr = new GvpVideo(hbase_post_id, ctime);
						videos.put(ctime, ghr);
				}

				if (in_range_count < 40) {
					isStop = true;
				}
			} // while (true)

			return videos;
	}

	public TreeMap<Long, GvpComment> readCommentsNotSent(GvpVideo ghp) throws Exception {
//		TreeMap<Long,GvpComment> comments =  new TreeMap<>(Collections.reverseOrder());
		TreeMap<Long,GvpComment> comments =  new TreeMap<>(); 		
		String url = "https://www.googleapis.com/youtube/v3/commentThreads?part=snippet%2C+replies&maxResults=100&order=time&textFormat=plainText&videoId="
				+ ghp.video_id
				+ "&fields=items(id%2Creplies%2Csnippet)%2CnextPageToken%2CpageInfo%2CtokenPagination&key="
				+ repository.youtube_token;
		String next_url = url;

		long i = 0;
		
		while (true) {
			if (next_url == null) break;
			String json = httpContainer.connectToRest_Json(next_url);
			JsonNode rootNode = mapper.readTree(json);
			if (rootNode == null) return null;
			if (rootNode.path("nextPageToken").isMissingNode())
				next_url = null;
			else {
				try {
					next_url = url + "&pageToken="
							+ URLEncoder.encode(rootNode.path("nextPageToken").asText(), "UTF-8");
				} catch (UnsupportedEncodingException e) {return null;}
			}
			JsonNode nodeData = rootNode.path("items");
			Iterator<JsonNode> itItems = nodeData.elements();
			
			while (itItems.hasNext()) {
				JsonNode commentNode = itItems.next();
				
//				if (commentNode.path("id").asText().equalsIgnoreCase("UgwmtvRlfao3lJAPxTZ4AaABAg")) {
//					i=i+100;
//				}
				
				GvpComment gvc = hbaseContainer.checkCommentForExistence(ghp.video_id, commentNode.path("id").asText());
				if (gvc == null) gvc = new GvpComment();
				gvc.video_id = ghp.video_id;
				gvc.channel_id = ghp.channel_id;
				gvc.comment_id = commentNode.path("id").asText();
				gvc.authorDisplayName = commentNode.path("snippet").path("topLevelComment").path("snippet").path("authorDisplayName").asText();
				gvc.created_ts = GvpUtils.convertToUnixTime(commentNode.path("snippet").path("topLevelComment").path("snippet").path("publishedAt").asText());
				gvc.publishedAt	= commentNode.path("snippet").path("topLevelComment").path("snippet").path("publishedAt").asText();
				ghp.new_commentsCount++;
				comments.put(i++, gvc);
				
				int totalReplyCount = commentNode.path("snippet").path("totalReplyCount").asInt(0);
				int size = commentNode.path("replies").path("comments").size();
				if (totalReplyCount != size) {
					checkReplies(ghp, commentNode.path("id").asText(), comments, i);
				} else {
					JsonNode nodeReplies = commentNode.path("replies").path("comments");
					Iterator<JsonNode> itReplies = nodeReplies.elements();
					while (itReplies.hasNext()) {
						JsonNode replyNode = itReplies.next();
						gvc = hbaseContainer.checkCommentForExistence(ghp.video_id, replyNode.path("id").asText());
						if (gvc == null) gvc = new GvpComment();
						gvc.video_id = ghp.video_id;
						gvc.channel_id = ghp.channel_id;
						gvc.comment_id = replyNode.path("id").asText();
						gvc.authorDisplayName = replyNode.path("snippet").path("authorDisplayName").asText();
						gvc.publishedAt	= replyNode.path("snippet").path("publishedAt").asText();
						gvc.created_ts = GvpUtils.convertToUnixTime(replyNode.path("snippet").path("publishedAt").asText());
						ghp.new_commentsCount++;
						comments.put(i++, gvc);
					}
				}
			}
		}
		return comments;
	}
	
	private void checkReplies(GvpVideo ghp, String parentId, TreeMap<Long, GvpComment> comments, long i) throws Exception {

		String url = "https://www.googleapis.com/youtube/v3/comments?part=snippet&maxResults=100&parentId=" + parentId
				+ "&key=" + repository.youtube_token;
		String next_url = url;

		while (true) {
			if (next_url == null)
				break;
			String json = httpContainer.connectToRest_Json(next_url);
			JsonNode rootNode = mapper.readTree(json);
			if (rootNode == null)
				return;
			if (rootNode.path("nextPageToken").isMissingNode())
				next_url = null;
			else {
				try {
					next_url = url + "&pageToken="
							+ URLEncoder.encode(rootNode.path("nextPageToken").asText(), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					return;
				}
			}
			JsonNode nodeData = rootNode.path("items");
			Iterator<JsonNode> itReplies = nodeData.elements();

			while (itReplies.hasNext()) {
				JsonNode replyNode = itReplies.next();
				GvpComment gvc = hbaseContainer.checkCommentForExistence(ghp.video_id, replyNode.path("id").asText());
				if (gvc == null) gvc = new GvpComment();
				gvc.video_id = ghp.video_id;
				gvc.channel_id = ghp.channel_id;
				gvc.comment_id = replyNode.path("id").asText();
				gvc.authorDisplayName = replyNode.path("snippet").path("authorDisplayName").asText();
				gvc.publishedAt	= replyNode.path("snippet").path("publishedAt").asText();
				gvc.created_ts = GvpUtils.convertToUnixTime(replyNode.path("snippet").path("publishedAt").asText());
				ghp.new_commentsCount++;
				comments.put(i++, gvc);
			}
		}
	}
	
public static long getUnixTime(String published_date) {
	Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(published_date);
	long unixTime = calendar.getTimeInMillis() / 1000;
	return unixTime;
}
	
public long  convertTimeToUnixTime(String time) {
	Calendar calendar = DatatypeConverter.parseDateTime(time);
	long unixTime = calendar.getTimeInMillis() / 1000;
	return unixTime;
}
public String buildHBaseVideoId(String video_id, long video_created_time) {
	return "youtube." + String.format("%04d", convertUnixTimeToThreadId(video_created_time)) + "." + video_created_time + "." + video_id;     
}

public long convertUnixTimeToThreadId(long unixTime) {
	Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    c.setTimeInMillis(unixTime*1000);
	return c.get(Calendar.HOUR_OF_DAY)*60 + c.get(Calendar.MINUTE);
}

}
