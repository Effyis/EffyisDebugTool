package com.socialgist.gvp.utils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.socialgist.gvp.utils.items.GvpChannel;
import com.socialgist.gvp.utils.items.GvpComment;
import com.socialgist.gvp.utils.items.GvpDictionaryItem;
import com.socialgist.gvp.utils.items.GvpServer;
import com.socialgist.gvp.utils.items.GvpTokenItem;
import com.socialgist.gvp.utils.items.GvpUtils;
import com.socialgist.gvp.utils.items.GvpVideo;
import com.socialgist.gvp.utils.items.GvpVideo.PremiumStatus;
import com.socialgist.gvp.utils.items.StatsList;
import com.socialgist.gvp.utils.items.StatsTimePeriod;

@Component
//@ManagedResource(objectName = "Effyis DBUtils:name=HBaseContainer")
public class HBaseContainer implements Closeable {

//*******  HBase Parameters *********************

//	boolean debug_hbase_update_off = false;

	@Value("${gvp.hbase.debug.readonly:false}") //
	boolean debug_readonly_mode; // Set a flag to indicate if hbase is in read only mode.    
	
//	#dev
//	#gvp.collector.hbase.table.name=gvp_v1_dev
//	#gvp.collector.hbase.subscription.table.name=video_subscriptions_v1_dev
//	#prod
//	gvp.collector.hbase.table.name=gvp_v1_prod
//	gvp.collector.hbase.subscription.table.name=video_subscriptions_v1_prod

	@Value("${gvp.hbase.table.video_gvp:hbase_dev:video_gvp}") //
	String video_gvp;
	@Value("${gvp.hbase.table.video_subscriptions:hbase_dev:video_subscriptions}")
	String video_subscriptions;
	@Value("${gvp.hbase.table.video_comments:hbase_dev:video_comments}")
	String video_comments;
	@Value("${gvp.hbase.table.video_tokens:hbase_dev:video_tokens}")
	String video_tokens;
	@Value("${gvp.hbase.table.video_dictionary:hbase_dev:video_dictionary}")
	String video_dictionary;
	
	@Value("${gvp.utils.hbase.table.video_comments:hbase_dev:debug_stats}")
	String debug_stats; // Old version of the table

// hbase user name	
	@Value("${gvp.hbase.username:}")
	String hbaseUsername;

	private static Logger LOGGER = LoggerFactory.getLogger(HBaseContainer.class);

	private Table hTable_VideoGvp;
	private Table hTable_VideoSubscribtions;
	private Table hTable_VideoComments;
	private Table hTable_VideoDictionary;
	private Table hTable_VideoTokens;
	private Table hTable_DebugStats;

	public Configuration config; // temporary

	@Autowired
//	GvpUtils gvpUtils;

	
	public void init() throws IOException {
		try {
			Configuration config = HBaseConfiguration.create();
			System.setProperty("HADOOP_USER_NAME", hbaseUsername);
			org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
			if (!video_gvp.isEmpty()) {
				hTable_VideoGvp = connection.getTable(TableName.valueOf(video_gvp));
			}
			if (!video_subscriptions.isEmpty()) {
				hTable_VideoSubscribtions = connection.getTable(TableName.valueOf(video_subscriptions));
			}
			if (!video_comments.isEmpty()) {
				hTable_VideoComments = connection.getTable(TableName.valueOf(video_comments));
			}
			if (!video_tokens.isEmpty()) {
				hTable_VideoTokens = connection.getTable(TableName.valueOf(video_tokens));
			}
			if (!video_dictionary.isEmpty()) {
				hTable_VideoDictionary = connection.getTable(TableName.valueOf(video_dictionary));
			}
			if (!debug_stats.isEmpty()) {
				hTable_DebugStats = connection.getTable(TableName.valueOf(debug_stats));
			}
			LOGGER.info("*** HBaseContainer was initialized succefully ***");
		} catch (Exception e) {
			LOGGER.error("*** HBaseContainer initialization error ***", e.getMessage());
		}
	}

	@Override
	public void close() throws IOException {
	}

	public synchronized void hBase_putStats(String hbase_index, StatsList stats) {
		LOGGER.debug("HBase:hBase_putStats: " + hbase_index + " , stats: " + stats);
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(hbase_index));
	    for (StatsTimePeriod stp : StatsTimePeriod.values()) {
	    	TreeMap<String, String> map = stats.getMapByPeriod(stp);
	    	  for (Map.Entry<String, String> entry : map.entrySet()) {
	    			put.addColumn(Bytes.toBytes(stp.getCfName()), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
//	                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
            }	    	
        }		
		try {
			hTable_DebugStats.put(put);
		} catch (IOException e) {}
	}
 
	public synchronized void hBase_incrementTimeStats(String hbase_index, long stats) {
		LOGGER.debug("HBase:hBase_incrementStats: " + hbase_index + " , stats: " + stats);
		if (debug_readonly_mode) return;
//        long currentUnixTime = Instant.now().getEpochSecond();
		Increment increment = new Increment(Bytes.toBytes(hbase_index));
		for (StatsTimePeriod stp : StatsTimePeriod.values()) {
	        long adjustedTime = GvpUtils.adjustCurrentUnixTime(stp.getPeriodInSeconds());
	        String cfName = stp.getCfName();
			increment.addColumn(Bytes.toBytes(cfName), Bytes.toBytes("ut" + adjustedTime), stats);
        }
	    try {
	    	hTable_DebugStats.increment(increment);
		} catch (IOException e) {}
	}
	
	public synchronized Result hBase_getStats(String hbase_index) {
			Result result = null;
			LOGGER.debug("HBase:hBase_getStats   " + hbase_index);
			Get get=new Get(Bytes.toBytes(hbase_index));
			try {
				result = hTable_DebugStats.get(get);
//				if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
			} catch (IOException e) {}
			return result;
	}
	
	//***********************************************
	//*******  Get Scanners  *********************
	//***********************************************
	public ResultScanner getScanner_TableVideoTokens(Scan scan) throws IOException{
		return hTable_VideoTokens.getScanner(scan);
	}
	public ResultScanner getScanner_TableVideoDictionary(Scan scan) throws IOException{
		return hTable_VideoDictionary.getScanner(scan);
	}
	public ResultScanner getScanner_TableVideoComments(Scan scan) throws IOException{
		return hTable_VideoComments.getScanner(scan);
	}
	public ResultScanner getScanner_TableVideoSubscribtions(Scan scan) throws IOException{
		return hTable_VideoSubscribtions.getScanner(scan);
	}
	public ResultScanner getScanner_TableVideoGvp(Scan scan) throws IOException{
		return hTable_VideoGvp.getScanner(scan);
	}
	public ResultScanner getScanner_TableDebugStats(Scan scan) throws IOException{
		return hTable_DebugStats.getScanner(scan);
	}
	
//***********************************************
//*******  Dictionary Table *********************
//***********************************************
	protected synchronized Result hBase_getDictionaryItem(String hbase_prefix, String keyword) {

		LOGGER.debug("HBase:hBase_getDictionaryItem   " + hbase_prefix + "." +keyword);
		
		Result result = null;
		String hbase_id = hbase_prefix + "." + GvpUtils.getMD5(keyword);
		Get get = new Get(Bytes.toBytes(hbase_id));
		try {
			result = hTable_VideoDictionary.get(get);
		} catch (IOException e) {}
		return result;
	}
	
	protected synchronized GvpDictionaryItem hBase_getDictionaryItemObject(String hbase_id)  { 
		Result result = null;
		GvpDictionaryItem gdi = null;
		LOGGER.debug("HBase:hBase_getDictionaryItem   " + hbase_id);
		
		Get get=new Get(Bytes.toBytes(hbase_id));
		try {
			result = hTable_VideoDictionary.get(get);
			if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
				gdi = new GvpDictionaryItem(result);
			}
		} catch (IOException e) {}
		return gdi;
	}

	protected synchronized GvpDictionaryItem hBase_updateDictionaryItemObject(GvpDictionaryItem gdi)  { 
		Result result = null;
		LOGGER.debug("HBase:hBase_updateDictionaryItemObject   " + gdi.keyword);
		
		Get get=new Get(Bytes.toBytes(gdi.hbase_index));
		try {
			result = hTable_VideoDictionary.get(get);
			if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
				gdi.syncFromHBase(result);
			}
		} catch (IOException e) {}
		return null;
	}

	protected synchronized long hBase_getDictionaryItem_ScheduledTime(String hbase_id) {
		LOGGER.debug("HBase:hBase_getDictionaryItem_ScheduledTime   " + hbase_id);
		Result result = null;
		long scheduled_ts = 0;
		Get get = new Get(Bytes.toBytes(hbase_id));
		try {
			result = hTable_VideoDictionary.get(get);
			byte[] hb_scheduled_ts = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("scheduled_ts"));
			scheduled_ts = (hb_scheduled_ts != null) ? Bytes.toLong(hb_scheduled_ts) : 0;
		} catch (IOException e) {}
		return scheduled_ts;
	}

	protected synchronized void hBase_updateDictionaryItem(String hbase_prefix, String keyword, String pubBefore,
			long pubBefore_ts, long search_timeframe_sec, long scheduled_ts) {
		LOGGER.debug("HBase:hBase_updateDictionaryItem   " + keyword );
		if (debug_readonly_mode) return;
		String hbase_id = hbase_prefix + "." + GvpUtils.getMD5(keyword);
		Put put = new Put(Bytes.toBytes(hbase_id));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("keyword"), Bytes.toBytes(keyword));

		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_pubBefore"), Bytes.toBytes(pubBefore));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_pubBefore_sec"), Bytes.toBytes(pubBefore_ts));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"),
				Bytes.toBytes(System.currentTimeMillis() / 1000));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("prev_search_timeframe_sec"),
				Bytes.toBytes(search_timeframe_sec));

		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("scheduled_ts"), Bytes.toBytes(scheduled_ts));

		try {
			hTable_VideoDictionary.put(put);
		} catch (IOException e) {}
	}

	// ***********************************************
	// ******* Tokens Table *********************
	// ***********************************************
	protected  synchronized long hBase_getQuota(String hbase_id) {
		LOGGER.debug("HBase:hBase_getQuota: " + hbase_id);
		Result result = null;
		long quota_used = 0;
		Get get = new Get(Bytes.toBytes(hbase_id));
		try {
			result = hTable_VideoTokens.get(get);
			byte[] hb_quota_used = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"));
			quota_used = (hb_quota_used != null) ? Bytes.toLong(hb_quota_used) : 0;
		} catch (IOException e) {}
		return quota_used;
	}
	
	protected synchronized void hBase_setQuotaUsed(String hbase_index, long quota_used ) {
			LOGGER.debug("HBase:hBase_setQuotaUsed: " + hbase_index);
			if (debug_readonly_mode) return;
			long lastuse = System.currentTimeMillis()/1000L;
			Put put = new Put(Bytes.toBytes(hbase_index));
			put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"), Bytes.toBytes(lastuse));
			put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"), Bytes.toBytes(quota_used));
		    try {
		    	hTable_VideoTokens.put(put);
			} catch (IOException e) {}
			return;
		}
		

	protected  synchronized boolean hBase_isQuotaExceededToken(String hbase_id) {
		LOGGER.debug("HBase:hBase_getQuotaExceededToken: " + hbase_id);
		Result result = null;
		boolean quota_exceeded = false;
				
		Get get = new Get(Bytes.toBytes(hbase_id));
		try {
			result = hTable_VideoTokens.get(get);
			byte[] hb_quota_exceeded = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"));
			quota_exceeded = ((hb_quota_exceeded != null) && (Bytes.toLong(hb_quota_exceeded)==1))  ? true : false;
		} catch (IOException e) {}
		return quota_exceeded;
	}
	
	
	protected synchronized long hBase_incrementToken(String hbase_index, long quota_used) {
		LOGGER.debug("HBase:hBase_incrementToken: " + hbase_index + " quota: " + quota_used);
		if (debug_readonly_mode) return 0;
		long currentQuotaUsed = 0;
		long lastuse = System.currentTimeMillis()/1000L;
		Put put = new Put(Bytes.toBytes(hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"), Bytes.toBytes(lastuse));
		Increment increment = new Increment(Bytes.toBytes(hbase_index));
	    increment.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"), quota_used);
	    try {
	    	hTable_VideoTokens.put(put);
	    	Result result = hTable_VideoTokens.increment(increment);
			currentQuotaUsed = Bytes.toLong(result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used")));			
		} catch (IOException e) {}
		return currentQuotaUsed;
	}

	protected synchronized void hBase_setQuotaExceededToken(String hbase_index) {
		LOGGER.debug("HBase:hBase_setQuotaExceededToken: " + hbase_index);
		if (debug_readonly_mode) return;
		long lastuse = System.currentTimeMillis()/1000L;
		Put put = new Put(Bytes.toBytes(hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"), Bytes.toBytes(lastuse));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"), Bytes.toBytes(1L));
	    try {
	    	hTable_VideoTokens.put(put);
		} catch (IOException e) {}
		return;
	}

	public synchronized GvpTokenItem hBase_getGvpTokenItem(String hbase_index) {
		LOGGER.debug("HBase:hBase_getGvpTokenItem: " + hbase_index);
		Result result = null;
		GvpTokenItem token = null;
		Get get = new Get(Bytes.toBytes(hbase_index));
		try {
			result = hTable_VideoTokens.get(get);
			if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
				token = new GvpTokenItem(result);
			}
		} catch (IOException e) {}
		return token;
	}
	
	
	
/*	public synchronized Result hBase_updateTokenProcess(GvpTokenItem item, int gvp_id) {

		Result result = null;
		LOGGER.debug("HBase:hBase_updateTokenProcess");

		String hbase_index = "youtube.gvp." +  gvp_id + "." + GvpUtils.getPDT_midnight_sec();
		
		long lastuse = System.currentTimeMillis()/1000L;
		Put put = new Put(Bytes.toBytes(hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"), Bytes.toBytes(lastuse));
		
		Increment increment = new Increment(Bytes.toBytes(hbase_index));
	    increment.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"), item.used_quota);
	    try {
//	    	hTable_VideoTokens.put(put);
			hTable_VideoTokens.increment(increment);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
*/	
	
// ***********************************************
// ******* Servers *********************
// ***********************************************

	public synchronized void hBase_putNewServer(String hbase_index, String token) {
		LOGGER.debug("HBase:hBase_putNewServer   " + hbase_index + "    " + token);
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"),
				Bytes.toBytes(System.currentTimeMillis() / 1000));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("token"), Bytes.toBytes(token));
		try {
			hTable_VideoTokens.put(put);
		} catch (IOException e) {}
	}

	public synchronized GvpServer hBase_getGvpServer(String hbase_index) {
		LOGGER.debug("HBase:hBase_getGvpServer   " + hbase_index);
		GvpServer gs = null;
		Result result = null;
		Get get = new Get(Bytes.toBytes(hbase_index));
		try {
			result = hTable_VideoTokens.get(get);
			if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
				gs = new GvpServer(result);
			}
		} catch (IOException e) {}
		return gs;
	}
	
//***********************************************
//**************  GVP Table *********************
//***********************************************
	public synchronized Result hBase_getGvpRecord(String hbase_media_id) {
		LOGGER.debug("HBase:hBase_getGvp   " + hbase_media_id);
		Result result = null;
		Get get = new Get(Bytes.toBytes(hbase_media_id));
		try {
			result = hTable_VideoGvp.get(get);
		} catch (IOException e) {}
		return result;
	}

	public synchronized GvpVideo hBase_getGvpVideo(String hbase_video_id) {
		GvpVideo ghr = null;
		LOGGER.debug("HBase:hBase_get_GvpHbaseRecord   " + hbase_video_id);
		Result result = hBase_getGvpRecord(hbase_video_id);
		if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
			ghr = new GvpVideo(result);
		}
		return ghr;
	}
	
	public synchronized GvpChannel hBase_getGvpChannel(String hbase_channel_id) {
		GvpChannel ghc = null;
		Result result = hBase_getGvpRecord(hbase_channel_id);
		if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
			ghc = new GvpChannel(result);
		}
		return ghc;
	}

	public synchronized Result hBase_getRefChannel(String channel_id) {
		String hbase_channel_ref_id = "ytc_ref." + channel_id;
		return hBase_getGvpRecord(hbase_channel_ref_id); // hBase_get(hbase_id, false);
	}
	
	public synchronized Result hBase_getVideoComments(String hbase_media_id) {
		LOGGER.debug("HBase:hBase_getVideoComments   " + hbase_media_id);
		Result result = null;
		Get get = new Get(Bytes.toBytes(hbase_media_id));
		try {
			result = hTable_VideoComments.get(get);
		} catch (IOException e) {}
		return result;
	}
	public synchronized void hBase_putNewVideoCategory(String hbase_index, String title) {
		LOGGER.debug("HBase:putNewVideoCategory   " + hbase_index + "    " + title);
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"),
				Bytes.toBytes(System.currentTimeMillis() / 1000));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("title"), Bytes.toBytes(title));
		try {
			hTable_VideoGvp.put(put);
		} catch (IOException e) {}
	}

	public synchronized void hBase_putVideoComments(String comment_hbase_index, String video_hbase_index)
			throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		LOGGER.debug("HBase:hBase_putVideoComments     " + comment_hbase_index + "   " + video_hbase_index);
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(comment_hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("collected"),
				Bytes.toBytes(System.currentTimeMillis() / 1000));
		if (video_hbase_index != null) {
			put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("video_hbase_index"),
					Bytes.toBytes(video_hbase_index));
		}
		try {
			hTable_VideoComments.put(put);
		} catch (IOException e) {}
		;
	}

	public synchronized void hBase_putNewVideo(GvpVideo ghp)
			throws RetriesExhaustedWithDetailsException, InterruptedIOException {
		LOGGER.debug("HBase:hBase_putNew");
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(ghp.hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("created_ts"), Bytes.toBytes(ghp.created_ts));
//		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("guid"), Bytes.toBytes(ghp.index_GUID));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastComment_ts"), Bytes.toBytes(ghp.lastComment_ts));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("commentsCount_int"), Bytes.toBytes(0L));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastCheck_ts"), Bytes.toBytes(0L));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("deleted"), Bytes.toBytes(0));
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("channel_id"), Bytes.toBytes(ghp.channel_id));

//		if (ghp.new_video_MD5 != null) {
//			put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("md5"), Bytes.toBytes(ghp.new_video_MD5));
//		}
		try {
				hTable_VideoGvp.put(put);
		} catch (IOException e) {}
	}

	public synchronized GvpChannel hBase_putNewChannel(String hbase_channel_id, String hbase_channel_ref_id, String playlist_id, long created_ts) 
			throws RetriesExhaustedWithDetailsException, InterruptedIOException {
	  	LOGGER.debug("HBase:hBase_putNewChannel");		  
		if (debug_readonly_mode) return new GvpChannel(hbase_channel_id, playlist_id);

		Put put1 = new Put(Bytes.toBytes(hbase_channel_id));
		put1.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("created_ts"),Bytes.toBytes(created_ts));
		put1.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("playlist_id"),Bytes.toBytes(playlist_id));
		put1.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("lastCheck_ts"),Bytes.toBytes(0L));
		put1.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("deleted"),Bytes.toBytes(0L));
		try {
			hTable_VideoGvp.put(put1);
		}
		catch (IOException e) { return null; }
		
		Put put2 = new Put(Bytes.toBytes(hbase_channel_ref_id));
		put2.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("hbase_channel_id"),Bytes.toBytes(hbase_channel_id));
		put2.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("created_ts"),Bytes.toBytes(created_ts));
		try {
			hTable_VideoGvp.put(put2);
		}
		catch (IOException e) {	return null; }
		
		return new GvpChannel(hbase_channel_id, playlist_id);
	}

	public synchronized void hBase_updateChannel(GvpChannel ghc)	 {
	  	LOGGER.debug("HBase:hBase_updateChannel");		  
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(ghc.hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("lastCheck_ts"),Bytes.toBytes(System.currentTimeMillis()/1000));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("deleted"),Bytes.toBytes(ghc.deleted));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("scheduled_ts"),Bytes.toBytes(ghc.new_scheduled_ts));
		try {
			hTable_VideoGvp.put(put);
		} 
		catch (IOException e) {};
	}

	public synchronized void hBase_updateGvpVideo(GvpVideo ghp)  {
	  	LOGGER.debug("HBase:hBase_updateGhp");		  
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(ghp.hbase_index));
    	if ((ghp.new_lastComment_ts > ghp.lastComment_ts)) {  // if there are new comments 
    		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("lastComment_ts"),Bytes.toBytes(ghp.new_lastComment_ts));
    	}	    		
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("commentsCount_int"),Bytes.toBytes(ghp.new_commentsCount));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("lastCheck_ts"),Bytes.toBytes(System.currentTimeMillis()/1000));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("commentsSent_int"),Bytes.toBytes(ghp.commentsSent + ghp.new_commentsSent));
		try {
			hTable_VideoGvp.put(put);
		} 
		catch (IOException e) {};
	}

	public void hBase_updateLastCheck(GvpVideo ghp) {
	  	LOGGER.debug("HBase:hBase_updateLastCheck");		  
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(ghp.hbase_index));
		put.addColumn(Bytes.toBytes("cfStatus"),Bytes.toBytes("lastCheck_ts"),Bytes.toBytes(System.currentTimeMillis()/1000));
		try {
			hTable_VideoGvp.put(put);
		} 
		catch (IOException e) {};
	}
	
	//***********************************************
	//*******  Check MEDIA For Existence   **********
	//***********************************************
	
	public GvpChannel checkChannelForExistence(String channel_id) {
		LOGGER.debug("HBase:checkChannelForExistence: " + channel_id);
		GvpChannel ghc = null; 
		String hbase_channel_ref_id = "ytc_ref." + channel_id;
		Result result = hBase_getGvpRecord(hbase_channel_ref_id); //ro.hBase_get(hbase_channel_ref_id, false);
		if ((result != null) && !result.isEmpty()) { // CHANNEL FOUND IN HBASE
			byte[] hbc_id  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("hbase_channel_id"));
			String hbase_channel_id = (hbc_id!=null)?Bytes.toString(hbc_id):"notfound";
			ghc = hBase_getGvpChannel(hbase_channel_id);
		 }
		 return ghc;
	}
	
	public GvpVideo checkVideoForExistence(String video_id) {
		LOGGER.debug("HBase:checkVideoForExistence: " + video_id);
		GvpVideo ghr = null;
		String hbase_video_ref_id = video_id + "|";
		Result result = hBase_getVideoComments(hbase_video_ref_id);
		if ((result != null) && !result.isEmpty()) { // VIDEO FOUND IN HBASE
				byte[] hbv_id  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("video_hbase_index"));
				String video_hbase_index = (hbv_id!=null)?Bytes.toString(hbv_id):"notfound";
				ghr = hBase_getGvpVideo(video_hbase_index);
		}
		return ghr;
	}

	public GvpComment checkCommentForExistence(String video_id, String comment_id) {
		LOGGER.debug("HBase:checkCommentForExistence: " + video_id  + "|" + comment_id);
		GvpComment gvc = null;
		String hbase_video_ref_id = video_id + "|" + comment_id;
		Result result = hBase_getVideoComments(hbase_video_ref_id);
		if ((result != null) && !result.isEmpty()) { // COMMENT FOUND IN HBASE
			gvc = new GvpComment(result);
		}
		return gvc;
	}
	
	//****************************************************************
	//**************  Premium subscription Table *********************
	//****************************************************************
	
	public synchronized void hBase_putSubscribtions(String datastream_id, String media_id, String json )	 {
		LOGGER.debug("HBase:hBase_putSubscribtions: " + media_id);
		if (debug_readonly_mode) return;
		Put put = new Put(Bytes.toBytes(media_id));
		long lastcheck = System.currentTimeMillis()/1000L;
		put.addColumn(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastcheck"), Bytes.toBytes(lastcheck));
		put.addColumn(Bytes.toBytes("cfDatastreams"), Bytes.toBytes(datastream_id), Bytes.toBytes(json));
		try {
			hTable_VideoSubscribtions.put(put);
		} catch (IOException e) {}
	}
	
	public synchronized Result hBase_getSubscrption(String hbase_media_id) {
		LOGGER.debug("HBase:hBase_getSubscrption " + hbase_media_id);
		Result result = null;
		Get get = new Get(Bytes.toBytes(hbase_media_id));
		try {
			result = hTable_VideoSubscribtions.get(get);
		} catch (IOException e) {}
		return result;
	}
	
	public boolean checkSubscribtionForExistence(String media_id) {
		LOGGER.debug("HBase:checkSubscribtionForExistence: " + media_id);
		Get get = new Get(Bytes.toBytes(media_id));
		try {
			Result result = hTable_VideoSubscribtions.get(get);
			if ((result != null) && !result.isEmpty()) { // CHANNEL FOUND IN HBASE
				return true;
			}
		} catch (IOException e) {}
		return false;
	}

	public synchronized long hBase_getLastCheckSubscription(String media_id) {
		LOGGER.debug("HBase:hBase_getLastCheckSubscription: " + media_id);
		long lastcheck = 0;
		Get get = new Get(Bytes.toBytes(media_id));
		try {
			Result result = hTable_VideoSubscribtions.get(get);
			if ((result != null) && !result.isEmpty()) { // VIDEO FOUND IN HBASE
				byte[] hb_lastcheck = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastcheck"));
				lastcheck = (hb_lastcheck != null) ? Bytes.toLong(hb_lastcheck) : 0;
			}	
		} catch (IOException e) {}
		return lastcheck;
	}
	
	public synchronized void hBase_readVideoSubscription(GvpVideo ghr) {
		LOGGER.debug("HBase:hBase_getLastCheckSubscription: " + ghr.video_id);
		Map<String, String> retrievedHashMap = new HashMap<>();
		ghr.premiumStatus = PremiumStatus.NO_PREMIUM;
		try {
			// Check video
			Get get = new Get(Bytes.toBytes(ghr.video_id));
			Result result = hTable_VideoSubscribtions.get(get);
			if ((result != null) && !result.isEmpty()) { // SUBSCRIPTION FOUND IN HBASE
				ghr.premiumStatus = PremiumStatus.PREMIUM;
				// Extract values from the Result instance and populate the HashMap
				for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes("cfDatastreams")).keySet()) {
					byte[] valueBytes = result.getValue(Bytes.toBytes("cfDatastreams"), qualifier);
					String value = Bytes.toString(valueBytes);
					String qualifierStr = Bytes.toString(qualifier);
					retrievedHashMap.put(qualifierStr, value);
				}
			}
			// Check channel
			get = new Get(Bytes.toBytes(ghr.channel_id));
			result = hTable_VideoSubscribtions.get(get);
			if ((result != null) && !result.isEmpty()) { // SUBSCRIPTION FOUND IN HBASE
				ghr.premiumStatus = PremiumStatus.PREMIUM;
				// Extract values from the Result instance and populate the HashMap
				for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes("cfDatastreams")).keySet()) {
					byte[] valueBytes = result.getValue(Bytes.toBytes("cfDatastreams"), qualifier);
					String value = Bytes.toString(valueBytes);
					String qualifierStr = Bytes.toString(qualifier);
					retrievedHashMap.put(qualifierStr, value);
				}
			}
			ghr.outputsMap = retrievedHashMap;
		} catch (IOException e) {}
	}
	
}
