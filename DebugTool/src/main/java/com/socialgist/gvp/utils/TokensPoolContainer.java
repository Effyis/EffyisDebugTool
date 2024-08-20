package com.socialgist.gvp.utils;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import com.socialgist.gvp.utils.items.GvpTokenItem;
import com.socialgist.gvp.utils.items.GvpUtils;

public class TokensPoolContainer {

	// If the property TOKENS_POOL_CONTENT_ID exists in the GenericVideoProcessProperties table for a specific GVP, 
	// it will overwrite the value from the property file.	
	@Value("${tokens.pool.id:0}")
	public int tokens_pool_id;
	
	@Autowired
	HBaseContainer hbaseContainer;
	@Autowired
	MySQLContainer mySQLContainer;
	
	public ConcurrentHashMap <String, GvpTokenItem> tokens = new ConcurrentHashMap <String, GvpTokenItem>();
	Iterator<Entry<String, GvpTokenItem>> itTokens = null;

//	public LinkedHashMap<String, GvpTokenItem> tokensStats = new LinkedHashMap<String, GvpTokenItem>();
//	Iterator<Entry<String, GvpTokenItem>> itTokensStats = null;
	
	long prevTime = 0;
	long last_HB_monitor = 0;   // last HB from this controller to monitor
	
	public boolean isReady = false;
	
	Table hTokensTable; 
	
	public void init() {
	}
	
	public void init(int tokens_pool_id) {
		this.tokens_pool_id = tokens_pool_id;
	}
	
	//****************************************************************************************************	
	//***************************** Tokens Content *******************************************************	
	//****************************************************************************************************	
	
	private String buildHBaseIndex(String token) {
		return token + "." + GvpUtils.getPDT_midnight_sec();
	}

//	private String buildHBaseIndex_Ts(String token) {
//		return GvpUtils.getPDT_midnight_sec() + "." + token;
//	}
	
//	private String buildHBaseIndex_Gvp(int gvp_id) {
//		return "gvp." + gvp_id + "." + GvpUtils.getPDT_midnight_sec();
//	}
	
	public synchronized void incrementToken(int gvp_id, String token, long quota_used ) {
		hbaseContainer.hBase_incrementToken(buildHBaseIndex(token), quota_used);
	}

	public synchronized void setQuotaUsed(String token, long quota_used) {
		hbaseContainer.hBase_setQuotaUsed(buildHBaseIndex(token), quota_used);
	}
	
	public synchronized void setQuotaExceeded(String token ) {
		hbaseContainer.hBase_setQuotaExceededToken(buildHBaseIndex(token));
	}
	
	public synchronized long getQuotaUsedByToken(String token) {
		return hbaseContainer.hBase_getQuota(buildHBaseIndex(token));
	}

	public synchronized long getTokenDT(String token) {
		return GvpUtils.getPDT_midnight_sec();
	}
	
	public synchronized boolean isQuotaExceeded(String token) {
		return hbaseContainer.hBase_isQuotaExceededToken(buildHBaseIndex(token));
	}
	
//	public synchronized long getQuotaUsedByGvp(int gvp_id) {
//		return hbaseContainer.hBase_getQuota(buildHBaseIndex_Gvp(gvp_id));
//	}
	
	public synchronized void syncFromDB_Token(boolean all_tokens) {

		isReady = false;		
//		LinkedHashMap<String, GvpTokenItem> tokens = tokensContent;
		for (Entry<String, GvpTokenItem> entry : tokens.entrySet()) {
						entry.getValue().isActive = false;
		}
		String sql = "SELECT * FROM YouTubePoolClientKeys where isactive=true";
		if (!all_tokens) {
			sql = sql + " and type=" + tokens_pool_id;
		}
		SqlRowSet result = mySQLContainer.getJdbcTemplate().queryForRowSet(sql);
	    while (result.next()) {
			String id = result.getString("id");
			String clientkey = result.getString("clientkey");
	    	long total_quota = result.getInt("quota");
			if (tokens.containsKey(id)) {
				tokens.get(id).isActive  = true;
				tokens.get(id).token = clientkey;
				tokens.get(id).total_quota = total_quota;
			}
			else {
				GvpTokenItem dsr = new GvpTokenItem(id, clientkey, total_quota);
				tokens.put(dsr.id, dsr);
//				updateDB(dsr);
			}
	    }
		isReady = true;		
	}
/*	
	public synchronized void syncFromDB_Stats_Token(GvpProccess gvpProccess) {
		LinkedHashMap<String, GvpTokenItem> tokens = tokensStats;
		
		for (Entry<String, GvpTokenItem> entry : tokens.entrySet()) {
						entry.getValue().isActive = false;
		}
		String sql = "SELECT * FROM YouTubePoolClientKeys where isactive=true and type=" + gvpProccess.tokens_pool_stats_id;
		SqlRowSet result = mySQLContainer.getJdbcTemplate().queryForRowSet(sql);
	    while (result.next()) {
			String id = result.getString("id");
			String clientkey = result.getString("clientkey");
	    	long total_quota = result.getInt("quota");
			if (tokens.containsKey(id)) {
				tokens.get(id).isActive  = true;
				tokens.get(id).token = clientkey;
				tokens.get(id).total_quota = total_quota;
			}
			else {
				GvpTokenItem dsr = new GvpTokenItem(id, clientkey, total_quota);
				tokens.put(dsr.id, dsr);
//				updateDB(dsr);
			}
	    }
	}
*/	
//****************************************************************************************************	
//***************************** Tokens Content *******************************************************	
//****************************************************************************************************	


	public synchronized GvpTokenItem getTokenItem(String token)  {
		return hbaseContainer.hBase_getGvpTokenItem(buildHBaseIndex(token));
	}
	
	
/*	public synchronized GvpTokenItem getTokenItem(String token)  {
		  for (GvpTokenItem gvpToken : tokens.values()) {
		            if (gvpToken.token.equalsIgnoreCase(token)) { 
		            	gvpToken.used_quota = getQuotaUsedByToken(token);		            	
		            	gvpToken.used_quota_dt = GvpUtils.getPDT_midnight_sec();
		            	return gvpToken; 
		            }
	      }		
		  return null;	
		}
*/
	
/*	
	public synchronized GvpTokenItem getTokenWithMaxFreeQuota()  {
		
		GvpTokenItem tokenMax = null;
		for (GvpTokenItem gvpToken : tokens.values()) {
          	gvpToken.used_quota = getQuotaUsedByToken(gvpToken.token);		            	
          	gvpToken.used_quota_dt = GvpUtils.getPDT_midnight_sec();
           	if ((tokenMax==null)||(gvpToken.getFreeQuota() > tokenMax.getFreeQuota())) {
           			tokenMax = gvpToken; 
            }
		}		
		return tokenMax;	
	}	
*/	
	
	public synchronized GvpTokenItem getNextAvailable_TokenContent()  {

		long counter = 0; 
		
		while (counter < tokens.size()) {
			GvpTokenItem item = getNext_TokenContent();
			counter++;
			if (item != null) 
				return item;
		}
		return null;
	}
	
	public synchronized GvpTokenItem getNext_TokenContent()  {

		if ((itTokens == null) || (!itTokens.hasNext())) {
			itTokens = tokens.entrySet().iterator();
		}     
		if 	(!itTokens.hasNext()) return null;  // if no keywords	

		try {
			GvpTokenItem item = itTokens.next().getValue();
			
			if (!item.isActive) return null;
			if (isQuotaExceeded(item.token)) return null;
			
			long current_quota = getQuotaUsedByToken(item.token);
			if (current_quota < item.total_quota ) 
				return item;
			else 
				return null;
		}
		catch (ConcurrentModificationException e) {
			itTokens = null;
			System.out.println("getNext_TokenContent: " + e);
			return null;
		}
			
	};

//****************************************************************************************************	
//***************************** Tokens Stats *********************************************************	
//****************************************************************************************************	

	/*	
	public synchronized GvpTokenItem getNextAvailable_TokenStats()  {

		long counter = 0; 
		
		while (counter < 100) {
			GvpTokenItem item = getNext_TokenStats();
			counter++;
			if (item != null) return item;
		}
		System.out.println("TEST Token 2");
		return null;
	}
	
	
	public synchronized GvpTokenItem getNext_TokenStats()  {

		if ((itTokensStats == null) || (!itTokensStats.hasNext())) {
			itTokensStats = tokensStats.entrySet().iterator();
		}     
		if 	(!itTokensStats.hasNext()) return null;  // if no keywords	
		
		try {

			GvpTokenItem item = itTokensStats.next().getValue();
		
			if (!item.isActive) return null; 
		
			String hbase_index = "youtube." + item.token + "." + GvpUtils.getPDT_midnight_sec();   

			long current_quota = hbaseContainer.hBase_getQuota(hbase_index);
		
			if (current_quota < item.total_quota ) 
				return item;
			else 
				return null;
		}
		catch (ConcurrentModificationException e) {
			itTokensStats = null;
			System.out.println("getNext_TokenStats: " + e);
			return null;
		}
	};
	
	*/
	
}
