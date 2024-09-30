package com.socialgist.debugtool;
import java.io.IOException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.socialgist.debugtool.items.DtHtmlTable;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.StatsContainer;
import com.socialgist.debugtool.utils.items.GvpUtils;
import com.socialgist.debugtool.utils.items.GvpVideo;

import jakarta.servlet.http.HttpServletResponse;


@RestController
public class ProxyToolsController {

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
	
    @GetMapping("/yt_tokens")
 	public String yt_tokens() throws Exception {

   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"youtube_index\"><img src=\"socialgist_youtube.jpg\" alt=\"DT Youtube Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>Youtube Tokens</h2></td></tr></table>");
   	 	htmlTable.append("<p>");

   	 	htmlTable.append("<hr>");

   	 	htmlTable.append("<form action='/yt_assign_proxies' method='POST'>");
   		htmlTable.append("<input type=\"hidden\" id=\"video_id\"  name=\"video_id\"  value=\"aaa\">");
   		htmlTable.append("<button type=\"submit\">Assign Proxies</button>");
   	 	
    	DtHtmlTable tokensTable = mySQLUtilsContainer.get_tokens("id>0", 0, 10000, 1, "asc");   	 	
 		
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append(tokensTable.htmlTable);
   	 	htmlTable.append("<hr>");
   	 	
		return htmlTable.toString();
    }

    @PostMapping("/yt_assign_proxies")
    public void yt_assign_proxies(HttpServletResponse response) throws IOException {
    	assignYoturubeProxies();    	
        response.sendRedirect("/yt_tokens");
    }    
    
    public String assignYoturubeProxies() {

        	RestTemplate restTemplate = new RestTemplate();
        	String url = "http://proxy-api-client.k8s-prod.sgdctroy.net/v1/availableips/provider/blazing/zone/1/type/shared";
        	String response = restTemplate.getForObject(url, String.class);
        	List<String> list = Arrays.asList(response.split("\\n"));
        	// Convert the immutable list to a modifiable list
        	List<String> proxyList = new ArrayList<>(list);
        	
        	HashMap <String, TokenUseRecord> tokens = new HashMap<String, TokenUseRecord>();
        	
    		String sql = "SELECT * FROM portalsdb.YouTubePoolClientKeys order by proxy";
    		SqlRowSet result = mySQLUtilsContainer.jdbcTemplateObject.queryForRowSet(sql);
    	    while (result.next()) {
    	    	TokenUseRecord tr = new TokenUseRecord();  
    			tr.id = result.getString("id");
    			tr.type = result.getString("type");
    			tr.apiproject = result.getString("apiproject");
    			tr.token = result.getString("clientkey");
    			tr.total_quota = result.getInt("quota");
    			tr.proxy = result.getString("proxy");
    			tokens.put(tr.id, tr);
    		    if ((tr.proxy != null) && (GvpUtils.isValidProxy(tr.proxy)))  
    		    	proxyList.remove(tr.proxy);
    	    }

        	Collections.sort(proxyList);
        	Iterator<String> iterator = proxyList.iterator();
    	    
    	    for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
    	    	TokenUseRecord token = entry.getValue();
    		    if ((token.proxy != null) && (GvpUtils.isValidProxy(token.proxy)))
    		    	continue;
                if (iterator.hasNext()) {
                    String proxy = iterator.next();
                    token.proxy = proxy;                      
                    updateYotubeTokenProxy(token);
                }
    	    }	    
    	    
        return "";    
    }
    
	public synchronized void updateYotubeTokenProxy(TokenUseRecord token) {
		String sql = "UPDATE portalsdb.YouTubePoolClientKeys SET proxy=? where id=? and proxy is null";
		Object[] params = new Object[] { token.proxy, token.id };
		int[] types = new int[] { Types.VARCHAR, Types.BIGINT};
		mySQLUtilsContainer.jdbcTemplateObject.update( sql, params, types);
	}

	
	//**********************************************************************************************		
	//********************************* VK *********************************************************		
	//**********************************************************************************************		

    @GetMapping("/vk_tokens")
 	public String vk_tokens() throws Exception {
   	 	StringBuilder htmlTable = new StringBuilder();
   	 	htmlTable.append("<table border=0 cellspacing=0 cellpadding=5><tr style=\"vertical-align: top;\"><td>");
   	 	htmlTable.append("<a href=\"vk_index\"><img src=\"socialgist_vk.jpg\" alt=\"DT VK Home\" height=25></a>");
   	 	htmlTable.append("</td><td width=\"30\"></td><td><h2>VK Tokens</h2></td></tr></table>");
   	 	htmlTable.append("<p>");

   	 	htmlTable.append("<hr>");

   	 	htmlTable.append("<form action='/vk_assign_proxies' method='POST'>");
   		htmlTable.append("<input type=\"hidden\" id=\"video_id\"  name=\"video_id\"  value=\"aaa\">");
   		htmlTable.append("<button type=\"submit\">Assign Proxies</button>");
   	 	
    	DtHtmlTable tokensTable = mySQLUtilsContainer.get_vk_tokens("id>0", 0, 10000, 1, "asc");   	 	
 		
   	 	htmlTable.append("<hr>");
   	 	htmlTable.append(tokensTable.htmlTable);
   	 	htmlTable.append("<hr>");
   	 	
		return htmlTable.toString();
    }

    @PostMapping("/vk_assign_proxies")
    public void vk_assign_proxies(HttpServletResponse response) throws IOException {
    	assignVkProxies();    	
        response.sendRedirect("/vk_tokens");
    }    
	
    public String assignVkProxies() {

    	RestTemplate restTemplate = new RestTemplate();
    	String url = "http://proxy-api-client.k8s-prod.sgdctroy.net/v1/availableips/provider/blazing/zone/1/type/shared";
    	String response = restTemplate.getForObject(url, String.class);
    	List<String> list = Arrays.asList(response.split("\\n"));
    	// Convert the immutable list to a modifiable list
    	List<String> proxyList = new ArrayList<>(list);
    	
    	HashMap <String, TokenUseRecord> tokens = new HashMap<String, TokenUseRecord>();
    	
		String sql = "SELECT * FROM portalsdb.VkTokens order by proxy";
		SqlRowSet result = mySQLUtilsContainer.jdbcTemplateObject.queryForRowSet(sql);
	    while (result.next()) {
	    	TokenUseRecord tr = new TokenUseRecord();  
			tr.id = result.getString("id");
			tr.type = result.getString("projectid");
			tr.token = result.getString("token");
			tr.proxy = result.getString("proxy");
			tokens.put(tr.id, tr);
		    if ((tr.proxy != null) && (GvpUtils.isValidProxy(tr.proxy)))  
		    	proxyList.remove(tr.proxy);
	    }

    	Collections.sort(proxyList);
    	Iterator<String> iterator = proxyList.iterator();
	    
	    for (Entry<String, TokenUseRecord> entry : tokens.entrySet()) {
	    	TokenUseRecord token = entry.getValue();
		    if ((token.proxy != null) && (GvpUtils.isValidProxy(token.proxy)))
		    	continue;
            if (iterator.hasNext()) {
                String proxy = iterator.next();
                token.proxy = proxy;                      
                updateVkTokenProxy(token);
            }
	    }	    
    return "";    
    }
    
	public synchronized void updateVkTokenProxy(TokenUseRecord token) {
		String sql = "UPDATE portalsdb.VkTokens SET proxy=? where id=? and proxy is null";
		Object[] params = new Object[] { token.proxy, token.id };
		int[] types = new int[] { Types.VARCHAR, Types.BIGINT};
		mySQLUtilsContainer.jdbcTemplateObject.update( sql, params, types);
	}
    
	
	
    
}
