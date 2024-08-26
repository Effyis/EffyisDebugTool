package com.socialgist.debugtool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.socialgist.debugtool.items.DtHtmlTable;
import com.socialgist.debugtool.model.Cell;
import com.socialgist.debugtool.model.JsonData;
import com.socialgist.debugtool.model.Row;
import com.socialgist.gvp.utils.HBaseContainer;
import com.socialgist.gvp.utils.MySQLContainer;
import com.socialgist.gvp.utils.items.PremiumSubscription;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.HashMap;


@Component
public class MySQLUtilsContainer  {

	@Autowired
	MySQLContainer mySQLContainer;
	@Autowired
	HBaseContainer hbaseContainer;
	
	
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	JdbcTemplate jdbcTemplateObject;

	private static Logger LOGGER = LoggerFactory.getLogger(MySQLUtilsContainer.class);
	
	public void init() {
		jdbcTemplateObject =  mySQLContainer.getJdbcTemplate();
	}
	
	public HashMap<String, PremiumSubscription> readRules(int limit, int offset) {
		HashMap<String, PremiumSubscription> premiumSubscriptions = new HashMap<>();

		String sql = " SELECT t1.id, t1.rule_name, t1.youtube_id, t1.last_start, t1.playlist_id, t1.type, "
				+ " t1.hbase_index, t1.stream_id, t1.recheck_hours, t2.topic "
				+ " FROM youtube_rules t1, stream t2 WHERE t1.stream_id=t2.id LIMIT " + limit + " OFFSET " + offset;
		SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
		while (result.next()) {
			PremiumSubscription ps = new PremiumSubscription();
			ps.setData(result, 0	);
			premiumSubscriptions.put(ps.id + ps.rule_name + result.getString("youtubeid"), ps);
			LOGGER.info("{}", ps.id + ps.rule_name + result.getString("youtubeid"));
		}// while
		return premiumSubscriptions;
	}

	
    // Method to build HTML table
    public DtHtmlTable buildHtmlTableFromQuery(SqlRowSet result) {

    	int columnsCount = 0;
    	int rowCount = 0;
    	
    	StringBuilder htmlTable = new StringBuilder("<table border=1 cellspacing=0 cellpadding=5>");
	        // Get column names
    		SqlRowSetMetaData metaData = result.getMetaData();
	        String[] columnNames = metaData.getColumnNames();
    		 
	        // Add header row
	        htmlTable.append("<tr>");
	        for (String columnName : columnNames) {
	        	columnsCount++;
//	            htmlTable.append("<th>").append(columnName).append("</th>");
	            htmlTable.append("<th>").append(metaData.getColumnLabel(columnsCount)).append("</th>");
	        }
	        htmlTable.append("</tr>");
	        // Add data rows
	        while (result.next()) {
	            htmlTable.append("<tr>");
	            for (String columnName : columnNames) {
	                htmlTable.append("<td>").append(result.getString(columnName)).append("</td>");
	            }
	            htmlTable.append("</tr>");
	        	rowCount++;
	        }
	        htmlTable.append("</table>");
	        return new DtHtmlTable(htmlTable.toString(), columnsCount, rowCount);
	    }

    	public String buildCoogleChartTableFromQuery(SqlRowSet result) {

            JsonData jsonData = new JsonData();
            jsonData.buildCollections();

            SqlRowSetMetaData metaData = result.getMetaData();
            int columnCount = metaData.getColumnCount();

            String[] columnNames = new String[columnCount];
            int[] columnTypes = new int[columnCount];
            
//	   	 	jsonData.addColumn("x", "string");
            
         // Fetch column names and types
            for (int i = 1; i <= columnCount; i++) {
            	columnNames[i - 1] = metaData.getColumnName(i);
                columnTypes[i - 1] = metaData.getColumnType(i);
                
                String columnName = metaData.getColumnName(i);
                String columnType = getCoogleChartType(metaData.getColumnType(i));
   		   	 	jsonData.addColumn(columnName, columnType);
            }            
            
	        // Add data rows
	        while (result.next()) {
//	            Row row = jsonData.addRow(0L, "");
	            Row row = jsonData.addRow();
	            for (int i = 1; i <= columnCount; i++) {
	                Object value = getSqlRowSetValue(result, i);
	                row.getC().add(new Cell(value));
	            }
	        }
	        
	        
	        ObjectMapper objectMapper = new ObjectMapper();
	        String jsonMessage ="{}";
	        try {
	           jsonMessage = objectMapper.writeValueAsString(jsonData);
	           System.out.println(jsonMessage);
	        } catch (JsonProcessingException e) {
	            e.printStackTrace();
	        }    	
	        return jsonMessage;
    	}
    
    	public String getCoogleChartType(int sqlType) {

    		String result = "";
    		
    		  switch (sqlType) {
              case java.sql.Types.VARCHAR:
            	  result = "string";
                  break;
              case  java.sql.Types.BIGINT:                  
              case  java.sql.Types.NUMERIC:
              case  java.sql.Types.INTEGER:
              case java.sql.Types.FLOAT:
              case java.sql.Types.DOUBLE:            	  
            	  result = "number";
                  break;
              case java.sql.Types.BIT:
              case java.sql.Types.BOOLEAN:
            	  result = "boolean";
                  break;
              case java.sql.Types.TIME:
              case java.sql.Types.DATE:
            	  result = "string";
//            	  result = "date";
                  break;
              case java.sql.Types.TIMESTAMP:
            	  result = "string";
//            	  result = "datetime";
                  break;
              default:
                  System.out.println("Invalid SQL type: " + sqlType);
          }
			return result;    		
    	}
    	
    	
    	public Object getSqlRowSetValue(SqlRowSet result, int columnIndex) {

    		Object value = null;
    		
    		// Get the SQL type of the column (you can also use metadata if needed)
    		int sqlType = result.getMetaData().getColumnType(columnIndex);
        
    		// Use a switch statement to handle different SQL types
    		switch (sqlType) {
            	case java.sql.Types.VARCHAR:
            	case java.sql.Types.CHAR:
            		value = result.getString(columnIndex);
            		break;
            	case java.sql.Types.INTEGER:
            	case java.sql.Types.SMALLINT:
            		value = result.getInt(columnIndex);
                break;
            case java.sql.Types.BIGINT:
            	value = result.getLong(columnIndex);
                break;
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
            	value = result.getBoolean(columnIndex);
                break;
            case java.sql.Types.DATE:
        		value = result.getString(columnIndex);
//            	value = result.getDate(columnIndex);
                break;
            case java.sql.Types.TIME:
        		value = result.getString(columnIndex);
//            	value = result.getTime(columnIndex);
                break;
            case java.sql.Types.TIMESTAMP:
        		value = result.getString(columnIndex);
//            	value = result.getTimestamp(columnIndex);
                break;
            default:
                System.out.println("Invalid SQL type: " + sqlType);
            	value = null;
    		}
    		return value;
        }    	
    	
		public String most_quota_used(String limit) {

			String sql = " SELECT id, rule_name, rule_url, quota_used  "
					+ " FROM portalsdb.youtube_rules  "
					+ " order by quota_used desc  "
					+ " limit " + limit;			
			SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
			
//			return buildHtmlTableFromQuery(result);
			return buildCoogleChartTableFromQuery(result);
		}
		
		public String rules_started() {

			String sql = " SELECT *  "
					+ " FROM portalsdb.youtube_rules  "
					+ " where status like '%Started%' ";			
			SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
			
			return buildCoogleChartTableFromQuery(result);
//			return buildHtmlTableFromQuery(result);
		}

		public DtHtmlTable get_subscriptions(String where, int offset, int limit, int order_by, String sort) {
			
			String sql = " SELECT "
					+ " id as id, "
					+ " stream_id AS stream, "	
					+ " IF(LENGTH(rule_name) > 30, "
					+ "       CONCAT(SUBSTRING(rule_name, 1, 27), '...'), "
					+ "       rule_name) AS rule_name, "
					+ " youtube_id,  " 
					+ " CASE WHEN type = 0 THEN CONCAT('<a target=''_blank'' href=''/video_list?video_id=', youtube_id, '''>debug</a>')"
					+ " WHEN type = 1 THEN CONCAT('<a target=''_blank'' href=''/channel_list?channel_id=', youtube_id, '''>debug</a>') END AS debug,  "
					+ " CASE WHEN type = 0 THEN CONCAT('<a target=''_blank'' href=''/api_video?video_id=', youtube_id, '''>api</a>')"
					+ " WHEN type = 1 THEN CONCAT('<a target=''_blank'' href=''/api_channel?channel_id=', youtube_id, '''>api</a>') END AS api,  "
					+ " CASE WHEN type = 0 THEN CONCAT('<a target=''_blank'' href=''https://www.youtube.com/watch?v=', youtube_id, '''>url</a>')"
					+ " WHEN type = 1 THEN CONCAT('<a target=''_blank'' href=''https://www.youtube.com/channel/', youtube_id, '''>url</a>') END AS url,  "
					+ " CASE WHEN type = 0 THEN 'video' WHEN type = 1 THEN 'channel' END AS type,  "
		    		+ " FROM_UNIXTIME(last_start) as last_start, FROM_UNIXTIME(last_start_scheduled) as scheduled,   " 
		    		+ " active, date_created   "
		    		+ " FROM portalsdb.youtube_rules   "
					+ " where " + where 
					+ " order by " + order_by + " " + sort 
					+ " limit " + limit
					+ " offset " + offset;
			SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
			
			return buildHtmlTableFromQuery(result);
			
		}
		
		
		public String premium_status() {

			String sql = " SELECT CONVERT(last_start_scheduled, CHAR) as laststartscheduled, DATE_FORMAT(FROM_UNIXTIME(last_start_scheduled), '%Y-%m-%d %H:%i:%s') AS laststartscheduled_ts, count(*)  "
					+ " FROM portalsdb.youtube_rules  "
					+ " where active=1  "
					+ " group by last_start_scheduled  "
					+ " order by 2 desc ";			
			SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
			
			return buildCoogleChartTableFromQuery(result);
		}		
		
		public SqlRowSet db_tokens(String type) {
			String sql = " SELECT * "
				+ " FROM portalsdb.YouTubePoolClientKeys  ";
			
			 if (type != null && type.isBlank()) {
				 sql =  sql + " where type=" + type;
			 }
			SqlRowSet result = jdbcTemplateObject.queryForRowSet(sql);
			return result;
		}		
}
