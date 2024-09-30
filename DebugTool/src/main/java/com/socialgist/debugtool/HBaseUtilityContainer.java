package com.socialgist.debugtool;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.items.GvpChannel;
import com.socialgist.debugtool.utils.items.GvpVideo;
import com.socialgist.debugtool.utils.items.StatsTimePeriod;
import com.socialgist.debugtool.utils.items.VkWall;
import com.socialgist.debugtool.utils.items.VkWallRef;

@Component
//@ManagedResource(objectName = "Effyis DBUtils:name=HBaseContainer")
public class HBaseUtilityContainer  {

	@Autowired
	HBaseContainer hbaseContainer;
	
	private static Logger LOGGER = LoggerFactory.getLogger(HBaseUtilityContainer.class);
	
	// Create a Scan object
    Scan scan = new Scan();
    
	public Map<String, String> getAllTokens1() throws IOException{
		
		 Map<String, String> map = new HashMap<>();
		 Scan scan = new Scan();
//		 Scan scan = new Scan().withStartRow(Bytes.toBytes("")).withStopRow(Bytes.toBytes("~~~"));
          try (ResultScanner scanner = hbaseContainer.getScanner_TableVideoTokens(scan)) {
              for (Result result : scanner) {

            	String rowKey = new String(result.getRow());
            	   
            	byte[] hb_lastuse_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"));
              	byte[] hb_quota_used       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"));
    			byte[] hb_quota_exceeded = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"));
              			
          		long lastuse = (hb_lastuse_ts!=null)?Bytes.toLong(hb_lastuse_ts):0;				
          		long quota_used = (hb_quota_used!=null)?Bytes.toLong(hb_quota_used):0;
    			boolean quota_exceeded = ((hb_quota_exceeded != null) && (Bytes.toLong(hb_quota_exceeded)==1))  ? true : false;
          		
          		map.put(rowKey, lastuse + "." + quota_used + "." + quota_exceeded);
              }
          }
		return map;		
	}
	
	public Map<String, TokenUseRecord> getAllTokens() throws IOException{
		
		 Map<String, TokenUseRecord> map = new HashMap<>();
		 Scan scan = new Scan();
//		 Scan scan = new Scan().withStartRow(Bytes.toBytes("")).withStopRow(Bytes.toBytes("~~~"));
         try (ResultScanner scanner = hbaseContainer.getScanner_TableVideoTokens(scan)) {
             for (Result result : scanner) {
            	 
            	 TokenUseRecord record = new TokenUseRecord();
            	 record.rowKey = new String(result.getRow());

            	 record.token = record.rowKey.split("\\.")[0];
            	 record.day_of_use = Long.parseLong(record.rowKey.split("\\.")[1]);

            	 byte[] hb_lastuse_ts     = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"));
            	 byte[] hb_quota_used     = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"));
            	 byte[] hb_quota_exceeded = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"));

            	 record.lastuse = (hb_lastuse_ts!=null)?Bytes.toLong(hb_lastuse_ts):0;				
            	 record.quota_used = (hb_quota_used!=null)?Bytes.toLong(hb_quota_used):0;
            	 record.quota_exceeded = ((hb_quota_exceeded != null) && (Bytes.toLong(hb_quota_exceeded)==1))  ? true : false;
         		
            	 map.put(record.token, record);
            }
         }
		return map;		
	}
	
	
/*	
	public JsonData getStats(String stats_id) throws IOException{
	
        JsonData jsonData = new JsonData();
        // Create and populate the "cols" list
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("year", "Year", "string"));
        columns.add(new Column("sales", "Sales", "number"));
        columns.add(new Column("sales", "Sales1", "number"));
        columns.add(new Column("sales", "Sales2", "number"));
        jsonData.setCols(columns);
        // Create and populate the "rows" list
        List<Row> rows = new ArrayList<>();
		
		Result result = hbaseContainer.hBase_getStats(stats_id);
		if (result==null) return null;
		if ((result != null) && !result.isEmpty()) { // SUBSCRIPTION FOUND IN HBASE
			// Extract values from the Result instance and populate the HashMap
			for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes("stats_5m")).keySet()) {
				byte[] valueBytes = result.getValue(Bytes.toBytes("stats_5m"), qualifier);
				Long value = Bytes.toLong(valueBytes);
				String qualifierStr = Bytes.toString(qualifier);
				
		        // Create the first row
		        Row row1 = new Row(qualifierStr);
		        List<Cell> cells1 = new ArrayList<>();
		        cells1.add(new Cell(qualifierStr));
		        cells1.add(new Cell(value));
		        cells1.add(new Cell(value+50));
		        cells1.add(new Cell(value+100));
		        row1.setC(cells1);
		        rows.add(row1);
			}
		}
        jsonData.setRows(rows);
		return jsonData;
	}
*/
	public Map<String, Long> getStatsMap(String stats_id, StatsTimePeriod chartTimePeriod) throws IOException{
		
		Map<String, Long> map = new HashMap<>();
		Result result = hbaseContainer.hBase_getStats(stats_id);
		if (result==null) return null;
		if ((result != null) && !result.isEmpty()) {
			// Extract values from the Result instance and populate the HashMap
			for (byte[] qualifier : result.getFamilyMap(Bytes.toBytes(chartTimePeriod.getCfName())).keySet()) {
				String qualifierStr = Bytes.toString(qualifier);
				 if (qualifierStr.startsWith("ut")) {
					 qualifierStr = qualifierStr.substring("ut".length());
					 byte[] valueBytes = result.getValue(Bytes.toBytes(chartTimePeriod.getCfName()), qualifier);
					 Long value = Bytes.toLong(valueBytes);
					 map.put(qualifierStr, value);
				 }
			}
		}
		return map;
	}

	
	public Map<Integer, Map<String, String>> getThreadsStats(String prefix) throws IOException{

//		threadStatsList = new StatsList("gvp" + gvp_id + ".ck" + clientkey_id + ".ckt" + clientkey_thread_id);
		
		 SortedMap<Integer, Map<String, String>> map = new TreeMap<>();		
//		SortedMap<Integer, Map<String, String>> map = new TreeMap<>();
//		 Scan scan = new Scan();
		 Scan scan = new Scan().withStartRow(Bytes.toBytes(prefix)).withStopRow(Bytes.toBytes(prefix + "~"));
		 
         try (ResultScanner scanner = hbaseContainer.getScanner_TableDebugStats(scan)) {
             for (Result result : scanner) {

           	String rowKey = new String(result.getRow());
           	
           	Integer thread_id = Integer.parseInt(rowKey.substring(rowKey.indexOf("ckt")+3));
           	
           	
           	Map<String, String> map1 = new HashMap<>();
           	
            // Iterate over all cells in the result
            for (Cell cell : result.listCells()) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                
               	map1.put(qualifier, value);
                // Do something with family, qualifier, and value
                System.out.println("RowKey: " + rowKey +
                                   ", Family: " + family +
                                   ", Qualifier: " + qualifier +
                                   ", Value: " + value);
            }           	
           	
//           	byte[] hb_lastuse_ts    = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("lastuse"));
//           	byte[] hb_quota_used       = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_used"));
//   			byte[] hb_quota_exceeded = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("quota_exceeded"));
             			
//         		long lastuse = (hb_lastuse_ts!=null)?Bytes.toLong(hb_lastuse_ts):0;				
//         		long quota_used = (hb_quota_used!=null)?Bytes.toLong(hb_quota_used):0;
//   			boolean quota_exceeded = ((hb_quota_exceeded != null) && (Bytes.toLong(hb_quota_exceeded)==1))  ? true : false;
         		
         		map.put(thread_id, map1);
             }
         }
		return map;		
	}
	
	public Map<String, GvpChannel> getChannels(String start_hbase_scan) throws IOException{
		
       	Map<String, GvpChannel> map = new HashMap<>();
		int i = 0;
//		String start_hbase_scan = "ytc." + s_thread_id + "." + start_sec;
	    Scan scan = new Scan().withStartRow(Bytes.toBytes(start_hbase_scan)).withStopRow(Bytes.toBytes("ytc.X"));		
         try (ResultScanner scanner = hbaseContainer.getScanner_TableVideoGvp(scan)) {
             for (Result rr : scanner) {
       	       String rowKey = new String(rr.getRow());
      	       GvpChannel ghc = new GvpChannel(rr);
      	       map.put(rowKey, ghc);
      	       i++;
      	       if (i>100) break;
             }
         }
         
		return map;		
	}

	
	public Map<String, GvpVideo> getVideos(String start_hbase_scan) throws IOException{
		
       	Map<String, GvpVideo> map = new HashMap<>();
		int i = 0;
//		String start_hbase_scan = "ytc." + s_thread_id + "." + start_sec;
	    Scan scan = new Scan().withStartRow(Bytes.toBytes(start_hbase_scan)).withStopRow(Bytes.toBytes("youtube.X"));		
         try (ResultScanner scanner = hbaseContainer.getScanner_TableVideoGvp(scan)) {
             for (Result rr : scanner) {
       	       String rowKey = new String(rr.getRow());
       	       GvpVideo ghc = new GvpVideo(rr);
      	       map.put(rowKey, ghc);
      	       i++;
      	       if (i>100) break;
             }
         }
         
		return map;		
	}

//*************************************************************************************************
	
	
	public Map<String, VkWall> getHBaseWalls(String start_hbase_scan) throws IOException 
	{
		
       	Map<String, VkWall> map = new LinkedHashMap<>();
		int i = 0;
	    Scan scan = new Scan().withStartRow(Bytes.toBytes(start_hbase_scan)).withStopRow(Bytes.toBytes("walls.X"));		
         try (ResultScanner scanner = hbaseContainer.getScanner_TableWalls(scan)) {
             for (Result rr : scanner) {
       	       String rowKey = new String(rr.getRow());
       	       VkWall vkWall = new VkWall(rr);
      	       map.put(rowKey, vkWall);
      	       i++;
      	       if (i>100) break;
             }
         }
         
		return map;		
	}
	
	public Map<String, VkWall> getHBaseWallsRef(String start_hbase_scan) throws IOException 
	{
       	Map<String, VkWall> map = new LinkedHashMap<>();
		int i = 0;
	    Scan scan = new Scan().withStartRow(Bytes.toBytes(start_hbase_scan)).withStopRow(Bytes.toBytes("wallref.X"));		
         try (ResultScanner scanner = hbaseContainer.getScanner_TableWalls(scan)) {
             for (Result rr : scanner) {
       	       String rowKey = new String(rr.getRow());
       	       String wall_id = rowKey.split("\\.")[2];
       	       VkWall vkWall = hbaseContainer.hBase_getWall("wall." + wall_id);
      	       map.put(rowKey, vkWall);
      	       i++;
      	       if (i>100) break;
             }
         }
		return map;		
	}
}
