package com.socialgist.debugtool.utils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;

import com.socialgist.debugtool.utils.items.GvpDictionaryItem;
import com.socialgist.debugtool.utils.items.GvpUtils;


//@Component
//@ManagedResource(objectName = "Effyis DBUtils:name=HBaseContainer")
public class DictionaryContainer implements Closeable {

	@Value("${gvp.dictionary.hbase.prefix:}") //
	String hbase_index_prefix; // Needed to use different dictionaries independent of gvp id, cp id and everything
	@Value("${gvp.dictionary.load:false}") //
	boolean loadToMemoty; // Set a flag to indicate whether the dictionary needs to be loaded into memory.    

	@Autowired
	HBaseContainer hbaseContainer;

	List<GvpDictionaryItem> list = new ArrayList<GvpDictionaryItem>();
	LinkedHashMap<String, GvpDictionaryItem> dictionary_list   = new LinkedHashMap<String, GvpDictionaryItem>();
	Iterator<Entry<String, GvpDictionaryItem>> itDictionary_list = null;
	
	private static Logger LOGGER = LoggerFactory.getLogger(DictionaryContainer.class);
	
	@Autowired
	private ResourcePatternResolver resourcePatternResolver;

	public void init() {
  		try {
  			if (loadToMemoty) { loadDictionary();
	  			LOGGER.info("*** DictionaryContainer. {} words was loaded ***", dictionary_list.size());
  			}
 			LOGGER.info("*** DictionaryContainer was initialized succefully. ***");
  		}
		catch(Exception e) {
			LOGGER.error("*** DictionaryContainer initialization error ***",  e.getMessage());
		}
	}	
	
	@Override
	public void close() throws IOException {
	}
	
    public void loadDictionary() throws IOException {
        String resourcePattern = "classpath*:/dictionary/*.txt";
        Resource[] resources = resourcePatternResolver.getResources(resourcePattern);

        for (Resource resource : resources) {
            if (resource.isReadable()) {
                readFileContents(resource);
            }
        }
    }

    private void readFileContents(Resource resource) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
				  line = line.trim();
				  if (line.isEmpty()) continue;
				  line = line.toLowerCase();
				  String[]  words = line.split(" ");
				  for (String word : words) {
					  	dictionary_list.putIfAbsent(word, new GvpDictionaryItem(hbase_index_prefix, word));
				  }
            }
        }
    }

	public synchronized GvpDictionaryItem getNextAvailable_DictionaryItem(){

		long counter = 0; 
		
		while (counter < 30000) {
			GvpDictionaryItem item = getNextDictionaryItem(hbase_index_prefix);
			counter++;
			if (item != null) return item;
		}
		return null;
	}

	public synchronized GvpDictionaryItem getNextDictionaryItem(String hbase_index_prefix){
		
		if ((itDictionary_list == null) || (!itDictionary_list.hasNext())) {
			itDictionary_list = dictionary_list.entrySet().iterator();
		}     
		if 	(!itDictionary_list.hasNext()) return null;  // if no keywords	
		
		GvpDictionaryItem item = itDictionary_list.next().getValue();
		
		if (item.scheduled_ts == 0) { // restarted controller or waiting response from collector
			if (item.lastRequest_ts > System.currentTimeMillis()/1000L - 60*10) { // 10 minutes
				return null;
			}
			item.scheduled_ts = getScheduledTimeFromHbase(item.keyword);
		}
		
		if (item.scheduled_ts < System.currentTimeMillis()/1000L) {
			item.lastRequest_ts = System.currentTimeMillis()/1000L;
			item.scheduled_ts = 0;
			return item;
		}
		else 
			return null;
	}

	public synchronized long getScheduledTimeFromHbase(String keyword) {
		long scheduled_ts = 0;
		Result result = getDictionaryItemFromHbase(keyword);
		if ((result != null) && !result.isEmpty()) { // MEDIA FOUND IN HBASE
			byte[] hb_scheduled_ts  = result.getValue(Bytes.toBytes("cfStatus"), Bytes.toBytes("scheduled_ts"));
			scheduled_ts = (hb_scheduled_ts!=null)?Bytes.toLong(hb_scheduled_ts):0;
		}
		return scheduled_ts;
	}
	
	public synchronized Result getDictionaryItemFromHbase(String keyword) {
		return hbaseContainer.hBase_getDictionaryItem(hbase_index_prefix, keyword);
	}
	
	public synchronized GvpDictionaryItem getDictionaryItemObjectFromHbase(String keyword)  { 
		String hbase_index = hbase_index_prefix + "." + GvpUtils.getMD5(keyword);
		return hbaseContainer.hBase_getDictionaryItemObject(hbase_index);
	}

	public synchronized GvpDictionaryItem updateDictionaryItemObjectInHBase(GvpDictionaryItem gdi)  { 
		return hbaseContainer.hBase_updateDictionaryItemObject(gdi);
	}
	
	public synchronized void updateDictionaryItemObjectInHBase(String keyword, String pubBefore,
			long pubBefore_ts, long search_timeframe_sec, long scheduled_ts) {  
			hbaseContainer.hBase_updateDictionaryItem(hbase_index_prefix, keyword, pubBefore,
							pubBefore_ts, search_timeframe_sec, scheduled_ts);
	}
}


