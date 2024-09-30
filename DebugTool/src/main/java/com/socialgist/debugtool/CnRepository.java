package com.socialgist.debugtool;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.socialgist.debugtool.utils.DictionaryContainer;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.MySQLContainer;
import com.socialgist.debugtool.utils.TokensPoolContainer;

@Component
public class CnRepository {

	@Value("${youtube_token:}")   
	String youtube_token;
	@Value("${vk_token:}")   
	String vk_token;
	
	@Autowired
	MySQLContainer mySQLContainer;
	@Autowired
	HBaseContainer hbaseContainer;
//	@Autowired
//	DictionaryContainer dictionaryContainer;
//	@Autowired
//	TokensPoolContainer tokensPoolContainer;

	@Autowired
	MySQLUtilsContainer mySQLUtilsContainer;
	@Autowired
	HttpContainer httpContainer;
	
	boolean initSuccess = true;  

    private static Logger LOG = LoggerFactory.getLogger(CnRepository.class);
    
    @PostConstruct
    public void init() throws Exception {
  		mySQLContainer.init();
  		hbaseContainer.init();
//  		dictionaryContainer.init();
//  		tokensPoolContainer.init();
  		
  		mySQLUtilsContainer.init();
  		httpContainer.init();  		
	    LOG.info("Repository initialization - ok");
	}

	public void shutdown() {
	}
	
}
