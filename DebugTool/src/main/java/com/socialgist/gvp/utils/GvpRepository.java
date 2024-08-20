package com.socialgist.gvp.utils;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GvpRepository implements Closeable {

	@Autowired
	MySQLContainer mySQLContainer;
	@Autowired
	HBaseContainer hbaseContainer;
	@Autowired
	RabbitMainContainer rabbitMainContainer;
	@Autowired
	DictionaryContainer dictionaryContainer;
	@Autowired
	TokensPoolContainer tokensPoolContainer;
	@Autowired
	StatsContainer statsContainer;

	boolean initSuccess = true;

	private static Logger LOGGER = LoggerFactory.getLogger(GvpRepository.class);

	public void init() {
		try {
			rabbitMainContainer.init();
			mySQLContainer.init();
			hbaseContainer.init();
			dictionaryContainer.init();
			tokensPoolContainer.init();
			statsContainer.init();
			LOGGER.info("*** GvpRepository was initialized succefully. ***");
		} catch (Exception e) {
			LOGGER.error("*** GvpRepository initialization error ***", e.getMessage());
			initSuccess = false;
		}
	}

	@Override
	public void close() throws IOException {
		hbaseContainer.close();
	}

	public boolean isInitSuccess() {
		return initSuccess;
	}
	
}
