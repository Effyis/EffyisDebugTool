package com.socialgist.debugtool.utils;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import java.sql.SQLException;


//@Component
//@ManagedResource(objectName = "Effyis GvpUtils:name=MySQLContainer")
public class MySQLContainer implements Closeable {

//*******  MySQL Parameters *********************

	@Value("${gvp.mysql.driver:}")
	String mysql_driver;
	@Value("${gvp.mysql.url:}")
	String mysql_url;
	@Value("${gvp.mysql.user:}")
	String mysql_user;
	@Value("${gvp.mysql.psw:}")
	String mysql_psw;
	
	JdbcTemplate jdbcTemplate;

	private static Logger LOGGER = LoggerFactory.getLogger(MySQLContainer.class);
	
  	public void init() throws SQLException {
  		try {
  			DriverManagerDataSource dataSource = new DriverManagerDataSource();
  			dataSource.setDriverClassName(mysql_driver);
  			dataSource.setUrl(mysql_url);
  			dataSource.setUsername(mysql_user);
  			dataSource.setPassword(mysql_psw);
//  		Connection connection = dataSource.getConnection();
  			jdbcTemplate = new JdbcTemplate(dataSource);
  	  		LOGGER.info("*** MySQLContainer was initialized succefully ***");
  		}
		catch(Exception e) {
			LOGGER.error("*** MySQLContainer initialization error ***",  e.getMessage());
		}
  	}
  
  	@Override
  	public void close() throws IOException {
  	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
  	
  	
}
