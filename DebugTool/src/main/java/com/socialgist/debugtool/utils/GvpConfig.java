package com.socialgist.debugtool.utils;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.socialgist.language_detector.LdContainer;

//@Configuration
public class GvpConfig {

		@Bean
		public GvpRepository  gvpUtilsContainer() {
			return new GvpRepository (); 
		}	

		@Bean
		public MySQLContainer  mySQLContainer() {
		    return new MySQLContainer (); 
		}	

		@Bean
		public HBaseContainer  hbaseContainer() {
		    return new HBaseContainer (); 
		}

		@Bean
		public TokensPoolContainer  tokensPoolContainer() {
		    return new TokensPoolContainer (); 
		}

		@Bean
		public LdContainer ldContainer() {
			return new LdContainer();
		}

		@Bean
		public StatsContainer statsContainer() {
			return new StatsContainer();
		}
		
		
}
