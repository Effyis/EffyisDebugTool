package com.socialgist.debugtool;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.socialgist.debugtool.utils.DictionaryContainer;
import com.socialgist.debugtool.utils.HBaseContainer;
import com.socialgist.debugtool.utils.MySQLContainer;
import com.socialgist.debugtool.utils.StatsContainer;
import com.socialgist.debugtool.utils.TokensPoolContainer;

@Configuration
public class GvpUtilsConfig {

		@Bean
		public MySQLContainer  mySQLContainer() {
		    return new MySQLContainer (); 
		}	

		@Bean
		public HBaseContainer  hbaseContainer() {
		    return new HBaseContainer (); 
		}
		
//		@Bean
//		public DictionaryContainer  dictionaryContainer() {
//		    return new DictionaryContainer (); 
//		}

		@Bean
		public TokensPoolContainer  tokensPoolContainer() {
		    return new TokensPoolContainer (); 
		}
		
		@Bean
		public StatsContainer statsContainer() {
			return new StatsContainer();
		}
}
