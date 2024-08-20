package com.socialgist.debugtool;

import java.io.IOException;
import java.sql.SQLException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DebugToolApplication {
	
	public static void main(String[] args) throws SQLException, IOException {
		SpringApplication.run(DebugToolApplication.class, args);
	}

}
