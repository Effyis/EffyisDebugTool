package com.socialgist.debugtool;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;

import jakarta.xml.bind.DatatypeConverter;

public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		for (int i = 0; i < 10; i++) {
	    	 String now = LocalDate.now().minusDays(i).toString();
	    	 if (getUnixTime(now) == convertTimeToUnixTime(now)){
    			System.out.println(" YES ");
	    	 }
        }

		for (int i = 0; i < 10; i++) {
	    	 String now = LocalTime.now().minusSeconds(i).toString();
	    	 if (getUnixTime(now) == convertTimeToUnixTime(now)){
   			System.out.println(" YES ");
	    	 }
       }
	     
	}

	public static long getUnixTime(String published_date) {
		Calendar calendar = javax.xml.bind.DatatypeConverter.parseDateTime(published_date);
		long unixTime = calendar.getTimeInMillis() / 1000;
		System.out.println(published_date + " === " + unixTime);
		return unixTime;
	}
		
	public static long  convertTimeToUnixTime(String time) {
		Calendar calendar = DatatypeConverter.parseDateTime(time);
		long unixTime = calendar.getTimeInMillis() / 1000;
		System.out.println(time + " === " + Instant.ofEpochSecond(unixTime).toString());
		return unixTime;
	}
	
	
	
}
