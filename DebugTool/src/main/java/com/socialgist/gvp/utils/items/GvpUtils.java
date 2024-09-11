package com.socialgist.gvp.utils.items;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

public class GvpUtils {
	
	 public static String getMD5(String s)  {

		 try {
			 MessageDigest m = MessageDigest.getInstance("MD5");
			 m.reset();
			 m.update(s.getBytes());
			 byte[] digest = m.digest();
			 BigInteger bigInt = new BigInteger(1,digest);
			 String hashtext = bigInt.toString(16);
		 // Now we need to zero pad it if you actually want the full 32 chars.
			 while(hashtext.length() < 32 ){
				 hashtext = "0"+hashtext;
			 }
			 return hashtext;
		 }
		 catch (Exception e) {
			 return null;
		 }
	 }
	 
	 public static long getPDT_midnight_ms()  {
	  LocalDateTime now = LocalDateTime.now(ZoneId.of("America/Los_Angeles")); // current date and time
	  LocalDateTime midnight = now.toLocalDate().atStartOfDay();
	  return midnight.atZone(ZoneId.of("America/Los_Angeles")).toInstant().toEpochMilli();
	 }

	 public static long getPDT_midnight_sec()  {
		  LocalDateTime now = LocalDateTime.now(ZoneId.of("America/Los_Angeles")); // current date and time
		  LocalDateTime midnight = now.toLocalDate().atStartOfDay();
		  return midnight.atZone(ZoneId.of("America/Los_Angeles")).toInstant().toEpochMilli()/1000L;
	 }
	 
	 
	 public static long getUTC_startMonth111()  {
		  LocalDateTime midnight = LocalDateTime.now().toLocalDate().atStartOfDay(); // current date and time
//		  System.out.println("midnight => " + midnight);
		  LocalDateTime first_day = midnight.with(TemporalAdjusters.firstDayOfMonth());
//		  System.out.println("first_day => " + first_day);
		  return first_day.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
	 }	 
	 
		public static long convertToUnixTime(String date) {
			Calendar calendar = DatatypeConverter.parseDateTime(date);
			long unixTime = calendar.getTimeInMillis() / 1000;
			return unixTime;
		}
		
		public static String getCurrentISOTimeUTC() {
			return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
		      	.withZone(ZoneOffset.UTC).format(Instant.now());
		}

		public static String getCurrentISOTimeLocal() {
			return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
		      	.withZone(ZoneOffset.systemDefault()).format(Instant.now());
		}

		public static long adjustCurrentUnixTime(int periodInSeconds) {
	        long currentUnixTime = Instant.now().getEpochSecond();
	        return (currentUnixTime / periodInSeconds) * periodInSeconds;
		}
		
		
		public String getDateDiff(long d1, long d2)
		{
			
			String format_full = "%dd %dh %dm %ds";
			String format_hours = "%dh %dm %ds";
			//in milliseconds
			long diff = d2 - d1;

			long diffSeconds = diff / 1000 % 60;
			long diffMinutes = diff / (60 * 1000) % 60;
			long diffHours = diff / (60 * 60 * 1000) % 24;
			long diffDays = diff / (24 * 60 * 60 * 1000);
			
			if (diffDays==0) 
				return String.format(format_hours, diffHours, diffMinutes, diffSeconds );
			else
				return String.format(format_full, diffDays, diffHours, diffMinutes, diffSeconds);
		}

		public static boolean isValidProxy(String proxy) {
		    // Regular expression for validating IPv4 address and port
		    String regex = "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):\\d{1,5}$";
		    // Compile the regex
		    Pattern pattern = Pattern.compile(regex);
		    // Match the regex against the proxy string
		    Matcher matcher = pattern.matcher(proxy);
		    // Check if the format is correct and port is within the valid range
		    if (matcher.matches()) {
		        // Extract the port part
		        String[] parts = proxy.split(":");
		        int port = Integer.parseInt(parts[1]);
		        return port >= 1 && port <= 65535;
		    }
		    return false;
		}		
		
}
