package com.socialgist.debugtool.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GvpSettings {

//	@Value("${gvp.module.running.mode:}")
//	public String runningMode;
	@Value("${gvp.module.server.name:}")
	public String server_name;
	@Value("${gvp.module.server.description:}")
	public String server_description;
	
    public SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    public String ip = "0.0.0.0";
    public String host_name = "";
	

	@Value("${gvp.output.exchange.name:}")
	public String output_exchange;
	@Value("${gvp.debug.queue.name:}")
	public String debug_queue;
	@Value("${gvp.debug.error.queue.name:}")
	public String debug_errors_queue;

	private static Logger LOG = LoggerFactory.getLogger(GvpSettings.class);
	
	@PostConstruct
  	public void init() {
		try {
			InetAddress inetAddress = InetAddress.getLocalHost();
			ip = inetAddress.getHostAddress();
		    host_name = inetAddress.getCanonicalHostName();
		} catch (UnknownHostException e) {}
		printConfiguration();
	}
	
	public void printConfiguration() {
		LOG.info("*** ip: {}", ip);
		LOG.info("*** host: {}", host_name);
		LOG.info("*** server: {}", server_name);
		LOG.info("*** description: {}", server_description);
	}
  
}
