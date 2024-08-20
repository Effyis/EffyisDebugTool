package com.socialgist.gvp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.rabbitmq.client.Channel;
import com.socialgist.template.rabbit.RabbitUtils;

public class RabbitMainContainer {

	// Exchange to use for binding request/result queues  
	@Value("${template.out.exchange.name:}")
	public	String exchange_name;
	@Value("#{new String('${template.out.exchange.autodeclare:true}').equalsIgnoreCase('true')}")
	public	boolean autodeclareExchange;
	@Value("${template.out.exchange.autodeclare.type:direct}")
	public	String exchange_type;
	@Value("#{new String('${template.out.exchange.autodeclare.durable:true}').equalsIgnoreCase('true')}")
	public	boolean exchange_durable;
	@Value("#{new String('${template.out.exchange.autodeclare.autodelete:false}').equalsIgnoreCase('true')}")
	public	boolean exchange_autodelete;
	@Value("#{new String('${template.out.exchange.autodeclare.internal:false}').equalsIgnoreCase('true')}")
	public	boolean exchange_internal;
	
	@Value("${gvp.queue.result.name:}")
	public	String resultQueue_Name;
	@Value("#{new String('${gvp.queue.result.autodeclare:true}').equalsIgnoreCase('true')}")
	public boolean resultQueue_Autodeclare;
	@Value("#{new String('${gvp.queue.result.autodeclare.durable:true}').equalsIgnoreCase('true')}")
	public boolean resultQueue_Durable;
	@Value("#{new String('${gvp.queue.result.autodeclare.autodelete:false}').equalsIgnoreCase('true')}")
	public boolean resultQueue_Autodelete;
	@Value("#{new String('${gvp.queue.result.autodeclare.exclusive:false}').equalsIgnoreCase('true')}")
	public boolean resultQueue_Exclusive;
	@Value("${gvp.queue.result.binding.routingkey.name:}")
	public String resultQueue_Rk;

	@Value("${gvp.queue.request.name:}")
	public String requestQueue_Name;
	@Value("#{new String('${gvp.queue.request.autodeclare:true}').equalsIgnoreCase('true')}")
	public boolean requestQueue_Autodeclare;
	@Value("#{new String('${gvp.queue.request.autodeclare.durable:true}').equalsIgnoreCase('true')}")
	public boolean requestQueue_Durable;
	@Value("#{new String('${gvp.queue.request.autodeclare.autodelete:false}').equalsIgnoreCase('true')}")
	public boolean requestQueue_Autodelete;
	@Value("#{new String('${gvp.queue.request.autodeclare.exclusive:false}').equalsIgnoreCase('true')}")
	public boolean requestQueue_Exclusive;
	@Value("${gvp.queue.request.binding.routingkey.name:}")
	public String requestQueue_Rk;
	
	@Autowired
	RabbitUtils utils;
	
	@Autowired
	ConnectionFactory connectionFactory;

	private static Logger LOGGER = LoggerFactory.getLogger(RabbitMainContainer.class);
	
  	public void init() {
  		if (createInfrastructure())
  			LOGGER.info("*** RabbitMainContainer was initialized succefully ***");
  		else
  			LOGGER.error("*** RabbitMainContainer initialization error ***");
  	}
	
	public boolean createInfrastructure() {
		
		Connection connection;
		Channel channel;

		boolean isReady = false;
		
		try {
		connection = connectionFactory.createConnection(); 
		channel = connection.createChannel(false);
				
				if (autodeclareExchange) {
						utils.declareExchange(channel, 
								exchange_name,
								exchange_type,
								exchange_durable,
								exchange_autodelete,
								exchange_internal
									);										
				}
				utils.declareQueueWithBinding(channel, 
						resultQueue_Name, 
						resultQueue_Durable,
						resultQueue_Exclusive,
						resultQueue_Autodelete,
						exchange_name,
						resultQueue_Rk,
						null, null, null);

				utils.declareQueueWithBinding(channel, 
						requestQueue_Name, 
						requestQueue_Durable,
						requestQueue_Exclusive,
						requestQueue_Autodelete,
						exchange_name,
						requestQueue_Rk,
						null, null, null);
				channel.abort();
				connection.close();
				isReady = true;
		}
		catch (Exception e) {
			isReady = false;			
		}
		return isReady;
	}
	
}
