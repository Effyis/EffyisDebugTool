package com.socialgist.debugtool;


import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.BasicHttpContext;


@Component
public class HttpContainer {

	CloseableHttpClient httpclient;
    PoolingHttpClientConnectionManager cm;
	
	public HttpContainer() {
		super();
	}

	public void init() {
		cm = new PoolingHttpClientConnectionManager();			
		cm.setMaxTotal(1000);
		cm.setDefaultMaxPerRoute(200);
		httpclient = HttpClients.custom().setConnectionManager(cm).setConnectionManagerShared(true).build();
	}

	public void shutdown() {
		this.shutdown();
	}
	
	public String connectToRest_Json(String nextUrl) throws Exception {

		// AAA p.last_api_call = System.currentTimeMillis();
		if ((nextUrl == null) || (nextUrl.equalsIgnoreCase("")))
			return null;
//			p.count_http_calls++;			

		HttpGet httpGet = new HttpGet(nextUrl);
		StringBuilder result = new StringBuilder(10000);
		HttpContext context = new BasicHttpContext();
		HttpResponse response = null;
		HttpEntity entity = null;

		InputStream instream = null;

		try {
			response = httpclient.execute(httpGet, context);
			entity = response.getEntity();
		} catch (IOException e) {
			// Here is connection timeout happens sometimes:
			// (java.net.SocketTimeoutException: Read timed out)
//			if (e.getClass() == java.net.SocketTimeoutException.class)
//				p.count_server_timeout++;
//			if (e.getClass() == org.apache.http.conn.ConnectTimeoutException.class)
//				p.count_server_timeout++;
			httpGet.releaseConnection();
//			p.count_http_error++;
			throw e;
		}

		if (entity != null) {
			try {
				instream = entity.getContent();

				BufferedReader rd = new BufferedReader(new InputStreamReader(instream));

				String line = "";
				while ((line = rd.readLine()) != null) {
					result.append(line);
				}

			} catch (IOException ex) {
				// In case of an IOException the connection will be released
				// back to the connection manager automatically
				httpGet.releaseConnection();
				throw ex;
			} catch (RuntimeException ex) {
				// In case of an unexpected exception you may want to abort
				// the HTTP request in order to shut down the underlying
				// connection immediately.
				httpGet.abort();
				httpGet.releaseConnection();
				throw ex;
			} finally {
				// Closing the input stream will trigger connection release
				try {
					instream.close();
				} catch (Exception ignore) {
				}
			}
		}
		httpGet.releaseConnection();

//AAA				p.count_success_requests++;
		return result.toString();
	};	
	
	
}
