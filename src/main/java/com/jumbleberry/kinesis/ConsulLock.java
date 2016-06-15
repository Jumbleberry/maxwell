package com.jumbleberry.kinesis;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.SessionCreatedResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLock
{
	private static final int lockWait = 1000;	
	private static final int lockDelay = 1;
	private static final int lockTtl = 10;
	
	private static Consul consul;	
	private static String kvKey;
	private static KeyValueClient kvClient;
	private static SessionClient sessionClient;	
	private static ConsulHeartbeat heartbeat;	
	private static String sessionId;
	
	/**
	 * Attempt to get a consul lock
	 * 
	 * @param String url
	 * @param String key
	 * @param String lockSession
	 * @return
	 * @throws Exception
	 */
	public static boolean AcquireLock(String url, String key) throws InterruptedException {				
		buildSession(url, key);
			
		// Keep trying to get a lock for our session
		while (!kvClient.acquireLock(kvKey, sessionId)) {			
			renewSession();
			Thread.sleep(lockWait);
		}		
				
		// Start the heartbeat
		heartbeat.start();	
		
		return true;
	}	
	
	/**
	 * Initialize the Consul session
	 * 
	 * @param String url
	 * @param String key
	 * @param String lockSession
	 */
	private static void buildSession(String url, String key) {
		kvKey = key;		
		
		consul = Consul.builder().withUrl(url).build();
		kvClient = consul.keyValueClient();
		sessionClient = consul.sessionClient();	
		
		SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().lockDelay(lockDelay + "s").ttl(lockTtl + "s").build());	
		sessionId = response.getId();
		
		heartbeat = new ConsulHeartbeat(lockTtl + 1);				
		heartbeat.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override	
			public void uncaughtException(Thread t, Throwable e) {
				LoggerFactory.getLogger(ConsulLock.class).error("Lock lost due to inactivity");
				System.exit(0);
			}
		});
	}
	
	/**
	 * Checks the heartbeat and makes sure the lock is locked to our session
	 * 
	 * @return
	 */
	public static boolean hasLock() throws ConsulException {		
		return (heartbeat != null) && heartbeat.isAlive() && hasLockSession();
	}
	
	public static String getSessionId() {
		return sessionId;
	}
	
	/**
	 * Release a consul lock
	 * 
	 * @return
	 */
	public static boolean releaseLock() {
		if (kvClient == null)
			throw new ConsulException("KeyValueClient not initialized");
		
		return kvClient.releaseLock(kvKey, sessionId);		
	}
	
	/**
	 * Destroy the existing session and release the lock
	 * 
	 * @param force
	 * @return
	 */
	public static boolean releaseSession(boolean force) throws ConsulException {
		if (sessionClient == null)
			throw new ConsulException("SessionClient not initialized");
		
		if (sessionId != null && force) {
			sessionClient.destroySession(sessionId);		
		}		
		
		return releaseLock();
	}	
	
	/**
	 * Checks if the lock is locked to our session
	 * 
	 * @return
	 */
	public static boolean hasLockSession() throws ConsulException {
		if (kvClient == null)
			throw new ConsulException("KeyValueClient not initialized");		
		
		return kvClient.getValue(kvKey).get().getSession().isPresent() && kvClient.getValue(kvKey).get().getSession().get().equals(sessionId);
	}
	
	/**
	 * Renew the session to keep it alive
	 */
	public static void renewSession() throws ConsulException {
		if (kvClient == null)
			throw new ConsulException("SessionClient not initialized");
		
		sessionClient.renewSession(sessionId);
	}
}

class ConsulHeartbeat extends Thread
{
	static final Logger LOGGER = LoggerFactory.getLogger(ConsulHeartbeat.class);
	
	private int interval;
	
	public ConsulHeartbeat(int interval) {
		super();
		this.interval = interval;		
	}
	
	public void run() throws ConsulException {
		boolean canRun = true;
		
		while(canRun) {						
			try {
				Thread.sleep(this.interval);
				
				// Check if the lock is locked to our session
				if (!ConsulLock.hasLockSession()) {
					LOGGER.error("ConsulHeartbeat: No lock found for session");					
					
					canRun = false;
				}															
			} catch (Exception e) {				
				LOGGER.error("ConsulHeartbeat: " + e.getMessage());
				canRun = false;				
			}		
		}	
		
		// We lost the lock
		throw new ConsulException("ConsulHeartbeat stopped");
	}
}