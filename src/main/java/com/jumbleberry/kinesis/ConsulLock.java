package com.jumbleberry.kinesis;

import java.util.UUID;

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.SessionCreatedResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLock {
	private static final int lockAttempts = 60;
	private static final int lockWait = 1000;
	
	private final String lockDelay = "1s";
	private final String lockTtl = "10s";	
	private final Consul consul;
	
	private static String kvKey;
	private static KeyValueClient kvClient;
	private static SessionClient sessionClient;
	
	private Thread heartbeat;	
	private String sessionId;

	public ConsulLock(String url, String key, String lockSession) {		
		kvKey = key;		
		lockSession = lockSession + "_" + UUID.randomUUID().toString();			
		
		consul = Consul.builder().withUrl(url).build();
		kvClient = consul.keyValueClient();
		sessionClient = consul.sessionClient();	
		
		SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().name(lockSession).lockDelay(lockDelay).ttl(lockTtl).build());	
		this.sessionId = response.getId();
		
		heartbeat = new ConsulHeartbeat(this.sessionId);
	}
	
	/**
	 * Attempt to get a consul lock
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean acquireLock() throws InterruptedException {				
		int attempts = 0;
		
		// Keep trying to get a lock for our session
		while (!kvClient.acquireLock(kvKey, this.sessionId)) {			
			renewSession(this.sessionId);
			
			if (lockAttempts != 0 && ++attempts > lockAttempts) {
				return false;
			}
			
			Thread.sleep(lockWait);
		}		
				
		// Start the heartbeat
		heartbeat.start();
		
		return true;
	}
	
	/**
	 * Release a consul lock
	 * 
	 * @return
	 */
	public static boolean releaseLock(String sessionId) {
		return kvClient.releaseLock(kvKey, sessionId);		
	}
	
	/**
	 * Destroy the existing session and release the lock
	 * 
	 * @param force
	 * @return
	 */
	public static boolean releaseLock(String sessionId, boolean force) {
		if (force) {
			sessionClient.destroySession(sessionId);		
		}		
		
		return releaseLock(sessionId);
	}
	
	/**
	 * Checks the heartbeat and makes sure the lock is locked to our session
	 * 
	 * @return
	 */
	public boolean hasLock(String sessionId) throws ConsulException {
		if (!heartbeat.isAlive())
			throw new ConsulException("ConsulHeartbeat died");
		
		return hasLockSession(sessionId);
	}
	
	/**
	 * Checks if the lock is locked to our session
	 * 
	 * @return
	 */
	public static boolean hasLockSession(String sessionId) throws ConsulException {		
		return kvClient.getValue(kvKey).get().getSession().isPresent() && kvClient.getValue(kvKey).get().getSession().get().equals(sessionId);
	}
	
	/**
	 * Renew the session to keep it alive
	 */
	public static void renewSession(String sessionId) throws ConsulException {							
		sessionClient.renewSession(sessionId);
	}
	
	public String getSessionId() {
		return this.sessionId;
	}
}

class ConsulHeartbeat extends Thread {
	static final Logger LOGGER = LoggerFactory.getLogger(ConsulHeartbeat.class);
	
	private final int defaultInterval = 1000;
	private int interval;
	private String sessionId;
	
	public ConsulHeartbeat(String sessionId) {
		this.sessionId = sessionId;
		this.interval = defaultInterval;
	};
	
	public ConsulHeartbeat(String sessionId, int interval) {
		super(sessionId);
		this.interval = interval;		
	}
	
	public void run() {
		boolean canRun = true;
		
		while(canRun) {											
			try {
				// Check if the lock is locked to our session
				if (!ConsulLock.hasLockSession(sessionId)) {
					LOGGER.error("No lock found for session");					
					canRun = false;
				}
											
				Thread.sleep(this.interval);	
			} catch (Exception e) {				
				LOGGER.error(e.getMessage());
				canRun = false;				
			}		
		}	
		
		// We lost the lock
		this.interrupt();
	}	
}