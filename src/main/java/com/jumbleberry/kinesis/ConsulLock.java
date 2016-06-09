package com.jumbleberry.kinesis;

import java.util.UUID;

import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.SessionCreatedResponse;

public class ConsulLock {
	private final int lockAttempts = 60;
	private final int lockWait = 1000;
	private final String lockDelay = "1s";
	private final String lockTtl = "10s";
	
	private Consul consul;
	private String sessionId, key, lockSession;
	private KeyValueClient kvClient;
	private SessionClient sessionClient;
	
	private Thread heartbeat;
	
	public ConsulLock(String url, String key, String lockSession) {		
		this.key = key;		
		this.lockSession = lockSession + "_" + UUID.randomUUID().toString();			
		
		consul = Consul.builder().withUrl(url).build();
		kvClient = consul.keyValueClient();
		sessionClient = consul.sessionClient();	
		
		SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().name(this.lockSession).lockDelay(this.lockDelay).ttl(this.lockTtl).build());	
		this.sessionId = response.getId();
		
		heartbeat = new ConsulHeartbeat(sessionClient, this.sessionId);
		heartbeat.start();
	}
	
	/**
	 * Attempt to get a consul lock
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean acquireLock() throws Exception {
		int attempts = 0;
		
		while (!kvClient.acquireLock(key, sessionId)) {						
			if (++attempts > this.lockAttempts) {
				return false;
			}
			
			Thread.sleep(this.lockWait);
		}
		
		return true;
	}
	
	/**
	 * Release a consul lock
	 * 
	 * @return
	 */
	public boolean releaseLock() {
		return kvClient.releaseLock(key, sessionId);		
	}
	
	/**
	 * Destroy the existing session and release the lock
	 * 
	 * @param force
	 * @return
	 */
	public boolean releaseLock(boolean force) {
		if (force) {
			sessionClient.destroySession(this.sessionId);		
		}		
		
		return releaseLock();
	}
	
	/**
	 * Check if the lock exists and renew the sessions
	 * 
	 * @return
	 */
	public boolean hasLock() {						
		return kvClient.getValue(key).get().getSession().isPresent();
	}
}

class ConsulHeartbeat extends Thread {
	private final SessionClient sessionClient;
	private final String sessionId;
	private final int defaultInterval = 1000;
	private int interval;	
	
	public ConsulHeartbeat(SessionClient sessionClient, String sessionId, int interval) {		
		this(sessionClient, sessionId);
		this.interval = interval;
	}
	
	public ConsulHeartbeat(SessionClient sessionClient, String sessionId) {		
		this.sessionClient = sessionClient;
		this.sessionId = sessionId;
		this.interval = defaultInterval;
	}
	
	public void run() {
		while(true) {					
			sessionClient.renewSession(this.sessionId);
			
			try {
				Thread.sleep(this.interval);	
			} catch (Exception e) {
				System.exit(1);
			}
		}
	}	
}
