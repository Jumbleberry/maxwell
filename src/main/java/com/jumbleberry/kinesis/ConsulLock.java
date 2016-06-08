package com.jumbleberry.kinesis;

import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.SessionCreatedResponse;
import com.orbitz.consul.model.session.SessionInfo;

public class ConsulLock {
	private final int lockAttempts = 60;
	private final int lockWait = 1000;
	private final String lockDelay = "1s";
	private final String lockTtl = "10s";
	
	private Consul consul;
	private String sessionId, key, lockSession;
	private KeyValueClient kvClient;
	private SessionClient sessionClient;
	
	public ConsulLock(String url, String key, String lockSession) {
		this.key = key;		
		this.lockSession = lockSession;
		
		consul = Consul.builder().withUrl(url).build();
		kvClient = consul.keyValueClient();
		sessionClient = consul.sessionClient();
		
		refreshSessionId();						
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
			refreshSessionId();			
			
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
		boolean hasLock = kvClient.getValue(key).get().getSession().isPresent();
		
		if (hasLock)
			sessionClient.renewSession(this.sessionId);
				
		return hasLock;		
	}
	
	/**
	 * Sets the session id to an existing session if it exists or creates a new one
	 */
	public void refreshSessionId() {
		if (!sessionClient.listSessions().isEmpty()) {			
			SessionInfo sessionInfo = sessionClient.listSessions().get(0);
			this.sessionId = sessionInfo.getId();
		} else {
			SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().name(this.lockSession).lockDelay(this.lockDelay).ttl(this.lockTtl).build());	
			this.sessionId = response.getId();
		}			
	}
}
