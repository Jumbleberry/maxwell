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
	private static final String lockDelay = "1s";
	private static final String lockTtl = "10s";
	
	private static Consul consul;	
	private static String kvKey;
	private static KeyValueClient kvClient;
	private static SessionClient sessionClient;	
	private static ConsulHeartbeat heartbeat;	
	private static String sessionId;
	
	private static HeartbeatExceptionHandler heartbeatExceptionHandler;
	
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
		
		SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().lockDelay(lockDelay).ttl(lockTtl).build());	
		sessionId = response.getId();
		
		heartbeatExceptionHandler = new HeartbeatExceptionHandler();
		
		heartbeat = new ConsulHeartbeat();				
		heartbeat.setUncaughtExceptionHandler(heartbeatExceptionHandler);
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
	public static boolean releaseLock(String sessionId, boolean force) throws ConsulException {
		if (sessionClient == null)
			throw new ConsulException("SessionClient not initialized");
		
		if (force) {
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
	
	public static void addObserver(Observer obs) {
		ObservableConsul obsCon = new ObservableConsul();		
		obsCon.addObserver(obs);
		
		heartbeatExceptionHandler.addObserver(obsCon);
	}
}

class ConsulHeartbeat extends Thread 
{
	static final Logger LOGGER = LoggerFactory.getLogger(ConsulHeartbeat.class);
	
	private final int defaultInterval = 1000;
	private int interval;
	
	public ConsulHeartbeat() {
		this.interval = defaultInterval;
	};
	
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

class ObservableConsul extends Observable 
{	
	public void notifyError() {
		// Trigger update() in observers
		setChanged();
		notifyObservers();		
	}
}

class HeartbeatExceptionHandler implements UncaughtExceptionHandler 
{		
	private List<ObservableConsul> obs;
	
	public HeartbeatExceptionHandler() {
		obs = new ArrayList<ObservableConsul>();
	}
	
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		// Notify observers
		for (int i = 0; i < obs.size(); i++) {
			ObservableConsul o = obs.get(i);
			o.notifyError();
		}		
	}
	
	public void addObserver(ObservableConsul obs) {
		this.obs.add(obs);
	}
}