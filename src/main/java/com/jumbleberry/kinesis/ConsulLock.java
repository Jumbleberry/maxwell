package com.jumbleberry.kinesis;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.SessionClient;
import com.orbitz.consul.model.kv.Value;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.SessionCreatedResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsulLock
{
	private static final int lockWait = 1000;	
	private static final int lockDelay = 30;
	private static final int lockTtl = 120;


	static final Logger LOGGER = LoggerFactory.getLogger(ConsulLock.class);

	private static Consul consul;	
	private static String kvKey;
	private static KeyValueClient kvClient;
	private static SessionClient sessionClient;	
	private static ConsulHeartbeat heartbeat;	
	private static String sessionId;
	private static AtomicBoolean pendingRefresh = new AtomicBoolean(false);

	private static long sessionRefresh = 0;
	private static long heartbeatStart = 0;

	class ConsulHeartbeat extends Thread {
		public void run() throws ConsulException {
			for (;;) {					
				try {
					// Renew the heartbeat session if renewal is pending
					if (ConsulLock.isSessionPendingRenewal())
						ConsulLock.renewSession();

					// Check if the lock is locked to our session
					if (!ConsulLock.hasLockSession())
						break;

					Thread.sleep(Math.max(Math.min(lockDelay, lockTtl) * 500, 1000));
				} catch (Exception e) {				
					LOGGER.error("ConsulHeartbeat: " + e.getMessage());		
				}		
			}	

			// We lost the lock
			throw new ConsulException("ConsulHeartbeat stopped");
		}
	}

	/**
	 * Attempt to get a consul lock
	 * 
	 * @param String url
	 * @param String key
	 * @param String lockSession
	 * @return
	 * @throws Exception
	 */
	public ConsulLock(String url, String key) throws InterruptedException {				
		LOGGER.info("Trying to acquire Consul lock on host: " + url);
		
		for (;;) {
			try {
				buildSession(url, key);
	
				// Keep trying to get a lock for our session
				while (!kvClient.acquireLock(kvKey, sessionId)) {			
					Thread.sleep(lockWait);
					renewSession();
				}
				
				break;
				
			} catch (Exception e) {
				Thread.sleep(lockWait);
			}
		}
			
		// Start the heartbeat
		heartbeatStart = sessionRefresh = System.currentTimeMillis();
		heartbeat.start();
		LOGGER.info("Consul lock acquired with session: " + getSessionId());
	}	

	/**
	 * Initialize the Consul session
	 * 
	 * @param String url
	 * @param String key
	 * @param String lockSession
	 */
	private void buildSession(String url, String key) {
		kvKey = key;		

		consul = Consul.builder().withUrl(url).build();
		kvClient = consul.keyValueClient();
		sessionClient = consul.sessionClient();	

		SessionCreatedResponse response = sessionClient.createSession(ImmutableSession.builder().lockDelay(lockDelay + "s").ttl(lockTtl + "s").build());	
		sessionId = response.getId();

		heartbeat = new ConsulHeartbeat();				
		heartbeat.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override	
			public void uncaughtException(Thread t, Throwable e) {
				LOGGER.error("Lock lost due to inactivity");
				System.exit(1);
			}
		});

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				LOGGER.error("Releasing Consul lock during shutdown sequence");
				ConsulLock.releaseSession(true);
			}
		});
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
			sessionRefresh = heartbeatStart = 0;
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

		Value v = kvClient.getValue(kvKey).orNull();
		return v != null && sessionId != null && sessionId.equals(v.getSession().orNull());
	}

	/**
	 * Set whether the heartbeat thread should renew the session
	 * @param b
	 */
	public static void setSessionPendingRenewal(boolean b) {
		pendingRefresh.set(b);
	}

	/**
	 * Return whether or not the session was voted to be renewed
	 * @return boolean
	 */
	public static boolean isSessionPendingRenewal() {
		return (sessionRefresh + (Math.floor(lockTtl * 1000 / 8)) <= System.currentTimeMillis()) && pendingRefresh.get();
	}

	/**
	 * Renew the session to keep it alive
	 */
	public static void renewSession() throws ConsulException {
		if (sessionClient == null)
			throw new ConsulException("SessionClient not initialized");


		sessionClient.renewSession(sessionId);
		sessionRefresh = System.currentTimeMillis();
		setSessionPendingRenewal(false);
	}

	/**
	 * Keep track of whether or not we should attempt to fire off a heartbeat event
	 */
	public static boolean isHeartbeatInterval() {
		if (heartbeatStart + (Math.floor(lockTtl * 1000 / 4))  <= System.currentTimeMillis()) {
			heartbeatStart = System.currentTimeMillis();
			return true;
		}

		return false;
	}
}