package com.citi.credit.gateway.recon;

import java.util.Hashtable;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.citi.credit.gateway.data.CGException;
import com.citi.credit.gateway.service.CreditGatewayInterface;
import com.citi.credit.gateway.service.impl.CreditGatewayImpl;

/*
 * History
 * 10/21/08		kt60981		Initial check-in
 */

public class CalypsoCallableHandler implements Callable {
	
	private CalypsoTradesDAO dao = null;
	private CreditGatewayInterface gateway = null;
	private String instance = null;
	private String login = null;
	private String passwd = null;
	private String query = null;
	private int retryCnt;
	private long delay;
	
	private final static Logger logger = Logger.getLogger("com.citi.credit.gateway.recon.CalypsoCallableHandler");
	public CalypsoCallableHandler(CalypsoTradesDAO dao, String instance, String login, String passwd, String query, int retry, long delay) {
		this.dao = dao;
		this.instance = instance;
		this.login = login;
		this.passwd = passwd;
		this.query = query;
		this.retryCnt = retry;
		this.delay = delay;
	}
	
	public Hashtable call() {
		Hashtable trades = null;
		
		try {
			long startTime = System.currentTimeMillis();
			Thread.currentThread().sleep(2*1000);
			//gateway = new CreditGatewayImpl(instance, login, passwd);//Commented-ss78999
			trades = dao.getTrades(gateway, query);
			long endTime = System.currentTimeMillis();
			System.out.println("Srini Calypso Callable Handler qury----> "+query+"\n trade size----->"+trades.size()+" \n Time taken ---->"+(endTime-startTime)/1000d);
		} catch (InterruptedException e) {
			logger.warn("Caught InterruptedException '" +  e.getMessage() + "'",  e);
		} catch (Exception e) {//modified here-ss78999
			logger.warn("Caught Exception retrieveing Calypso Recon Trades '" + e.getMessage() + "'", e);
			closeConnection(gateway);
			trades = retry();
		} finally {
			closeConnection(gateway);
		}
		return trades;
	}

	private Hashtable retry() {
		Hashtable result = null;
		System.out.println("Srini CalypsoCallableHandler retry");
		for (int i=1;i<=retryCnt;i++) {
			try {
				System.out.println("Srini CalypsoCallableHAndler retry");
				Thread.currentThread().sleep(i*delay);
				//gateway = new CreditGatewayImpl(instance, login, passwd);//Commented-ss78999
				dao = new CalypsoTradesDAO();
				result = dao.getTrades(gateway, query);
				break;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {//modified here-ss78999
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				logger.debug("Trades Retry[" + i + "] : " + (result != null ? result.size() : 0) + " query='" + query + "'");
				closeConnection(gateway);
			}
		}
		
		return result;
	}

	private void closeConnection(CreditGatewayInterface gateway) {
		if (gateway!=null) {
			try {
				gateway.closeGateway();
				gateway = null;
			} catch (CGException e) {
				logger.warn("Caught Exception closing gateway '" + e.getMessage() + "'", e);
			}
		}
		closeDAO(dao);
	}
	
	private void closeDAO(CalypsoTradesDAO dao) {
		if (dao!=null) {
			dao = null;
		}
	}
}
