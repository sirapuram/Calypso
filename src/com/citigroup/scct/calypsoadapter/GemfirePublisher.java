package com.citigroup.scct.calypsoadapter.publisher;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.jms.*;
import javax.xml.bind.JAXBElement;

import org.apache.log4j.Logger;

import com.calypso.tk.core.PersistenceException;
import com.calypso.tk.core.Product;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.util.StreamingTradeArray;
import com.calypso.tk.util.TradeArray;
import com.citigroup.scct.calypsoadapter.AdapterUserSession;
import com.citigroup.scct.calypsoadapter.CalypsoAdapterConstants;
import com.citigroup.scct.calypsoadapter.CollectorException;
import com.citigroup.scct.calypsoadapter.DAOException;
import com.citigroup.scct.calypsoadapter.PublisherException;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.QueryScctReconResponseType;
import com.citigroup.scct.cgen.QueryScctReconType;
import com.citigroup.scct.cgen.QueryScctTradeArraysResponseType;
import com.citigroup.scct.cgen.QueryScctTradeGemfireResponseType;
import com.citigroup.scct.cgen.QueryScctTradeGemfireType;
import com.citigroup.scct.cgen.QueryScctTradeResponseType;
import com.citigroup.scct.cgen.QueryStaticDataResponseType;
import com.citigroup.scct.cgen.QueryTickerResponseType;
import com.citigroup.scct.cgen.ScctErrorType;
import com.citigroup.scct.cgen.ScctReconType;
import com.citigroup.scct.cgen.ScctTickerType;
import com.citigroup.scct.cgen.ScctTradeType;
import com.citigroup.scct.util.CalypsoDAO;
import com.citigroup.scct.util.ScctUtil;
import com.citigroup.scct.util.StringUtilities;

/*
 * Change History (most recent first):
 *		
 *		10/08/2008	kt60981		:	Gemfire/Calypso Publisher
 */

public class GemfirePublisher extends AbstractPublisher implements BatchPublisher {

	private static final Logger logger = Logger.getLogger("com.citigroup.scct.calypsoadapter.publisher.GemfirePublisher");
	
	private static int REMOTE_BATCH_SIZE = 1000;
	
	AdapterUserSession userSession;
	MessageProducer producer;
	Session session;
	Destination dest;
	String region;
	ObjectFactory factory;
	int totTradesCnt;
	
	static {
		String tmp = System.getProperty(CalypsoAdapterConstants.REMOTE_BATCH_SIZE);
		if (StringUtilities.isFilled(tmp)) {
			try {
				REMOTE_BATCH_SIZE = Integer.parseInt(tmp);
			} catch (NumberFormatException e) {
				logger.warn("Unable to parse 'remoteBatchSize' : " + tmp, e);
			}
		}
	}
	
	public GemfirePublisher() {}

	public GemfirePublisher(AdapterUserSession userSession, MessageProducer producer, Session session, Destination dest, String region, ObjectFactory factory) {
		this.userSession = userSession;
		this.producer = producer;
		this.session = session;
		this.dest = dest;
		this.region = region;
		this.factory = factory;
	}
	
	// not implemented for now
	public void publish(JAXBElement response, MessageProducer producer, Session session, Destination dest, String region) {
	}


	public void publish(List l, int batchNo) throws PublisherException {
		int[] arr = ScctUtil.listToNativeIntArray(l);
		if (arr!=null && arr.length>0) {
			try {
				TradeArray tradeArray = userSession.getDsConnection().getDSConnection().getRemoteTrade().getTrades(arr);
				int size = (tradeArray!=null ? tradeArray.size() : 0);
				if (tradeArray!= null && (tradeArray.size()) > 0) {
					List trades = new ArrayList();
					for (int j = 0; j<size; j++) {
						ScctTradeType strade = userSession.convertRawTrade(tradeArray.get(j));
						if (strade != null) {
							trades.add(strade);
						}
					}
					bindAndPublish(batchNo, trades, producer, session, dest, region, factory);
				}
			} catch (RemoteException e) {
				String msg = "Caught Exception getting Remote Trades '" + e.getMessage(); 
				logger.warn(msg, e);
				throw new PublisherException(msg, e);
			} catch (CollectorException e) {
				String msg = "Caught Exception Collecting Trades '" + e.getMessage(); 
				logger.warn(msg, e);
				throw new PublisherException(msg, e);
			} catch (Throwable e) {
				String msg = "Caught Calypso Exception Collecting Trades '" + e.getMessage(); 
				logger.warn(msg, e);
				throw new PublisherException(msg, e);
			}
		}
		
	}
	
	public void publish(int [] results, Object obj) {
		
		ObjectFactory factory = ScctUtil.getObjectFactory();
		
		logger.debug("Attempting to process raw cnt : " + (results!=null ? results.length : 0));
		
		long start = System.currentTimeMillis();
		List tradesToPublish = new ArrayList();
		try {
			
			ArrayList raw = (ArrayList) ScctUtil.nativeIntArrayToList(results);
			if (raw!=null && raw.size()>0) {
				totTradesCnt = raw.size();
				BatchCollector collector = new BatchCollector(raw, REMOTE_BATCH_SIZE);
				totBatchCnt = (totTradesCnt%batchSize==0 ? totTradesCnt/batchSize : totTradesCnt/batchSize+1);
				totMesgCnt = raw.size();
				collector.collect(this);
			} else {
				publishEmptyMessage(producer, session, dest, region, "");
			}
		} catch (CollectorException e) {
			logger.warn("Caught Exception Collecting Trades '" + e.getMessage() + "'", e);
			publishErrorMessage(producer, session, dest, region, e.getMessage());
		}
		

		long end = System.currentTimeMillis();
		logger.debug("Time to stream Trade Array rawCnt : " + totTradesCnt + " batchSz : " + batchSize + " time : " + (end-start)/1000.0);
	}

	private void bindAndPublish(int remoteBatch, Collection rawTrades, MessageProducer producer, Session session, Destination dest, String region, ObjectFactory factory) throws CollectorException, PublisherException {

		ArrayList trades = new ArrayList();
		trades.addAll(rawTrades);
		
		BatchCollector batch = new BatchCollector(trades, batchSize);
		List batches = null;
 		batches = batch.collect();
		
		if (batches !=null && batches.size()>0) {
			ScctTradeType[] src = (ScctTradeType []) trades.toArray(new ScctTradeType[0]);
			Iterator itr = batches.iterator();
			int offset = 0;
			while (itr.hasNext()) {
				ArrayList l = (ArrayList) itr.next();
				if (l!=null && l.size()>0) {
					currMesgCnt = l.size();
					QueryScctTradeResponseType response = factory.createQueryScctTradeResponseType();
					JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(response);
					ScctTradeType[] tgtArr = new ScctTradeType[l.size()];
					System.arraycopy(src, offset, tgtArr, 0, tgtArr.length);
					Collection toBePublished = Arrays.asList(tgtArr);
					response.getScctTrade().addAll(toBePublished);
					offset += l.size();
					try {
						Message message = createTextMessage(outer, session, region,
								String.valueOf(totBatchCnt), String.valueOf(currBatchCnt), 
								String.valueOf(totBatchCnt), String.valueOf(currBatchCnt));
						send(producer, dest, message);
						logger.debug("publishing batch[" + remoteBatch + "] total : " + totTradesCnt + " totBatchCnt : " + totBatchCnt + " currBatchCnt : " + currBatchCnt + " currMesgCnt : " + currMesgCnt);
					} 
					catch(InvalidDestinationException ide){
						logMessage();
					}
					catch (Exception e) {
						logger.warn("Caught Exception streaming batch[" + currBatchCnt + "]" + " results." + e.getMessage(), e);
						throw new PublisherException(e.getMessage(), e);
					} finally {
						currBatchCnt++;
					}
				}
			}
		}
	}
	
	public void publishEmptyMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) {
		ObjectFactory factory = new ObjectFactory();
		QueryScctTradeResponseType response = factory.createQueryScctTradeResponseType();
		JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(response);
		try {
			Message message = createTextMessage(outer, session, region,
					String.valueOf(0), String.valueOf(0), 
					String.valueOf(0), String.valueOf(0));
			send(producer, dest, message);
		} 
		catch(InvalidDestinationException ide){
			logMessage();
		}
		catch (Exception e) {
			logger.warn("Caught Exception streaming empty message '" + e.getMessage(), e);
		}
	}

	public void publishErrorMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) {
		ObjectFactory factory = new ObjectFactory();
		ScctErrorType errorResp = factory.createScctErrorType();
		JAXBElement<ScctErrorType> outer = factory.createScctError(errorResp);
		errorResp.setCode("-1");
		errorResp.setText(mesg);

		try {
			Message message = createTextMessage(outer, session, region,
					String.valueOf(0), String.valueOf(0), 
					String.valueOf(0), String.valueOf(0));
			send(producer, dest, message);
		} catch(InvalidDestinationException ide){
			logMessage();
		}
		catch (Exception e) {
			logger.warn("Caught Exception streaming error message '" + e.getMessage(), e);
		}
	}
	
	private void resetArray(Object[] a) {
		if (a!=null && a.length>0) {
			for (int i=0;i<a.length;i++) {
				a[i] = null;
			}
		}
		a = null;
	}
}

