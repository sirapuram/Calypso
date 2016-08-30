package com.citigroup.scct.calypsoadapter.publisher;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.jms.*;
import javax.xml.bind.JAXBElement;

import org.apache.log4j.Logger;

import com.calypso.tk.core.PersistenceException;
import com.calypso.tk.core.Trade;
import com.calypso.tk.util.StreamingTradeArray;
import com.calypso.tk.util.TradeArray;
import com.citigroup.scct.calypsoadapter.AdapterUserSession;
import com.citigroup.scct.calypsoadapter.CalypsoAdapterConstants;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.QueryScctTradeArraysResponseType;
import com.citigroup.scct.cgen.QueryScctTradeResponseType;
import com.citigroup.scct.cgen.QueryStaticDataResponseType;
import com.citigroup.scct.cgen.QueryTickerResponseType;
import com.citigroup.scct.cgen.ScctErrorType;
import com.citigroup.scct.cgen.ScctTickerType;
import com.citigroup.scct.cgen.ScctTradeType;
import com.citigroup.scct.util.ScctUtil;
import com.citigroup.scct.util.StringUtilities;

/*
 * Jul 15'08    Publish empty mesg when no trades are converted
 * May 29'08	Add Remote Exception Handling - If remote trades failed, send a ScctError to client and bail... 
 */

public class TradePublisher extends AbstractPublisher {

	private static final Logger logger = Logger.getLogger("com.citigroup.scct.calypsoadapter.publisher.TradePublisher");
	
	private static int REMOTE_BATCH_SIZE = 1000;
	
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

	public void publish(JAXBElement response, MessageProducer producer, Session session, Destination dest, String region) throws Exception {
		
		Object object = response.getValue();

		if (object ==null) {
			logger.warn("Unable to stream results, response is null");
			return;
		}
		
		int offset = 0;
		ObjectFactory factory = ScctUtil.getObjectFactory();
		
		long start = System.currentTimeMillis();
		
		if (object instanceof QueryScctTradeArraysResponseType) {
			QueryScctTradeArraysResponseType results = (QueryScctTradeArraysResponseType) object;
			List<ScctTradeType> trades = results.getScctTrade();
			ScctTradeType [] srcArr = trades.toArray(new ScctTradeType[0]);

			totMesgCnt = srcArr.length;
			totBatchCnt = (srcArr.length / batchSize) + 1;

			for (int i=0;i<totBatchCnt; i++) {                           
				QueryScctTradeArraysResponseType tradeArraysResponse = factory.createQueryScctTradeArraysResponseType();
				JAXBElement<QueryScctTradeArraysResponseType> outer = factory.createQueryScctTradeArraysResponse(tradeArraysResponse);
				if (i < (totBatchCnt-1)) {
					currMesgCnt = batchSize;
					ScctTradeType [] destArr = new ScctTradeType[batchSize];
					System.arraycopy(srcArr, offset, destArr, 0, batchSize);
					tradeArraysResponse.getScctTrade().addAll(Arrays.asList(destArr));
					offset += batchSize;
				} else if (i==(totBatchCnt-1)) {
					currMesgCnt = srcArr.length % batchSize;
					ScctTradeType [] destArr = new ScctTradeType[currMesgCnt]; 
					System.arraycopy(srcArr, offset, destArr, 0, currMesgCnt);
					tradeArraysResponse.getScctTrade().addAll(Arrays.asList(destArr));
				}
				currBatchCnt = i;
				try {
					Message message = createTextMessage(outer, session, region, String.valueOf(totBatchCnt), String.valueOf(currBatchCnt), String.valueOf(totBatchCnt), String.valueOf(currBatchCnt) );
					send(producer, dest, message);
				} catch(InvalidDestinationException ide){
					logMessage();
				}catch (Exception e) {
					logger.warn("Caught Exception streaming results : " + e.getMessage(), e);
					throw e;
				}
			}
		} else if (object instanceof QueryScctTradeResponseType) {
			QueryScctTradeResponseType results = (QueryScctTradeResponseType) object;
			List<ScctTradeType> trades = results.getScctTrade();
			ScctTradeType[] srcArr = trades.toArray(new ScctTradeType[0]);
			
			totMesgCnt = srcArr.length;
			totBatchCnt = (srcArr.length/batchSize) + 1;
			
			for (int i=0; i<totBatchCnt; i++) {
				QueryScctTradeResponseType tradeResponse = factory.createQueryScctTradeResponseType();
				JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(tradeResponse);
				if (i < (totBatchCnt-1)) {
					currMesgCnt = batchSize;
					ScctTradeType [] destArr = new ScctTradeType[batchSize];
					System.arraycopy(srcArr, offset, destArr, 0, batchSize);
					tradeResponse.getScctTrade().addAll(Arrays.asList(destArr));
					offset += batchSize;
				} else if (i==(totBatchCnt-1)) {
					currMesgCnt = srcArr.length % batchSize;
					ScctTradeType [] destArr = new ScctTradeType[currMesgCnt]; 
					System.arraycopy(srcArr, offset, destArr, 0, currMesgCnt);
					tradeResponse.getScctTrade().addAll(Arrays.asList(destArr));
				}
				currBatchCnt = i;
				try {
					Message message = createTextMessage(outer, session, region, String.valueOf(totBatchCnt), String.valueOf(currBatchCnt), String.valueOf(totBatchCnt), String.valueOf(currBatchCnt) );
					send(producer, dest, message);
				} catch(InvalidDestinationException ide){
					logMessage();
				}catch (Exception e) {
					logger.warn("Caught Exception streaming results : " + e.getMessage(), e);
					throw e;
				}
			}
		}

		long end = System.currentTimeMillis();
		logger.debug("Time to stream " + response.getValue() + " totalCnt : " + totMesgCnt + " batchSz : " + batchSize + " time : " + (end-start)/1000.0);
	}

	public void publish(int [] results, AdapterUserSession userSession, MessageProducer producer, Session session, Destination dest, String region) throws Exception{
		
		ObjectFactory factory = ScctUtil.getObjectFactory();
		
		long start = System.currentTimeMillis();
		totMesgCnt = results.length;
		int remoteBatch; 
		if (totMesgCnt % REMOTE_BATCH_SIZE==0) {
			remoteBatch = totMesgCnt / REMOTE_BATCH_SIZE;
		} else {
			remoteBatch = totMesgCnt / REMOTE_BATCH_SIZE + 1;
		}
		if (results !=null && results.length>0) {
			if (results.length%batchSize==0) {
				totBatchCnt = (results.length / batchSize);
			} else {
				totBatchCnt = (results.length / batchSize) + 1;
			}
		}
		int converted = 0;
		int offset = 0;
		
		TradeArray destArr = new TradeArray();
		List trades = new ArrayList();
		
		logger.debug("Attempting to process raw cnt : " + totMesgCnt);
		boolean quit = false;
		
		for (int i=0;i<remoteBatch;i++) {
			if (!quit) {
				TradeArray rawTrades = null;
				try {
					int[] tmp = null;
					if (i == (remoteBatch - 1)) {
						int remaining = ((totMesgCnt % REMOTE_BATCH_SIZE==0 ? REMOTE_BATCH_SIZE : (totMesgCnt%REMOTE_BATCH_SIZE)));
						tmp = new int[remaining];
						System.arraycopy(results, offset, tmp, 0, remaining);
					} else {
						tmp = new int[REMOTE_BATCH_SIZE];
						System.arraycopy(results, offset, tmp, 0,
								REMOTE_BATCH_SIZE);
						offset += REMOTE_BATCH_SIZE;
					}
					long x = System.currentTimeMillis();
					rawTrades = userSession.getDsConnection().getDSConnection()
							.getRemoteTrade().getTrades(tmp);
					long y = System.currentTimeMillis();
					tmp = null;
					logger.debug("Time to retrieve batch[" + i + "] : "
							+ (y - x) / 1000.0);
				} catch (RemoteException e) {
					quit = true;
					logger.warn("Unable to retrieve trades batch[" + i + "]",e);
					publishErrorMessage(producer, session, dest, region, e.getMessage());
				} catch (Throwable e) {
					quit = true;
					logger.warn("Caught Runtime Error. Unable to retrieve trades batch[" + i + "]",e);
					publishErrorMessage(producer, session, dest, region, e.getMessage());
				}
				// convert Calypso Trade to ScctTrade and add to trades
				int rawCnt = 0;
				long x = System.currentTimeMillis();
				if (rawTrades != null && (rawCnt = rawTrades.size()) > 0) {
					for (int j = 0; j < rawCnt; j++) {
						ScctTradeType strade = userSession.convertRawTrade(rawTrades.get(j));
						if (strade != null) {
							trades.add(strade);
						}
					}
				long y = System.currentTimeMillis();
				logger.debug("Time to convert [" + i + "] : " + (y - x) / 1000.0);
				
					converted += trades.size();
					if (trades.size() > 0) {
						bindAndPublish(i, trades, producer, session, dest,
								region, factory);
					}
					trades.clear();
					rawTrades.clear();
					logger.debug("Converted : " + converted);
				}
			}			
		}
		if (converted == 0) {
			publishEmptyMessage(producer, session, dest, region, "");
		}
			
		long end = System.currentTimeMillis();
		logger.debug("Time to stream Trade Array rawCnt : " + totMesgCnt + " batchSz : " + batchSize + " time : " + (end-start)/1000.0);
	}
	
	private void bindAndPublish(int batchNo, Collection trades, MessageProducer producer, Session session, Destination dest, String region, ObjectFactory factory) throws Exception{
		int size = trades.size();
		int sBatch = 0;
		if (size%batchSize==0) {
			sBatch = size/batchSize;
		} else {
			sBatch = (size/batchSize) + 1;
		}
		int offset = 0;
		
		for (int i = 0; i < sBatch; i++) {
			if (i != (sBatch-1)) {
				currMesgCnt = batchSize;
				QueryScctTradeResponseType tradeResponse = factory.createQueryScctTradeResponseType();
				JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(tradeResponse);
				ScctTradeType[] tgtArr = new ScctTradeType[currMesgCnt];
				System.arraycopy(trades.toArray(new ScctTradeType[0]), offset, tgtArr, 0, currMesgCnt);
				tradeResponse.getScctTrade().addAll(Arrays.asList(tgtArr));
				try {
					Message message = createTextMessage(outer, session, region,
							String.valueOf(totBatchCnt), String.valueOf(currBatchCnt), 
							String.valueOf(totBatchCnt), String.valueOf(currBatchCnt));
					send(producer, dest, message);
				} catch(InvalidDestinationException ide){
					logMessage();
				}catch (Exception e) {
					logger.warn("Caught Exception streaming batch[" + currBatchCnt + "]" + " results." + e.getMessage(), e);
					throw e;
				}
				offset += batchSize;
				tgtArr = null;
				outer.getValue().getScctTrade().clear();
			} else {
				if (size%batchSize!=0) {
					currMesgCnt = (size % batchSize);
				} else {
					currMesgCnt = batchSize;
				}
				
				QueryScctTradeResponseType tradeResponse = factory.createQueryScctTradeResponseType();
				JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(tradeResponse);
				ScctTradeType[] tgtArr = new ScctTradeType[currMesgCnt];
				System.arraycopy(trades.toArray(new ScctTradeType[0]), offset, tgtArr,0, currMesgCnt);
				tradeResponse.getScctTrade().addAll(Arrays.asList(tgtArr));
				try {
					Message message = createTextMessage(outer, session, region,
														String.valueOf(totBatchCnt),String.valueOf(currBatchCnt), 
														String.valueOf(totBatchCnt), String.valueOf(currBatchCnt));
					send(producer, dest, message);
				} catch(InvalidDestinationException ide){
					logMessage();
				}catch (Exception e) {
					logger.warn("Caught Exception streaming batch[" + currBatchCnt + "]" + " results." + e.getMessage(), e);
					throw e;
				}
				tgtArr = null;
				outer.getValue().getScctTrade().clear();
			}
			logger.debug("publishing batch[" + batchNo + "] total : " + totMesgCnt + " currCnt : " + currMesgCnt + " totBatchCnt : " + totBatchCnt + " currBatchCnt : " + currBatchCnt + " currMesgCnt : " + currMesgCnt);
			currBatchCnt++;
		}
	}
	
	public void publishEmptyMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctTradeResponseType tradeResponse = factory.createQueryScctTradeResponseType();
		JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(tradeResponse);
		try {
			Message message = createTextMessage(outer, session, region,
					String.valueOf(0), String.valueOf(0), 
					String.valueOf(0), String.valueOf(0));
			send(producer, dest, message);
		} catch(InvalidDestinationException ide){
			logMessage();
		}catch (Exception e) {
			logger.warn("Caught Exception streaming batch[" + currBatchCnt + "]" + " results." + e.getMessage(), e);
			throw e;
		}
	}

	public void publishErrorMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception{
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
		}catch (Exception e) {
			logger.warn("Caught Exception streaming batch[" + currBatchCnt + "]" + " results." + e.getMessage(), e);
			throw e;
		}
	}

	public void publish2(StreamingTradeArray results, AdapterUserSession userSession, MessageProducer producer, Session session, Destination dest, String region) throws Exception{
		
		ObjectFactory factory = ScctUtil.getObjectFactory();
		
		long start = System.currentTimeMillis();
		totMesgCnt = results.size();
		int converted = 0;
		int currBatch = 0;
		
		TradeArray destArr = new TradeArray();
		Set trades = new HashSet();
		Iterator itr = results.iterator();
		logger.debug("Attempting to process raw cnt : " + totMesgCnt);
		
		Trade trade = null; 
		while (itr.hasNext()) {
			trade = (Trade) itr.next();
			ScctTradeType strade = userSession.convertTrade(trade, true);
			if (strade!=null) {
				trades.add(strade);
				converted++;
			}
			if (trades.size()!=0 && trades.size()%batchSize==0) {
				bindAndPublish(currBatch, trades, producer, session, dest, region, factory);
				currBatch++;
				trades.clear();
			}
		}
			
		long end = System.currentTimeMillis();
		logger.debug("Time to stream Trade Array rawCnt : " + totMesgCnt + " converted : " + converted + " batchSz : " + batchSize + " time : " + (end-start)/1000.0);
	}
}

