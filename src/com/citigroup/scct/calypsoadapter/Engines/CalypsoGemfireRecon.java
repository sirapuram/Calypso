package com.citi.credit.gateway.recon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.citi.credit.gateway.core.CGConstants;
import com.citi.credit.gateway.data.CGException;
import com.citi.credit.gateway.exception.ConfigException;
import com.citi.credit.gateway.exception.ReloadException;
import com.citi.credit.gateway.service.CreditGatewayInterface;
import com.citi.credit.gateway.service.impl.CreditGatewayImpl;
import com.citi.credit.gateway.util.CGUtilities;
import com.citi.rds.model.EntityRegisterHelper;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.QueryScctReconType;
import com.citigroup.scct.cgen.ScctCalypsoConfigType;
import com.citigroup.scct.cgen.ScctGemfireConfigType;
import com.citigroup.scct.cgen.ScctReconConfigEnvType;
import com.citigroup.scct.cgen.ScctReconTermType;

/*
 * History:
 *  10/14/2010   pk00001		Gemfire Fee Recon + Performace Improvement
 *  09/21/2010   pk00001		Gemfire Recon reorg + adding new fields in recon report 
 * 	DF - 07/31/2009 modified from GemfireReconClient
 */
public class CalypsoGemfireRecon {

	private static final int MAX_WAIT_TIME_FOR_LOAD = 1000 * 60 * 1; // 1 min 
	private static final Logger logger = Logger.getLogger("com.citi.credit.gateway.recon.CalypsoGemfireRecon");

	static long DT_TLRNC = 0L;
	public static Date reportStartDate = null;

	/*private static List<String> loadBookStrategy(String str) {
		String list = "020, 023, 032, 046, 050, 054, 055, 056, 05E, 060, 062, 064, 067, 070, 071, 072, 074, 076, 079, 07F, 07L, 080, 081";
		List<String> result = new ArrayList<String>();
		StringTokenizer token = new StringTokenizer(list,",");
		while(token.hasMoreTokens()) {
			result.add(token.nextToken().trim());
		}
		return result;
	}*/
	
	private static List<String> loadBookStrategy() {
		List<String> result = new ArrayList<String>();
		try {
			CalypsoLocalDAO dao = new CalypsoLocalDAO();
			result = dao.getAllBookStrategy();
		} catch (Exception e) {
			logger.error("loadBookStrategy: Failed to get strategy(es).EXIT ");
			e.printStackTrace();
			System.exit(1);
		}
		return result;
	}

	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd k:m:s");
	    //Date dNow = new Date(); 
		reportStartDate = new Date();
		System.out.println("Started Execution at "+ sdf.format(reportStartDate));
		String platform = configureLog();
		System.out.println("#### PLATFORM : '" + platform);

		EntityRegisterHelper entityHelper = new com.citi.rds.model.EntityRegisterHelper();
		entityHelper.registerClassIds();
		
		ReconResult reconResult = null;
		try {
			ReconConfiguration reconConfig = new ReconConfiguration(args, platform);
			CalypsoLocalDAO.init(reconConfig.getCalypsoDBLogin(), reconConfig.getCalypsoDBPass(), 
					reconConfig.getCalypsoDBDriver(), reconConfig.getCalypsoDBURL());
			
			ExecutorService exCalExecutor = Executors.newFixedThreadPool(reconConfig.getThreadCount(), new ThreadFactory() {
				public Thread newThread(Runnable paramRunnable) {
					return new Thread(paramRunnable, "ReconSubTaskThread");
				}
			});
			List<ReconSubTask> subTasks = new Vector<ReconSubTask>();
			
			if (reconConfig.getDefaultGfeQuery()) {
				handleLiveTrades(reconConfig, exCalExecutor, subTasks);
				handleTerminatedTrades(reconConfig, exCalExecutor, subTasks);
			} else {
				handleQueriesFromConfigFile(reconConfig, exCalExecutor, subTasks);
			}
			
			exCalExecutor.shutdown();
			
			while(!exCalExecutor.isTerminated()) {
				try {
					Thread.sleep(3*1000);
					//logger.debug("CalypsoGemfireRecon-mainMethod : Waiting for all SubTasks to complete");
				} catch (InterruptedException e) {
					logger.warn("CalypsoGemfireRecon-mainMethod : Caught InterruptedException " + e.getMessage(), e);
				}
			}
			
			reconResult = aggregateResult(subTasks);
			
			crossStrategyCheck(reconResult);

			printMissingCalypsoTrades(reconResult.getCalypsoTradesMissingInGemfire());
			
			//below method call commenting for production, its here for testing only
			//printMissingGemfireTrades(reconResult.getGemfireTradesMissingInCalypso());
			
			printMismatchTrades(reconConfig.getsRepFileName(), reconConfig.getPrintToFile(), reconResult);
			
			reloadMismatchTrades(reconConfig, reconResult);
			
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			e.printStackTrace();
		} finally {
			if(reconResult!=null) {
				logger.debug("+++++++++ Calypso - Gemfire Reconcialiation Statistics : ++++++++++");
				logger.debug("Gemfire Loading Time : " + reconResult.getGemfireLoadingTime());
				logger.debug("Calypso Loading Time : " + reconResult.getCalypsoLoadingTime());
				logger.debug("Reconciliation  Time : " + reconResult.getComareTime());
				logger.debug("===================================================================");
			} else {
				logger.error("Something wrong Please check all log file for suspects");
			}
			try {
				CalypsoLocalDAO.closeAllConnections();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	
	private static void handleQueriesFromConfigFile(ReconConfiguration reconConfig,  
			ExecutorService exCalExecutor, List<ReconSubTask> subTasks) throws Exception {
		List<QueryScctReconType> calQueries = getClpQueriesForSpeicifiedConfig(reconConfig);
		List<String> gfeQueries = getGemfireQueriesForSpeicifiedConfig(reconConfig);
		List<QueryScctReconType> calFeeQueries = getClpFeeQueriesForSpeicifiedConfig(reconConfig);
		
		ReconSubTask task = new ReconSubTask(gfeQueries, calQueries, calFeeQueries, reconConfig);
		subTasks.add(task);
		exCalExecutor.submit(task);
	}

	private static void handleTerminatedTrades(ReconConfiguration reconConfig,
			ExecutorService exCalExecutor, List<ReconSubTask> subTasks) throws ConfigException {
		// for all terminated trades for all strtg
		List<String> gfeQueries = getGfeQueriesForTeminatedTrades(reconConfig);
		List<QueryScctReconType> calQueries = getClpQueriesForTerminatedTrade(reconConfig);
		List<QueryScctReconType> calypsoFeeQueries = getClpFeeQueriesForTerminatedTrade(reconConfig);
		
		ReconSubTask task = new ReconSubTask(gfeQueries, calQueries, calypsoFeeQueries, reconConfig);
		subTasks.add(task);
		exCalExecutor.submit(task);
	}

	private static void handleLiveTrades(ReconConfiguration reconConfig, 
			ExecutorService exCalExecutor, List<ReconSubTask> subTasks) throws ConfigException {
		List<String> strategies = loadBookStrategy();
		System.out.println(strategies);
		List<String[]> strtgySets = strategyBreakIntoSet(strategies, reconConfig.getStrategyBlockSize());
		
		for (String[] stragegy: strtgySets) {
			String[] stg = stragegy;
			List<String> gemfireQueries = getGfeQueries(stg, reconConfig);
			List<QueryScctReconType> calypsoQueries = getClpQueriesForLiveTrades(stg, reconConfig);
			List<QueryScctReconType> calypsoFeeQueries = getClpFeeQueriesForLiveTrades(stg, reconConfig);
			
			ReconSubTask task = new ReconSubTask(gemfireQueries, calypsoQueries, calypsoFeeQueries, reconConfig);
			subTasks.add(task);
			exCalExecutor.submit(task);
		}
	}
	
	private static ReconResult aggregateResult(List<ReconSubTask> subTasks) {
		ArrayList<String> arMismatch = new ArrayList<String>();
		Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = new HashMap<Integer, TradeReconObject>();
		Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = new HashMap<Integer, TradeReconObject>();
		
		long gfeLoadingTime = 0;
		long reconTime = 0;
		long clpLoadingTime = 0;
		
		for (ReconSubTask aTask : subTasks) {
			ReconResult aResult = aTask.getReconResult();
			arMismatch.addAll(aResult.getMismatchTrades());
			vMissingCalypsoidsInGemfire.putAll(aResult.getCalypsoTradesMissingInGemfire());
			vMissingGFEidsInCalypso.putAll(aResult.getGemfireTradesMissingInCalypso());
			gfeLoadingTime += aResult.getGemfireLoadingTime();
			clpLoadingTime += aResult.getCalypsoLoadingTime();
			reconTime += aResult.getComareTime();
		}
		
		ReconResult aggregateResult = new ReconResult();
		aggregateResult.setCalypsoLoadingTime(clpLoadingTime);
		aggregateResult.setGemfireLoadingTime(gfeLoadingTime);
		aggregateResult.setComareTime(reconTime);
		aggregateResult.setCalypsoTradesMissingInGemfire(vMissingCalypsoidsInGemfire);
		aggregateResult.setGemfireTradesMissingInCalypso(vMissingGFEidsInCalypso);
		aggregateResult.setMismatchTrades(arMismatch);
		return aggregateResult;
	}

	private static String configureLog() {
		String platform = System.getProperty("os.name", "Windows");
		if (platform.startsWith("Windows")) {
			PropertyConfigurator.configure(CGConstants.LOG_FILE);
		} else {
			PropertyConfigurator.configure("../config/log4j.properties");
		}
		return platform;
	}
	
	private static void crossStrategyCheck(ReconResult reconResult) {
		Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = reconResult.getCalypsoTradesMissingInGemfire();
		Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = reconResult.getGemfireTradesMissingInCalypso();
		ArrayList<String> arMismatch = reconResult.getMismatchTrades();
		int count = 0;
		if (vMissingCalypsoidsInGemfire.size() > 0) {
			// Print header
			int iMaxLength = 80;
			StringBuffer sbLine = new StringBuffer();
			Iterator<Map.Entry<Integer, TradeReconObject>> iter = vMissingCalypsoidsInGemfire.entrySet().iterator();
			while(iter.hasNext()) {
				Map.Entry<Integer, TradeReconObject> entry = iter.next();
				Integer missingCalypsoId = entry.getKey();
				if(vMissingGFEidsInCalypso.containsKey(missingCalypsoId)) {
					TradeReconObject clptrade = entry.getValue();//vMissingCalypsoidsInGemfire.get(missingCalypsoId);
					TradeReconObject gfetrade = vMissingGFEidsInCalypso.get(missingCalypsoId);
					ArrayList<String> mismatchValues = compareTwoTradeReconObject(clptrade, gfetrade);
					  if(mismatchValues!=null && mismatchValues.size()>0) {
						  arMismatch.addAll(mismatchValues);  
					  }
					//vMissingCalypsoidsInGemfire.remove(missingCalypsoId);
					iter.remove();
					vMissingGFEidsInCalypso.remove(missingCalypsoId);
					
					sbLine.append("crossStrategyCheck - Missed in first loop "+missingCalypsoId);
					sbLine.append("Calypso Trade - "+clptrade);
					sbLine.append("Gemfire Trade - "+gfetrade);
					count++;
					if (sbLine.toString().length() >= iMaxLength) {
						// sbLine.append("\n");
						logger.debug(sbLine.toString());
						sbLine.delete(0, sbLine.length());
					}
				}
			}
			// Print the rest if in the buffer
			if (sbLine.toString().length() > 0) {
				String sOut = sbLine.substring(0, sbLine.length() - 2).toString() + "\n";
				logger.debug(sOut);
			}
			logger.debug("crossStrategyCheck - Total trade due to strg logic "+count);
		}
	}

	private static void reloadMismatchTrades(ReconConfiguration recConf, 
			ReconResult reconResult) {
		
		Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = reconResult.getCalypsoTradesMissingInGemfire();
		Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = reconResult.getGemfireTradesMissingInCalypso();
		ArrayList<String> arMismatch = reconResult.getMismatchTrades();
		int batchSz = Integer.parseInt(recConf.getBatchReloadSize());
		if(recConf.reload("reloadGfe") || recConf.reload("reloadClp") || recConf.reload("reloadMismatch")) {
			CreditGatewayInterface gateway = null;
			try {
				gateway = new CreditGatewayImpl(recConf.getGatewayInstance(), recConf.getSzLogin(), recConf.getSzPasswd());
				if (recConf.reload("reloadGfe")) {
					if (vMissingGFEidsInCalypso.size() > 0){
						logger.debug("Reloading Gemfire Trades Missing in Calypso : " + vMissingGFEidsInCalypso.size());
						reloadTrades(gateway, vMissingGFEidsInCalypso.keySet(), batchSz);						
					}
				}

				if (recConf.reload("reloadClp")) {
					if (vMissingCalypsoidsInGemfire.size() > 0){
						logger.debug("Reloading Calypso Trades Missing in Gemfire : " + vMissingCalypsoidsInGemfire.size());
						reloadTrades(gateway, vMissingCalypsoidsInGemfire.keySet(), batchSz);						
					}

				}

				if (recConf.reload("reloadMismatch")) {
					logger.debug("Reloading Mismatch Trades " + arMismatch.size());
					if (arMismatch.size() > 0){
						Set<Integer> vMissMatchTrades = new HashSet<Integer>();
						for (String sMisRecord : arMismatch){
							//logger.debug("reloadMismatch: processing sMissRecord: "+sMisRecord);
							String[] sParts = null;
							sParts = sMisRecord.trim().split("\\|");
							if (!sParts[0].trim().equals("")){
								Integer ITrade = new Integer(sParts[0]);
								if (ITrade instanceof Integer){
									//logger.debug("reloadMismatch: trade id: "+ITrade.intValue()+" added to vMissMatchTrades");
									vMissMatchTrades.add(ITrade);									
								}
							}
						}
						reloadTrades(gateway, vMissMatchTrades, batchSz);					
					}
				}
			} catch (CGException e) {
				logger.error(e.getMessage(), e);
				e.printStackTrace();
			} finally {
				if (gateway != null) {
					try {
						gateway.closeGateway();
					} catch (CGException e) {
						logger.error(e.getMessage(), e);
						e.printStackTrace();
					}
				}
			}
		}
	}

	private static void printMismatchTrades(String sRepFileName, String printToFile, ReconResult reconResult) {
		ArrayList<String> arMismatch = reconResult.getMismatchTrades();
		if (arMismatch.size() > 0) {
			if (printToFile.equals("Y")) {
				reportMismatchTrades(sRepFileName, reconResult);
			} else {
				// Printing Missmatch:
				String arResults[] = new String[arMismatch.size()];
				arResults = arMismatch.toArray(arResults);
				Arrays.sort(arResults);
				// Print header
				logger.debug("Trade Id   Break Type    Calypso Value      Gemfire Value");
				logger.debug("==========================================================");
				for (String sMisRecord : arResults) {
					logger.debug(sMisRecord + "\n");
				}
				logger.debug("==========================================================");
				logger.debug("====== Total breaks # " + arMismatch.size() + " ====");
				logger.debug("======================DONE================================");
			}
		} else {
			logger.debug("No mismatch record(s) found. Printing cancel.");
		}
	}

	private static void printMissingGemfireTrades(Map<Integer, TradeReconObject> vMissingGFEidsInCalypso) {
		if (vMissingGFEidsInCalypso.size() > 0) {
			// Print header
			logger.debug("");
			logger.debug("Trade Ids in Gemfire but missing in Calypso");
			logger.debug("==========================================================");
			logger.debug("");
			int iMaxLength = 80;
			StringBuffer sbLine = new StringBuffer();
			Iterator<Integer> iter = vMissingGFEidsInCalypso.keySet().iterator();
			while (iter.hasNext()) {
				sbLine.append(iter.next()).append(", ");
				if (sbLine.toString().length() >= iMaxLength) {
					// sbLine.append("\n");
					logger.debug(sbLine.toString());
					sbLine.delete(0, sbLine.length());
				}
			}
			// Print the rest if in the buffer
			if (sbLine.toString().length() > 0) {
				String sOut = sbLine.substring(0, sbLine.length() - 2).toString() + "\n";
				logger.debug(sOut);
			}
			logger.debug("TOTAL missing trades in Calypso: " + vMissingGFEidsInCalypso.size());
			logger.debug("END of missing trades in Calypso");
			logger.debug("==========================================================");
			logger.debug("");
		}
	}

	private static void printMissingCalypsoTrades(Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire) {
		// Printing Missing in Gemfire:
		if (vMissingCalypsoidsInGemfire.size() > 0) {
			// Print header
			logger.debug("");
			logger.debug("Trade Ids in Calypso but missing in GFE");
			logger.debug("==========================================================");
			logger.debug("");
			int iMaxLength = 80;
			StringBuffer sbLine = new StringBuffer();
			Iterator<Integer> iter = vMissingCalypsoidsInGemfire.keySet().iterator();
			while(iter.hasNext()) {
				sbLine.append(iter.next()).append(", ");
				if (sbLine.toString().length() >= iMaxLength) {
					// sbLine.append("\n");
					logger.debug(sbLine.toString());
					sbLine.delete(0, sbLine.length());
				}
			}
			// Print the rest if in the buffer
			if (sbLine.toString().length() > 0) {
				String sOut = sbLine.substring(0, sbLine.length() - 2).toString() + "\n";
				logger.debug(sOut);
			}
			logger.debug("TOTAL missing trades in GFE: " + vMissingCalypsoidsInGemfire.size());
			logger.debug("END of missing trades in GFE");
			logger.debug("==========================================================");
			logger.debug("");
		}
	}
	
	private static ArrayList<String> compareTwoTradeReconObject(TradeReconObject ClpTrade, TradeReconObject GfeTrade) {
		return ScctTradeComparator.compare(ClpTrade, GfeTrade );
	}
	
	private static List<String[]> strategyBreakIntoSet(List<String> list, int blockSize) {
		List<String[]> result = new ArrayList<String[]>();
		String arStrategies[] = new String[list.size()];
		arStrategies = list.toArray(arStrategies);
		Arrays.sort(arStrategies);
		int iSplitCount = 0;
		if (arStrategies.length > blockSize) {
			while ((iSplitCount * blockSize) < arStrategies.length) {
				int iLen = 0;
				if ((arStrategies.length) - (iSplitCount * blockSize) >= blockSize) {
					iLen = blockSize;
				} else {
					iLen = (arStrategies.length) - (iSplitCount * blockSize);
				}
				String arDest[] = new String[iLen];
				System.arraycopy(arStrategies, (iSplitCount * blockSize), arDest, 0, iLen);
				result.add(arDest);
				iSplitCount++;
			}
		} else {
			result.add(arStrategies);
		}
		return result;
	}
	
	private static List<String> getGfeQueriesForTeminatedTrades(ReconConfiguration recConf) throws ConfigException {
		List<String> result = new ArrayList<String>();
		ScctReconTermType reconTerm = getTerminationConfig(recConf);
		for (String sProduct : recConf.getvProductList()) {
			StringBuffer buf = new StringBuffer(GemfireDAO.SELECT_RDSI);
			buf.append("where TrdStatus='TERMINATED' ");
			buf.append(" AND FiCDSTyp='" + sProduct + "' ");
			if (recConf.getGemSrc().length() > 0) {
				buf.append(" AND SrcSys = '" + recConf.getGemSrc() + "' ");
			}
			buf.append(" and ValidD >= ").append(getTermOffset(true, reconTerm)).append("L");
			buf.append(" and AsOfD <= ").append(getTermOffset(false, reconTerm)).append("L");
			buf.append(" and ValidD <= ").append(getTermOffset(false, reconTerm)).append("L");
			result.add(buf.toString());
		}
		return result;
	}
	
	private static List<String> getGfeQueries(String[] strategy, 
			ReconConfiguration recConf) throws ConfigException {
		List<String> result = new ArrayList<String>();
		long startTime = reportStartDate.getTime();
		for (String sProduct : recConf.getvProductList()){
			StringBuffer buf = new StringBuffer(GemfireDAO.SELECT_RDSI);
			buf.append("where Strtgy IN SET (");
			for ( String sStg : strategy){
				buf.append("'").append(sStg).append("',");				
			}
			buf.deleteCharAt(buf.length() - 1);
			buf.append(") ");
			buf.append(" AND FiCDSTyp='"+sProduct+"' ");
			if (recConf.getGemSrc().length() > 0){
				buf.append(" AND SrcSys = '"+recConf.getGemSrc()+"' ");						
			}
			buf.append(GemfireDAO.STATUS_ALL);
			// let us get the last RDSI on the trade => [AsOfD <= Long.MAX_VALUE+"L";]
			//buf.append("AND AsOfD <= "+Long.MAX_VALUE+"L");
			//buf.append("AND AsOfD <= (9223372036854775807L)");  
			//buf.append("and ").append(latestRDSI);
			 buf.append("AND AsOfD <= "+startTime+"L");
			logger.debug("### RSDI NON-TERM QUERY '" + buf.toString() + "'");
			result.add(buf.toString());
		}
		return result;
	}
	
	private static List<QueryScctReconType> getClpFeeQueriesForTerminatedTrade(ReconConfiguration recConf) throws ConfigException {
		List<QueryScctReconType> result = new ArrayList<QueryScctReconType>();
		try {
			for(String sProduct: recConf.getvProductList()){
				QueryScctReconType query = getClpFeeQueriesList(true, sProduct, null, false, recConf);
				result.add(query);
			}
		} catch (Exception e) {
			logger.error("getClpQueries: Failed to run Calypso query.EXIT ");
			e.printStackTrace();
		}
		return result;
	}
	
	private static List<QueryScctReconType> getClpQueriesForTerminatedTrade(ReconConfiguration recConf) throws ConfigException {
		List<QueryScctReconType> result = new ArrayList<QueryScctReconType>();
		try {
			for(String sProduct: recConf.getvProductList()){
				QueryScctReconType query = getClpQueryList(true, sProduct, null, false, recConf);
				result.add(query);
			}
		} catch (Exception e) {
			logger.error("getClpQueries: Failed to run Calypso query.EXIT ");
			e.printStackTrace();
		}
		return result;
	}
	
	private static List<QueryScctReconType> getClpFeeQueriesForLiveTrades(String[] arStrategies, ReconConfiguration recConf) {
		List<QueryScctReconType> result = new ArrayList<QueryScctReconType>();
		try {
			for(String sProduct: recConf.getvProductList()){
				QueryScctReconType query = getClpFeeQueriesList(false, sProduct, arStrategies, false, recConf);
				result.add(query);
				query = getClpFeeQueriesList(false, sProduct, arStrategies, true, recConf);
				result.add(query);
			}
		} catch (Exception e) {
			logger.error("getClpQueries: Failed to run Calypso query.EXIT ");
			e.printStackTrace();
		}
		return result;
	}
	
	private static ScctReconTermType getTerminationConfig(ReconConfiguration recConf) {
		ScctGemfireConfigType config = ReconUtil.getGemfireConfig(recConf.getEnv(),recConf.getReconCfg());
		return config.getTermination();
	}
	
	private static QueryScctReconType getClpFeeQueriesList(boolean isTerminated, String sProduct, String[] arStrategies, boolean isExternal, ReconConfiguration recConf ) {
		ScctReconTermType reconTerm = getTerminationConfig(recConf);
		QueryScctReconType request = getClpQueryList(isTerminated, sProduct, arStrategies, isExternal, recConf);
		request.setWhere(request.getWhere() + " and fee.fee_date >= '"+getCalypsoFutureFeeCondition(reconTerm)+"' ");
		return request;
	}

	private static QueryScctReconType getClpQueryList(boolean isTerminated, String product,
			String[]  arStrategy, boolean isExternal, ReconConfiguration recConf) {
		ScctReconTermType reconTerm = getTerminationConfig(recConf);
		StringBuffer buf = new StringBuffer();
		
		if (!isTerminated) {
			if (recConf.getGemSrc().length() > 0 ){
				if(isExternal) {
					final String SQL_EXTERNAL = "t.trade_status = 'EXTERNAL' AND EXISTS (SELECT tk.trade_id FROM trade_keyword tk WHERE t.trade_id = tk.trade_id AND tk.keyword_name = 'DPSTradeEntrySystem' AND tk.keyword_value IN (";
					buf.append(" "+SQL_EXTERNAL+recConf.getExternalInValues()+")) ");
				} else {
					buf.append(" t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T','TPS_RISK_ONLY' , 'TPS_FO_COMPLETE') ");
				}
				buf.append("AND t.update_date_time <= '"+getCalypsoVerifiedTradeUpdateCondition(reconTerm)+"' ");
			}
			else{
				buf.append(" t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T', 'EXTERNAL') "); // Old condition
				buf.append("AND t.update_date_time <= '"+getCalypsoVerifiedTradeUpdateCondition(reconTerm)+"'");
			}
		} else {
			buf.append(" t.trade_status = 'TERMINATED' ");
			buf.append(getCalypsoTerminatedTradeUpdateCondition(reconTerm));
		}
		if(arStrategy !=null && arStrategy.length > 0) {
			buf.append(" AND ba.attribute_value in (");		
	 		for ( String sStg : arStrategy){
				buf.append("'").append(sStg).append("',");				
			}
	 		buf.deleteCharAt(buf.length() - 1);
	 		buf.append(") ");
		}
		QueryScctReconType request = new QueryScctReconType();
		request.setProduct(product);
		request.setWhere(buf.toString());
		return request;
	}
		
	private static List<QueryScctReconType> getClpQueriesForLiveTrades(String[] arStrategies, ReconConfiguration recConf) throws ConfigException {
		List<QueryScctReconType> result = new ArrayList<QueryScctReconType>();
		try {
			for(String sProduct: recConf.getvProductList()){
				QueryScctReconType query = getClpQueryList(false, sProduct, arStrategies, false, recConf);
				result.add(query);
				query = getClpQueryList(false, sProduct, arStrategies, true, recConf);
				result.add(query);
			}
		} catch (Exception e) {
			logger.error("getClpQueries: Failed to run Calypso query.EXIT ");
			e.printStackTrace();
		}
		return result;
	}
	
	private static List<String> getGemfireQueriesForSpeicifiedConfig(ReconConfiguration recConf) throws Exception {
		ScctGemfireConfigType sgctGfeconfig = ReconUtil.getGemfireConfig(recConf.getEnv(), recConf.getReconCfg());
		if (sgctGfeconfig != null) {
			return sgctGfeconfig.getQuery();
		}
		else{
			String sErorMesg ="getClpQueries: Configuration does not have any query for Gemfire";
			logger.error(sErorMesg);
			throw new ConfigException(sErorMesg);
		}
	}
	
	private static List<QueryScctReconType> getClpFeeQueriesForSpeicifiedConfig(ReconConfiguration recConf) throws Exception {
		List<QueryScctReconType> queries = getClpQueriesForSpeicifiedConfig(recConf);
		ScctReconTermType reconTerm = getTerminationConfig(recConf);
		for (QueryScctReconType query : queries) {
			query.setWhere(query.getWhere() + " and fee.fee_date >= '"+getCalypsoFutureFeeCondition(reconTerm)+"' ");
		}
		return queries;
	}

	private static List<QueryScctReconType> getClpQueriesForSpeicifiedConfig(ReconConfiguration recConf) throws Exception {
		ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig(recConf.getEnv(),recConf.getReconCfg());
		List<QueryScctReconType> result = new ArrayList<QueryScctReconType>();
		if (config != null) {
			QueryScctReconType request = new QueryScctReconType();
		
		    List <QueryScctReconType> laQCRT = new ArrayList<QueryScctReconType>(); 
		    laQCRT = (List<QueryScctReconType>) config.getQuery();
		    Iterator<QueryScctReconType> iQue = laQCRT.iterator();
		    int iPos = 0;
			while(iQue.hasNext()){
				QueryScctReconType qsrt = (QueryScctReconType) iQue.next();
				request.setProduct(qsrt.getProduct());
				request.setFrom(qsrt.getFrom());
				if (qsrt.getWhere() != null && !qsrt.getWhere().equals("")){
					request.setWhere(qsrt.getWhere());
				}
				result.add(request);
				iPos++;
			}
			return result;
		}
		else{
			String sErorMesg ="getClpQueries: Configuration does not have any query for Calypso";
			logger.debug(sErorMesg);
			throw new ConfigException(sErorMesg);
		}	
	}

	private static TradeReconObject cleanRecord(TradeReconObject srtReturn, Vector<String> vExclude) {
		try {
			for (String sFieldName: vExclude ){
				if (sFieldName.equalsIgnoreCase("Status")){
					srtReturn.setTradeStatus("");
				}
				if (sFieldName.equalsIgnoreCase("Ccy")){
					srtReturn.setCurrency("");
				}
				if (sFieldName.equalsIgnoreCase("BuySell")){
					srtReturn.setBuySell("");
				}
				if (sFieldName.equalsIgnoreCase("Book")){
					srtReturn.setBook("");
				}
				if (sFieldName.equalsIgnoreCase("Cpty")){
					srtReturn.setCpty("");
				}
				if (sFieldName.equalsIgnoreCase("Notional")){
					srtReturn.setNotional(0);
				}
				if (sFieldName.equalsIgnoreCase("Version")){
					srtReturn.setVersion(0);
				}				
				if (sFieldName.equalsIgnoreCase("UpDate")){
					srtReturn.setUpdateDt("");
				}				
			}
		} catch (Exception e) {
			logger.error("cleanRecord: trade "+srtReturn.getTradeId()+" failed to modify");
			return null;
		}
		return srtReturn;
	}

	private static void reportMismatchTrades(String filename, ReconResult reconResult) {
		Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = reconResult.getCalypsoTradesMissingInGemfire();
		Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = reconResult.getGemfireTradesMissingInCalypso();
		ArrayList<String> arMismatch = reconResult.getMismatchTrades();
		
		File fReport = null;
		FileWriter fwReport = null;
		BufferedWriter bwReport = null;
		String sRepHeader = "Trade Id|Break Type|Calypso Value|Gemfire Value";
		try {
			fReport = new File(filename);
			fwReport = new FileWriter(fReport);
			bwReport = new BufferedWriter(fwReport);
			// Print header
			StringBuffer sbRecord = new StringBuffer();
			sbRecord.append(sRepHeader).append("\n");
			bwReport.write(sbRecord.toString());
			String arResults[] = new String[arMismatch.size()];
			arResults = arMismatch.toArray(arResults);
			Arrays.sort(arResults);
			for (String sMisRecord : arResults){
			    bwReport.write(sMisRecord+"\n");
			    bwReport.flush();
			}
			// Part II List Calypso trade's id missing in Gemfire:
			if ( vMissingCalypsoidsInGemfire.size() > 0){
			  sbRecord.delete(0, sbRecord.length());
			  // Print header
			  sbRecord.append("\n");				  
			  sbRecord.append("\n");
			  sbRecord.append("Part II List Calypso trade's id missing in Gemfire:\n");
			  sbRecord.append("==========================================================\n");
			  sbRecord.append("\n");
			  bwReport.write(sbRecord.toString());
			  bwReport.flush();
			  sbRecord.delete(0, sbRecord.length());
			  // Print trade ids:
			  int iMaxLength = 80;
			  StringBuffer sbLine = new StringBuffer();
			  Iterator<Integer> iter = vMissingCalypsoidsInGemfire.keySet().iterator();
			  while(iter.hasNext()) {
				  sbLine.append(iter.next()).append(", ");
				  if (sbLine.toString().length() >= iMaxLength){
					  sbRecord.append(sbLine.toString()+"\n");
					  bwReport.write(sbRecord.toString());
					  bwReport.flush();
					  sbLine.delete(0, sbLine.length());
					  sbRecord.delete(0, sbRecord.length());
				  }
			  }
			  // Print the rest if in the buffer
			  if (sbLine.toString().length() > 0 ){
				  String sOut = sbLine.substring(0, sbLine.length()-2).toString()+ "\n";
				  sbRecord.append(sOut);
				  bwReport.write(sbRecord.toString());
				  bwReport.flush();
			  }
			  sbRecord.delete(0, sbRecord.length());
			  sbRecord.append("\n");				  
			  sbRecord.append("\n");
			  sbRecord.append("TOTAL trades missing in Gemfire: "+vMissingCalypsoidsInGemfire.size()+"\n");
			  sbRecord.append("==========================================================\n");
			  sbRecord.append("\n");
			  bwReport.write(sbRecord.toString());
			  bwReport.flush();
			}

			//below block commenting for production, its here for testing only
			/*if ( vMissingGFEidsInCalypso.size() > 0){
				  sbRecord.delete(0, sbRecord.length());
				  // Print header
				  sbRecord.append("\n");				  
				  sbRecord.append("\n");
				  sbRecord.append("Part III List Gemfire trade's id missing in Calypso:\n");
				  sbRecord.append("==========================================================\n");
				  sbRecord.append("\n");
				  bwReport.write(sbRecord.toString());
				  bwReport.flush();
				  sbRecord.delete(0, sbRecord.length());
				  // Print trade ids:
				  int iMaxLength = 80;
				  StringBuffer sbLine = new StringBuffer();
				  Iterator<Integer> iter = vMissingGFEidsInCalypso.keySet().iterator();
				  while(iter.hasNext()) {
					  sbLine.append(iter.next()).append(", ");
					  if (sbLine.toString().length() >= iMaxLength){
						  sbRecord.append(sbLine.toString()+"\n");
						  bwReport.write(sbRecord.toString());
						  bwReport.flush();
						  sbLine.delete(0, sbLine.length());
						  sbRecord.delete(0, sbRecord.length());
					  }
				  }
				  // Print the rest if in the buffer
				  if (sbLine.toString().length() > 0 ){
					  String sOut = sbLine.substring(0, sbLine.length()-2).toString()+ "\n";
					  sbRecord.append(sOut);
					  bwReport.write(sbRecord.toString());
					  bwReport.flush();
				  }
				  sbRecord.delete(0, sbRecord.length());
				  sbRecord.append("\n");				  
				  sbRecord.append("\n");
				  sbRecord.append("TOTAL trades missing in Calypso: "+vMissingGFEidsInCalypso.size()+"\n");
				  sbRecord.append("==========================================================\n");
				  sbRecord.append("\n");
				  bwReport.write(sbRecord.toString());
				  bwReport.flush();
			}*/
		} catch (IOException e) {
			logger.warn("Unable to write to '" + filename + "'", e);
		} finally {
			if (bwReport != null) {
				try {
					bwReport.close();
				} catch (IOException e) {
					logger.error("reportMismatchTrades: Error while closing file "+filename);
					e.printStackTrace();
				}
			}
			if (fwReport != null) {
				try {
					fwReport.close();
				} catch (IOException e) {
					logger.error("reportMismatchTrades: Error while closing file "+filename);
					e.printStackTrace();
				}
			}
		}
	}
	
	public static long getTermOffset(boolean start, ScctReconTermType term) {
		Calendar cal = getStartDayOrEndDayCalendar(start);
		try {
			if (start) {
				if (term != null) {
					if ("MONTH".equals(term.getTermDtOffsetUnit())) {
						cal.add(Calendar.MONTH, term.getTermDtOffset());
					} else {
						cal.add(Calendar.DAY_OF_MONTH, term.getTermDtOffset());
					}
				} else {
					cal.add(Calendar.MONTH, -2);
				}
			} else {
				cal.setTimeInMillis(reportStartDate.getTime());
			}
			return cal.getTimeInMillis();
		} catch (RuntimeException e) {
			logger.warn("Caught Exception '" + e.getMessage(), e);
		}
		return 0l;
	}

	public static String getCalypsoTerminatedTradeUpdateCondition(ScctReconTermType term) {
		if (term == null) {
			return null;
		}
		int dtOffset = term.getTermDtOffset();
		String dtOffsetUnit = term.getTermDtOffsetUnit();
		String dtOffsetFmt = term.getTermDtFormat();

		SimpleDateFormat sdf = null;
		try {
			sdf = new SimpleDateFormat(dtOffsetFmt);
		} catch (RuntimeException e1) {
			logger.warn("Caught Exception parsing date format '" + dtOffsetFmt + "'" + e1.getMessage(), e1);
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		}

		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

		String startDt = null;
		if (dtOffset > 0) {
			startDt = sdf.format(getStartDayOrEndDayCalendar(true).getTime());
		} else {
			Calendar cal = getStartDayOrEndDayCalendar(true);
			cal.add(lookupOffsetUnit(dtOffsetUnit), dtOffset);
			startDt = sdf.format(cal.getTime());
		}
		StringBuffer buf = new StringBuffer();
		buf.append(" and t.update_date_time between {ts '").append(startDt).append("'} ");
		buf.append(" and {ts '").append(sdf.format(reportStartDate)).append("'} ");

		return buf.toString();
	}

	private static String getCalypsoVerifiedTradeUpdateCondition(ScctReconTermType term)
	{
		if (term == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(term.getTermDtFormat());
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		String endDate = sdf.format(reportStartDate);
		//System.out.println("getDateString--->"+endDate);
		return endDate;
	}

	private static String getCalypsoFutureFeeCondition(ScctReconTermType term) {
		if (term == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(term.getTermDtFormat());
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		String date = sdf.format(getStartDayOrEndDayCalendar(true).getTime());
		//System.out.println("getDateString--->"+endDate);
		return date;
	}


	private static int lookupOffsetUnit(String s) {
		if ("MONTH".equalsIgnoreCase(s)) {
			return Calendar.MONTH;
		} else {
			return Calendar.DAY_OF_MONTH;
		}
	}

	public static Calendar getStartDayOrEndDayCalendar(boolean start) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		Calendar clone = (Calendar) cal.clone();
		if (start) {
			clone.set(Calendar.HOUR_OF_DAY, 0);
			clone.set(Calendar.MINUTE, 0);
			clone.set(Calendar.SECOND, 0);
			clone.set(Calendar.MILLISECOND, 0);
		} else {
			clone.set(Calendar.HOUR_OF_DAY, 23);
			clone.set(Calendar.MINUTE, 59);
			clone.set(Calendar.SECOND, 59);
			clone.set(Calendar.MILLISECOND, 999);
		}
		return clone;
	}

	public static void reloadTrades(CreditGatewayInterface gateway,
			Set<Integer> missingTrades, int defaultBatchSize) {
		logger.debug("reloadTrades: Before: parm Vector size: "+missingTrades.size());
		int reloadCnt = (missingTrades != null ? missingTrades.size() : 0);
		logger.debug("reloadTrades: After: reloadCnt: "+reloadCnt);
		if (reloadCnt > 0) {
			int totBatch = (reloadCnt % defaultBatchSize == 0 ? (reloadCnt / defaultBatchSize)
					: ((reloadCnt / defaultBatchSize) + 1));
			int[] converted = new int[reloadCnt];
			int i = 0;
			for (Integer tradeId : missingTrades) {
				converted[i++] = tradeId;
			}
			Reloader reloader = new GemfireTradeReloader(gateway);
			int offset = 0;
			for (int j = 0; j < totBatch; j++) {
				int[] tgt;
				int currentBatch = 0;
				if (j != (totBatch - 1)) {
					currentBatch = defaultBatchSize;
					tgt = new int[currentBatch];
				} else {
					currentBatch = (reloadCnt % defaultBatchSize == 0 ? defaultBatchSize
							: (reloadCnt % defaultBatchSize));
					tgt = new int[currentBatch];
				}
				System.arraycopy(converted, offset, tgt, 0, currentBatch);
				offset += currentBatch;

				try {
					reloader.reload(tgt);
					tgt = null;
				} catch (ReloadException e) {
					logger.warn("Caught Exception reloading trades batch [" + j + "]");
				}
			}
		} else {
			logger.debug(gateway.getClass().getName()+ " -- Nothing to reload");
		}
	}

	static class ReconConfiguration {
		//String[] args;
		
		final public static String DB_LOGIN = "calypsodblogin";
		final public static String DB_PASSWD = "calypsodbpasswd";
		final public static String DB_DRIVER = "calypsodriver";
		final public static String DB_URL = "calypsourl";
		final public static String LOGIN_KEY = "login";
		final public static String PASSWD_KEY = "passwd";
		final public static String INSTANCE_KEY = "instance";
		final public static String ENV_KEY = "env";
		final public static String EXCLUDE_KEY = "exclude";
		final public static String PRODUCTS_KEY = "products";
		final public static String PRINT_KEY = "fileOut";
		final public static String DATE_TLRNC_KEY = "tD";
		final public static String GEM_SRC_KEY = "srcSys";
		final public static String EXTERNAL_IN_KEY = "externalIn";
		final public static String CALYPSO_LOGIN = "";
		final public static String CALYPSO_PASSWD = "";
		final public static String DEFAULT_INSTANCE = "CreditGatewayServer1";
		final public static String DELIM = "\t";
		final public static String NEWLINE = "\n";
		final public static String QUOTED_IDENTIFIER = "\"";

		String szLogin;
		String szPasswd;
		String env;
		String gatewayInstance;
		String gemSrc;
		String sDtTl;
		String calypsoDBLogin;
		String calypsoDBPass;
		String calypsoDBDriver;
		String calypsoDBURL;
		Vector<String> vExclude = new Vector<String>();
		Vector<String> vProductList = new Vector<String>();
		String externalInValues = "";
		boolean defaultGfeQuery = true;
		HashMap<String, ScctReconConfigEnvType> reconCfg;
		String printToFile;
		String sRepFileName;
		Properties cmdArgs;
		String batchReloadSize;
		int threadCount = 5;
		int iMaxStgSize = 50;


		
		ReconConfiguration(String[] args, String platform) throws ConfigException {
			//this.args = args;
			init(args);
			reconCfg = init(platform);
		}
		
		private void init(String[] args) {
			cmdArgs = loadCmdArgs(args);
			szLogin = CGUtilities.getKeyFromMap(cmdArgs, LOGIN_KEY, CALYPSO_LOGIN);
			szPasswd = CGUtilities.getKeyFromMap(cmdArgs, PASSWD_KEY, CALYPSO_PASSWD);
			gatewayInstance = CGUtilities.getKeyFromMap(cmdArgs, INSTANCE_KEY, DEFAULT_INSTANCE);
			
			calypsoDBLogin = CGUtilities.getKeyFromMap(cmdArgs, DB_LOGIN, "");
			calypsoDBPass = CGUtilities.getKeyFromMap(cmdArgs, DB_PASSWD, "");
			calypsoDBDriver = CGUtilities.getKeyFromMap(cmdArgs, DB_DRIVER, "");
			calypsoDBURL = CGUtilities.getKeyFromMap(cmdArgs, DB_URL, "");
			
			sDtTl = CGUtilities.getKeyFromMap(cmdArgs, DATE_TLRNC_KEY, "0");
			if (!sDtTl.equals("0")){
				//Convert parameter to Long
				Long lTl = new Long(sDtTl);
				DT_TLRNC = lTl.longValue();
			}

			env = CGUtilities.getKeyFromMap(cmdArgs, ENV_KEY, "prod");
			if (env.length() == 0 && env.trim().equals("")){
				logger.error("Parameter env not set. Exit");
				System.exit(1);
			}

			String exclude = CGUtilities.getKeyFromMap(cmdArgs, EXCLUDE_KEY, "");
			if (exclude.length() > 0 && !exclude.trim().equals("")){
				String[] saFields = exclude.split("~");
				for (int z=0; z < saFields.length; z++){
					vExclude.add(saFields[z]);
				}
			}
			
			String products = CGUtilities.getKeyFromMap(cmdArgs, PRODUCTS_KEY, "");
			if (products.length() > 0 && !products.trim().equals("")){
				String[] saFields = products.split("~");
				for (int z=0; z < saFields.length; z++){
					vProductList.add(saFields[z]);
				}
			} else{
				logger.error("Empty product list.EXIT");
				System.exit(1);
			}
			
			gemSrc = CGUtilities.getKeyFromMap(cmdArgs, GEM_SRC_KEY, "");
			if (gemSrc.length() > 0 && !gemSrc.trim().equals("")){
				if (!gemSrc.equalsIgnoreCase("CALYPSO") && !gemSrc.equalsIgnoreCase("CALYPSOEM")){
					logger.error("Invalid Gemfire source setting => srcSys set to: " + gemSrc+" .EXIT");
					System.exit(1);
				}
			}
			
			String externalInClause = CGUtilities.getKeyFromMap(cmdArgs, EXTERNAL_IN_KEY, "OASYS");
			if (!externalInClause.trim().equals("OASYS")){
				// Parse list
				String[] saEXIN = externalInClause.split("~");
				for (int z=0; z < saEXIN.length; z++){
					if (externalInValues.equals("")){
						externalInValues="'"+saEXIN[z]+"'";					
					}
					else{
						externalInValues=externalInValues+", '"+saEXIN[z]+"'";
					}
				}
			} else{
				externalInValues="'"+externalInClause+"'";
			}

			if (!CGUtilities.isStringEmpty(cmdArgs.getProperty("defaultQuery"))) {
				defaultGfeQuery = Boolean.valueOf(cmdArgs.getProperty("defaultQuery"));
			}
			
			printToFile = CGUtilities.getKeyFromMap(cmdArgs, PRINT_KEY, "Y");
			sRepFileName = System.getProperty("repname");
			if (printToFile == "Y"){
				if (sRepFileName.trim().length() == 0 ){
					logger.error("Printing set to: Y but report file name is Empty .EXIT");
					System.exit(1);
				}
			}
			
			batchReloadSize = CGUtilities.getKeyFromMap(cmdArgs, "batchReloadSize", "1000");
		}

		private static HashMap<String, ScctReconConfigEnvType> init(String platform) throws ConfigException {
			String cfgFile = null;
			HashMap<String, ScctReconConfigEnvType> reconCfg = null;
			if (platform.startsWith("Windows")) {
				System.out.println("Windows");
				cfgFile = "./config/gemfireReconConfig.xml";
			} else {
				System.out.println("loading Solaris");
				cfgFile = "../config/gemfireReconConfig.xml";
			}
			try {
				reconCfg = ReconUtil.loadReconConfig(cfgFile);
				return reconCfg;
			} catch (CGException e) {
				logger.warn(e.getMessage(), e);
				throw new ConfigException("Unable to bootstrap Reconcilation configuration, please check config entries in '" + cfgFile + "'", e);
			}
		}

		protected static Properties loadCmdArgs(String[] args) {
			Properties cmdArgs = new Properties();
			if (args != null && args.length > 0) {
				String szTmp;
				for (int i = 0; i < args.length; i++) {
					szTmp = args[i];
					String[] szVals = szTmp.split("=");
					if (!CGUtilities.isStringEmpty(szVals[0])
							&& !CGUtilities.isStringEmpty(szVals[1])) {
						cmdArgs.put(szVals[0], szVals[1]);
					}
				}
			}
			return cmdArgs;
		}

		public boolean reload(String type) {
			return (Boolean.TRUE.toString().equalsIgnoreCase(cmdArgs.getProperty(type, "false")));
		}
		
		public String getCalypsoDBLogin() {
			return calypsoDBLogin;
		}

		public String getCalypsoDBPass() {
			return calypsoDBPass;
		}

		public String getCalypsoDBDriver() {
			return calypsoDBDriver;
		}

		public String getCalypsoDBURL() {
			return calypsoDBURL;
		}

		public String getSzLogin() {
			return szLogin;
		}

		public String getSzPasswd() {
			return szPasswd;
		}

		public String getEnv() {
			return env;
		}

		public String getGatewayInstance() {
			return gatewayInstance;
		}

		public String getGemSrc() {
			return gemSrc;
		}

		public Vector<String> getvExclude() {
			return vExclude;
		}

		public Vector<String> getvProductList() {
			return vProductList;
		}

		public String getExternalInValues() {
			return externalInValues;
		}

		public boolean getDefaultGfeQuery() {
			return defaultGfeQuery;
		}

		public HashMap<String, ScctReconConfigEnvType> getReconCfg() {
			return reconCfg;
		}

		public String getPrintToFile() {
			return printToFile;
		}

		public String getsRepFileName() {
			return sRepFileName;
		}
		
		public String getBatchReloadSize() {
			return batchReloadSize;
		}
		
		public int getThreadCount() {
			ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig(getEnv(),reconCfg);
			if(config != null) {
				threadCount = config.getPoolSize();
			}
			return threadCount;
		}
		
		public int getStrategyBlockSize() {
			return iMaxStgSize;
		}
	}
	
	static class ReconSubTask implements Callable<ReconResult> {
		List<String> gemfireQueries;
		List<QueryScctReconType> calypsoQueries;
		List<QueryScctReconType> calypsoFeeQueries;
		ReconConfiguration reconConfig;
		
		private static int count = 0;
		
		ReconResult reconResult;
		
		ReconSubTask(List<String> gemfireQueries, List<QueryScctReconType> calypsoQueries, List<QueryScctReconType> calypsoFeeQueries, ReconConfiguration reconConfig) {
			this.gemfireQueries = gemfireQueries;
			this.calypsoQueries = calypsoQueries;
			this.calypsoFeeQueries = calypsoFeeQueries;
			this.reconConfig = reconConfig;
		}
		
		public ReconResult call() {
			try {
				run();
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
			}
			return reconResult;
		}
		
		public void run() {
			final int localCount = count++;
			GemfireTradeLoader gemfireTradeLoader = new GemfireTradeLoader(gemfireQueries, reconConfig);
			CalypsoTradeLoader calypsoTradeLoader = new CalypsoTradeLoader(calypsoQueries, reconConfig);
			CalypsoFeeLoader calypsoFeeLoader = new CalypsoFeeLoader(calypsoFeeQueries, reconConfig);

			final Thread gemfireThread = new Thread(gemfireTradeLoader, "GemfireMultiQueryTradeLoader-"+count);
			gemfireThread.start();

			final Thread calypsoFeeThread = new Thread(calypsoFeeLoader, "CalypsoMultiQueryFeeLoader-"+count);
			calypsoFeeThread.start();

			final Thread calypsoThread = new Thread(calypsoTradeLoader, "CalypsoMultiQueryTradeLoader-"+count);
			calypsoThread.start();
			
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {
				public void run() {
					boolean gtTLive = gemfireThread.isAlive();
					boolean ctTLive = calypsoThread.isAlive();
					boolean cfTLive = calypsoFeeThread.isAlive();
					if(gtTLive || ctTLive || cfTLive) {
						logger.warn("ReconSubTask-"+localCount+" : Waiting CalTradePending-"+ctTLive+" CalFeePending-"+cfTLive + " GemTradePending-"+gtTLive);
					}
				}
			}, MAX_WAIT_TIME_FOR_LOAD, MAX_WAIT_TIME_FOR_LOAD);
			
			try {
				gemfireThread.join();
				calypsoThread.join();
				calypsoFeeThread.join();
			} catch (InterruptedException e) {
				logger.warn("Caught InterruptedException ", e);
			}
			
			timer.cancel();
			
		    Map<Integer, TradeReconObject> calypsoTrades = calypsoTradeLoader.getLoadedResult();
			
			
		    Map<Integer, List<FeeReconObject>> tradeFees = calypsoFeeLoader.getLoadedResult();
			for (Integer trdId : tradeFees.keySet()) {
				List<FeeReconObject> feeReconObject = tradeFees.get(trdId);
				//int trdId = feeReconObject.get(0).getTradeId();
				TradeReconObject tRec = calypsoTrades.get(trdId);
				if(tRec!=null)
					tRec.getFees().addAll(feeReconObject);
			}
		    
			Map<Integer, TradeReconObject> gemfireTrades = gemfireTradeLoader.getLoadedResult();

			reconResult = compareTrades(gemfireTrades, calypsoTrades);
		    reconResult.setCalypsoLoadingTime(calypsoTradeLoader.getLoadingTime() + calypsoFeeLoader.getLoadingTime());
		    reconResult.setGemfireLoadingTime(gemfireTradeLoader.getLoadingTime());
		}

		private ReconResult compareTrades(Map<Integer, TradeReconObject> gemfireTrades, Map<Integer, TradeReconObject> calypsoTrades) {
			long x = System.currentTimeMillis();
			ArrayList<String> arMismatch = new ArrayList<String>();
			Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = new HashMap<Integer, TradeReconObject>();
			Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = new HashMap<Integer, TradeReconObject>();
			
			 // Iterate over both collections but not always in the same pace
			Iterator<Integer> gfeIterator = gemfireTrades.keySet().iterator();
			Iterator<Integer> calIterator = calypsoTrades.keySet().iterator();
			TradeReconObject calTrade = null;
			TradeReconObject gemTrade = null;
			while (calIterator.hasNext() && gfeIterator.hasNext()) {
				if(calTrade == null) {
					calTrade = calypsoTrades.get(calIterator.next());
				}
				if(gemTrade == null) {
					gemTrade = gemfireTrades.get(gfeIterator.next());
				}
				
				// System.out.println("Calypso ID: "+ClpTrade.getTradeId()+" vs GFE ID: "+GfeTrade.getTradeId());
				if (calTrade.getTradeId() < gemTrade.getTradeId()) {
					vMissingCalypsoidsInGemfire.put(calTrade.getTradeId(),calTrade);
					calTrade = null;
				} else if (calTrade.getTradeId() > gemTrade.getTradeId()) {
					vMissingGFEidsInCalypso
							.put(gemTrade.getTradeId(), gemTrade);
					gemTrade = null;
				} else if (calTrade.getTradeId() == gemTrade.getTradeId()) {
					ArrayList<String> mismatchValues = compareTwoTradeReconObject(calTrade, gemTrade);
					if (mismatchValues != null && mismatchValues.size() > 0) {
						arMismatch.addAll(mismatchValues);
					}
					calTrade = null;
					gemTrade = null;
				}
			}
			  // Process any left trades:
			  // Calypso
			  while ( calIterator.hasNext()){
				  calTrade = calypsoTrades.get(calIterator.next());
				  vMissingCalypsoidsInGemfire.put(calTrade.getTradeId(), calTrade);
			  }
			  // Gemfire
			  while (gfeIterator.hasNext()){
				  gemTrade = gemfireTrades.get(gfeIterator.next());
				  vMissingGFEidsInCalypso.put(gemTrade.getTradeId(), gemTrade);
			  }
			  long y = System.currentTimeMillis();
			  long reconTime = (y - x) / 1000;
			  
			  ReconResult result = new ReconResult();
			  result.setComareTime(reconTime);
			  result.setCalypsoTradesMissingInGemfire(vMissingCalypsoidsInGemfire);
			  result.setGemfireTradesMissingInCalypso(vMissingGFEidsInCalypso);
			  result.setMismatchTrades(arMismatch);
			  
			  return result;
		}
		
		public ReconResult getReconResult() {
			return reconResult;
		}
	}
	
	static class ReconResult {
		long reconTime;
		long calypsoLoadingTime;
		long gemfireLoadingTime;
		ArrayList<String> arMismatch = new ArrayList<String>();
		Map<Integer, TradeReconObject> vMissingCalypsoidsInGemfire = new HashMap<Integer, TradeReconObject>();
		Map<Integer, TradeReconObject> vMissingGFEidsInCalypso = new HashMap<Integer, TradeReconObject>();

		public long getComareTime() {
			return reconTime;
		}
		
		public void setComareTime(long reconTime) {
			this.reconTime = reconTime;
		}

		public long getCalypsoLoadingTime() {
			return calypsoLoadingTime;
		}
		
		public void setCalypsoLoadingTime(long calypsoLoadingTime) {
			this.calypsoLoadingTime = calypsoLoadingTime;
		}

		public long getGemfireLoadingTime() {
			return gemfireLoadingTime; 
		}
		
		public void setGemfireLoadingTime(long gemfireLoadingTime) {
			this.gemfireLoadingTime = gemfireLoadingTime;
		}

		public void setMismatchTrades(ArrayList<String> arMismatch) {
			this.arMismatch = arMismatch;
		}

		public ArrayList<String> getMismatchTrades() {
			return arMismatch;
		}
		
		public Map<Integer, TradeReconObject> getCalypsoTradesMissingInGemfire() {
			return vMissingCalypsoidsInGemfire;
		}

		public void setCalypsoTradesMissingInGemfire(Map<Integer, TradeReconObject> map) {
			this.vMissingCalypsoidsInGemfire = map;
		}

		public void setGemfireTradesMissingInCalypso(Map<Integer, TradeReconObject> map) {
			this.vMissingGFEidsInCalypso = map;
		}

		public Map<Integer, TradeReconObject> getGemfireTradesMissingInCalypso() {
			return vMissingGFEidsInCalypso;
		}
	}
	
	static class GemfireTradeLoader extends MultiQueryBasedLoader<Integer, TradeReconObject, String>  {
		
		ReconConfiguration recConfig;
		
		GemfireTradeLoader(List<String> queries, ReconConfiguration recConfig) {
			super(queries);
			this.recConfig = recConfig; 
		}
		
		public Callable<Map<Integer, TradeReconObject>> getQueryLoader(String query) {
			ScctGemfireConfigType config = ReconUtil.getGemfireConfig(recConfig.getEnv(), recConfig.getReconCfg());
			
			GemfireDAO gdao = new GemfireDAO(config.getDriver(), config.getUrl(), 250, recConfig.getvExclude() );
			GemfireCallableHandler handler = new GemfireCallableHandler(
					gdao, query, new ObjectFactory(), config.getRetry(), config.getDelay());
			return handler;
		}
		
		protected void aggregationLogic(Map<Integer, TradeReconObject> combinedMap, Map<Integer, TradeReconObject> tobeAddedMap) {
			Vector<Integer> vResult = new Vector<Integer>(tobeAddedMap.keySet());
		    Collections.sort(vResult);
		    for(int i=0; i < vResult.size(); i++){
    	        Integer iNext = vResult.get(i);
    	        TradeReconObject sVal = null;
	        	sVal = tobeAddedMap.get(iNext);
	        	// Because the nature of Gemfire DB, Validate Gemfire trade to get the latest.
	        	if (isLatestTrade(combinedMap, sVal)){
	        		combinedMap.put(iNext, sVal);
	        	}
		    }
		}
		
		private static boolean isLatestTrade(Map<Integer, TradeReconObject> mTrades, TradeReconObject scctVal) {
	    	boolean bReturn=true;
    		TradeReconObject scctExist = mTrades.get(scctVal.getTradeId());
	    	if (scctExist!=null){
	    		long dateNew = scctVal.getRawUpdateDt();
	    		long dateOld = scctExist.getRawUpdateDt();
	    		if(dateNew<=dateOld) {
	    			bReturn = false;
	    		}
	    		
	    		/*String sDateNew = scctVal.getUpdateDt();
	    		String sDateOld = scctExist.getUpdateDt();
	    		
	    		try {
	    			java.sql.Timestamp ts1 = java.sql.Timestamp.valueOf(sDateNew);
	    			java.sql.Timestamp ts2 = java.sql.Timestamp.valueOf(sDateOld);
	    			if (ts1.getTime() <= ts2.getTime()) {
	    				bReturn=false;
	    			}
    			} catch (IllegalArgumentException ie) {
    				System.out.println("ERRROR:testMData: Invalid date format for the method");
    				System.out.println("Check parameters: 1:["+sDateNew+"] and 2:["+sDateOld+"]");
    				ie.printStackTrace();
    			}*/
    			return bReturn;
    		}
			return bReturn;
		}
	}
	
	static class QueryResultLoader<KEY, T, Q> implements Runnable {
		
		Callable<Map<KEY, T>> callable;
		Map<KEY, T> result;
		Q query;
		
		QueryResultLoader(Callable<Map<KEY, T>> callable, Q query) {
			this.callable = callable;
			this.query = query;
		}
		
		public void run() {
			final Thread currThread = Thread.currentThread();
			try {
				Timer timer = new Timer();
				timer.schedule(new TimerTask() {
					public void run() {
						if(currThread.isAlive()) {
							logger.warn(currThread.getName() +" still running after "+MAX_WAIT_TIME_FOR_LOAD/1000+" seconds for query - "+query);
							logger.warn("TotalMemory-" + Runtime.getRuntime().totalMemory()/1024 + " FreeMemory-"+Runtime.getRuntime().freeMemory()/1024);
						}
					}
				}, MAX_WAIT_TIME_FOR_LOAD, MAX_WAIT_TIME_FOR_LOAD);
				
				result = callable.call();
				
				timer.cancel();
			} catch (Exception e1) {
				e1.printStackTrace();
				logger.error(currThread.getName()+" : Unable to retrive Query result because "+ e1.getMessage());
			}
		}
		
		public Map<KEY, T> getLoadedResult() {
			return result;
		}
	}
	
	static abstract class MultiQueryBasedLoader<KEY, T, Q> implements Runnable {
		long loadingTime;
		List<Q> queries;
		Map<KEY, T> result;
		
		private static int count = 0;
		
		MultiQueryBasedLoader(List<Q> queries) {
			this.queries = queries;
		}
		
		public void run() {
			long x = System.currentTimeMillis();
			try {
				result = loadResult();
			} catch (Exception e) {
				logger.warn("Caught Exception retrieving trades '" + e.getMessage(), e);
			}
			long y = System.currentTimeMillis();
			loadingTime = (y - x) / 1000;
		}
		
		public abstract Callable<Map<KEY, T>> getQueryLoader(Q query);
		
		private String getName() {
			return this.getClass().getSimpleName();
		}
		
		private Map<KEY, T> loadResult() throws Exception {
			/*if(queries!=null && queries.size()==1) {
				Callable<Map<KEY, T>> callable = getQueryLoader(queries.get(0));
				return callable.call();
			}*/
			
			List<QueryResultLoader<KEY, T, Q>> queryLoaderList = new ArrayList<QueryResultLoader<KEY, T, Q>>();
			List<Thread> childThreads = new ArrayList<Thread>();
			if (queries != null && queries.size() > 0) {
				for (Q query : queries) {
					if (query!=null) {
						Callable<Map<KEY, T>> callable = getQueryLoader(query); 
						QueryResultLoader<KEY, T, Q> queryResultLoader = new QueryResultLoader<KEY, T, Q>(callable, query);
						queryLoaderList.add(queryResultLoader);
						
						Thread queryResultThread = new Thread(queryResultLoader, getName()+"-"+(++count));
						childThreads.add(queryResultThread);
						queryResultThread.start();
					}
				}
			}
			
			for (Thread thead : childThreads) {
				try {
					thead.join();
				} catch (InterruptedException e) {
					logger.warn("Caught InterruptedException " + e.getMessage(), e);
				}
			}

			return aggregateCallableResult(queryLoaderList);
		}
		
		private Map<KEY, T> aggregateCallableResult(List<QueryResultLoader<KEY, T, Q>> queryLoaderList) {
			Map<KEY, T> result = new TreeMap<KEY, T>();
			for (QueryResultLoader<KEY, T, Q> queryResultLoader : queryLoaderList) {
				Map<KEY, T> oneResult = queryResultLoader.getLoadedResult();
				if(oneResult!=null) {
					aggregationLogic(result, oneResult);
				}
			}
			return result;
		}
		
		protected void aggregationLogic(Map<KEY, T> combinedMap, Map<KEY, T> tobeAddedMap) {
			combinedMap.putAll(tobeAddedMap);
		}
		
		public Map<KEY, T> getLoadedResult() {
			return result;
		}
		
		public long getLoadingTime() {
			return loadingTime;
		}
	}
	
	static class CalypsoFeeLoader extends MultiQueryBasedLoader<Integer, List<FeeReconObject>, QueryScctReconType> {
		
		CalypsoFeeLoader(List<QueryScctReconType> queries, ReconConfiguration recConfig) {
			super(queries);
		}
		
		public Callable<Map<Integer, List<FeeReconObject>>> getQueryLoader(QueryScctReconType query) {
			return new CalypsoFeeLoaderCallable(new CalypsoTradesDAO(), query);
		}
	}
	
	static class CalypsoTradeLoader extends MultiQueryBasedLoader<Integer, TradeReconObject, QueryScctReconType> {
		
		ReconConfiguration recConfig;
		
		CalypsoTradeLoader(List<QueryScctReconType> queries, ReconConfiguration recConfig) {
			super(queries);
			this.recConfig = recConfig;
		}
		
		public Callable<Map<Integer, TradeReconObject>> getQueryLoader(QueryScctReconType query) {
			return new CalypsoTradeLoaderCallable(new CalypsoTradesDAO(), query);
		}
		
		protected void aggregationLogic(Map<Integer, TradeReconObject> combinedMap, Map<Integer, TradeReconObject> tobeAddedMap) {
			Vector<String> vExclude = recConfig.getvExclude();
			Vector<TradeReconObject> vResult = new Vector<TradeReconObject>(tobeAddedMap.values());
			for(TradeReconObject sVal : vResult) {
				if (vExclude != null && vExclude.size() > 0){
	        		sVal = cleanRecord(sVal, vExclude);
	        	}
				combinedMap.put(sVal.getTradeId(), sVal);
			}
		}
	}
	
	static abstract class RetryCallable<T> implements Callable<T> {
		private boolean retried = false;
		
		public T call() {
			try {
				return load();
			} catch (Exception e) {
				logger.warn("Caught Exception retrieveing Calypso Recon Trades '" + e.getMessage() + "'", e);
				if(!retried) {
					retried = true;
					return call(); // try one more time
				}
			} 
			return null; 
		}
		
		public abstract T load() throws Exception;
	}
	
	static class CalypsoTradeLoaderCallable extends RetryCallable<Map<Integer, TradeReconObject>> {
		private CalypsoTradesDAO dao = null;
		private QueryScctReconType queryScctReconType = null;
		
		public CalypsoTradeLoaderCallable(CalypsoTradesDAO dao, QueryScctReconType queryScctReconType) {
			this.dao = dao;
			this.queryScctReconType = queryScctReconType;
		}
		
		public Map<Integer, TradeReconObject> load() throws Exception {
			return dao.getTrades(queryScctReconType);
		}
		
	}

	static class CalypsoFeeLoaderCallable extends RetryCallable<Map<Integer, List<FeeReconObject>>> {
		private CalypsoTradesDAO dao = null;
		private QueryScctReconType query = null;
		
		public CalypsoFeeLoaderCallable(CalypsoTradesDAO dao, QueryScctReconType query) {
			this.dao = dao;
			this.query = query;
		}
		
		public Map<Integer, List<FeeReconObject>> load() throws Exception {
			return dao.getTradeFees(query);
		}
	}
}

