package com.citi.credit.gateway.recon;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.citi.credit.gateway.core.CGConstants;
import com.citi.credit.gateway.core.CGSchemaObjects;
import com.citi.credit.gateway.data.CGException;
import com.citi.credit.gateway.data.CGReconTrade;
import com.citi.credit.gateway.data.CGResult;
import com.citi.credit.gateway.exception.ConfigException;
import com.citi.credit.gateway.exception.ReloadException;
import com.citi.credit.gateway.service.CreditGatewayInterface;
import com.citi.credit.gateway.service.impl.CreditGatewayImpl;
import com.citi.credit.gateway.util.CGUtilities;
import com.citi.rds.model.EntityRegisterHelper;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.QueryScctBookStrategyType;
import com.citigroup.scct.cgen.QueryScctReconType;
import com.citigroup.scct.cgen.ScctCalypsoConfigType;
import com.citigroup.scct.cgen.ScctFeeType;
import com.citigroup.scct.cgen.ScctGemfireConfigType;
import com.citigroup.scct.cgen.ScctOutputConfigType;
import com.citigroup.scct.cgen.ScctReconConfigEnvType;
import com.citigroup.scct.cgen.ScctReconTermType;
import com.citigroup.scct.cgen.ScctReconType;

/*
 * History:
 * 	DF - 07/31/2009 modified from GemfireReconClient
 * 
 */
public class CalypsoGemfireRecon {

	static CreditGatewayInterface gateway = null;

	private static final Logger logger = Logger.getLogger("com.citi.credit.gateway.recon.CalypsoGemfireRecon");
	private static Properties cmdArgs = new Properties();
	private static String cfgFile;
	private static HashMap reconCfg;

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
	final public static int iMaxStgSize = 50;
	final public static int iScale =2;
	final public static String SQL_EXTERNAL = "t.trade_status = 'EXTERNAL' AND EXISTS (SELECT tk.trade_id FROM trade_keyword tk WHERE t.trade_id = tk.trade_id AND tk.keyword_name = 'DPSTradeEntrySystem' AND tk.keyword_value IN (";
	
	static ArrayList<ScctReconType> arlGfeTrades = new ArrayList<ScctReconType>();
    static ArrayList<CGReconTrade> arlClpTrades = new ArrayList<CGReconTrade>();
	static ArrayList<String> arMismatch = new ArrayList<String>();
	static String szLogin = "";
	static String szPasswd = "";
	static String ENV;
	static String INSTANCE = "";
	static String EXCLUDE = "";
	static String PRODUCTS = "";
	static long DT_TLRNC = 0L;
	static String GEM_SRC= "";
	static String EXTERNAL_IN= "";
	static String EXTERNAL_IN_SQL= "";
	static String PRINT_TO_FILE = "Y"; // Can be set to Y (print) or N (not)
	static Hashtable<String, Boolean> done = new Hashtable<String, Boolean>();
	static boolean defaultGfeQuery = true;
	// Created exclude fields vector
	static Vector<String> vExclude = new Vector<String>();
	static Vector<String> vProductList = new Vector<String>();
	// Added for thread test
	static int iCalThreadCount =0;
	// Calypso ids missing in Gemfire
	static Vector vMissingCalypsoids = new Vector();
	// Gemfire ids missing in Calypso
	static Vector vMissingGFEids = new Vector();
	// Mismatch report name (getting as parameter)
	static String sRepFileName = "";

	//static String latestRDSI = "AsOfD <=" + System.currentTimeMillis() + "L";
	static String latestRDSI = "AsOfD <= "+Long.MAX_VALUE+"L";

	final static String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.S";
	static long startTime =0L;//Added-ss78999
	static Date dNow = null;//Added-ss78999

	static {
		done.put("Gemfire", Boolean.FALSE);
		done.put("Calypso", Boolean.FALSE);
	}

	// load args in the format key=value
	protected static void loadCmdArgs(String[] args) {
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
	}

	public static void init(String cfgFile) throws ConfigException {
		try {
			cfgFile = cfgFile;
			reconCfg = ReconUtil.loadReconConfig(cfgFile);
		} catch (CGException e) {
			logger.warn(e.getMessage(), e);
			throw new ConfigException(
					"Unable to bootstrap Reconcilation configuration, please check config entries in '"
							+ cfgFile + "'", e);
		}
	}

	public static List<String> loadBookStrategy() {
		List<String> result = new ArrayList<String>();
		CreditGatewayInterface gateway = null;
		try {
			/*ObjectFactory factory = CGSchemaObjects.getFactory();//Commented-ss78999
			QueryScctBookStrategyType request = factory
					.createQueryScctBookStrategyType();
			JAXBElement<QueryScctBookStrategyType> outer = factory
					.createQueryScctBookStrategy(request);
			String query = null;

			Marshaller marshaller = CGSchemaObjects.getMarshaller();
			StringWriter w = new StringWriter();
			try {
				marshaller.marshal(outer, w);
				query = w.toString();
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			gateway = new CreditGatewayImpl(INSTANCE, szLogin, szPasswd);
			CGResult strategies = gateway.getStrategies(query);
			while (strategies.hasNext()) {
				String s = (String) strategies.next();
				if (!CGUtilities.isStringEmpty(s)) {
					result.add(s);
				}
			}*/
			//Added-ss78999
			CalypsoDAO dao = new CalypsoDAO();
			result = dao.getAllBookStrategy();
			//End
		} catch (Exception e) {
			logger.error("loadBookStrategy: Failed to get strategy(es).EXIT ");
			e.printStackTrace();
			System.exit(1);
		} /*catch (RuntimeException e) {//Commenetd here-srini
			logger.error("loadBookStrategy: Failed to get strategy(es).EXIT ");
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (gateway != null) {
				try {
					gateway.closeGateway();
				} catch (CGException e) {
					logger.error("loadBookStrategy: Failed to close gateway connection.EXIT ");
					e.printStackTrace();
					System.exit(1);
				}
			}
		}*/
		return result;
	}
	//Added for test-srini
	private static List<String> getStrgies()
	{
		List<String> result = new ArrayList<String>();
		//String str = "754, 755, 757, 758, 75A, 75Q, 75S, 761, 762, 767, 769, 76B, 76C, 776, 777, 779, 77D, 780, 786, 787, 78P, 78V, 790, 792, 79C, 79D, 79J, 79P, 79Q, 79U, 79V, 79W, 7SE, 804, 806, 80A, 817, 83H, 855, 856, 85P, 864, 866, 86B, 86H, 86J, 886, 909, 910, 914, 917, 927, 942, 94R, 95P, 973, 974, 975, 976, 977, 978, 979, 97A, 97B, 97C, 97D, 97E, 97F, 97G, 97H, 97I, 981, 98A, 990, 996, 999, 99C, 99G, A01, A02, A03, A04, A05, A06";
		String str = "694";
		StringTokenizer token = new StringTokenizer(str,",");
		while(token.hasMoreTokens())
		{
			result.add(token.nextToken());
			//System.out.println("K...."+result.toString());
		}
		
		return result;
	}

	private static List<String> getGfeQueries() throws ConfigException{
		List<String> result = new ArrayList<String>();
		if (defaultGfeQuery) {
			//List<String> strategies = loadBookStrategy();
			List<String> strategies = getStrgies();//Srini
			System.out.println("Srini getGfeQueries() ---strategies----"+strategies);
			for (String s : strategies) {
				for (String sProduct : vProductList){
					StringBuffer buf = new StringBuffer(GemfireDAO.SELECT_RDSI);
					//buf.append("where strtgy='").append(s).append("' ");
					buf.append("where Strtgy='").append(s).append("' ");//Added-ss78999
					buf.append(" AND FiCDSTyp='"+sProduct+"' ");
					if (GEM_SRC.length() > 0){
						buf.append(" AND SrcSys = '"+GEM_SRC+"' ");						
					}
					buf.append(GemfireDAO.STATUS_ALL);
					// let us get the last RDSI on the trade => [AsOfD <= Long.MAX_VALUE+"L";]
					//buf.append("AND AsOfD <= "+Long.MAX_VALUE+"L");
					//buf.append("AND AsOfD <= (9223372036854775807L)");  
					//buf.append("and ").append(latestRDSI);
					 buf.append("AND AsOfD <= "+startTime+"L");//Added -ss78999
					logger.debug("### RSDI NON-TERM QUERY '" + buf.toString() + "'");
					result.add(buf.toString());
					buf.delete(0, buf.length());

					buf.append(GemfireDAO.SELECT_RDSI);
					//buf.append("where strtgy='").append(s).append("' ");
					buf.append("where Strtgy='").append(s).append("' ");//Added-ss78999
					//buf.append(" and trdStatus='TERMINATED' ");
					buf.append(" and TrdStatus='TERMINATED' ");//Added-ss78999
					buf.append(" AND FiCDSTyp='"+sProduct+"' ");
					if (GEM_SRC.length() > 0){
						buf.append(" AND SrcSys  = '"+GEM_SRC+"' ");						
					}					
					buf.append(" and ValidD >= ").append(getTermOffset(true)).append("L");
					buf.append(" and AsOfD <= ").append(getTermOffset(false)).append("L");
					buf.append(" and ValidD <= ").append(getTermOffset(false)).append("L");
					logger.warn("### RSDI TERM QUERY '" + buf.toString() + "'");
					result.add(buf.toString());
				}
			}
		} else {
			ScctGemfireConfigType sgctGfeconfig = ReconUtil.getGemfireConfig(ENV, reconCfg);
			if (sgctGfeconfig != null) {
				ArrayList<String> arGfeQueries = new ArrayList<String>();
				arGfeQueries.addAll(sgctGfeconfig.getQuery());
				Iterator<String> iGfeQue = arGfeQueries.iterator();
				while(iGfeQue.hasNext()){
					result.add((String) iGfeQue.next());
				}
			}
			else{
				String sErorMesg ="getClpQueries: Configuration does not have any query for Gemfire";
				logger.error(sErorMesg);
				throw new ConfigException(sErorMesg);
			}
		}
		return result;
	}

	private static List<String> getClpQueries() throws CGException{
		List<String> result = new ArrayList<String>();
		String xml = null;
		if (defaultGfeQuery) {
			//List<String> strategies = loadBookStrategy();
			List<String> strategies = getStrgies();//Srini
			System.out.println("Srini getClpQueries() ---strategies----"+strategies);
			if (defaultGfeQuery) {
				try {
					
					for(String sProduct: vProductList){
						xml = null;
						String arStrategies[] = new String[strategies.size()];
						arStrategies = strategies.toArray(arStrategies);
						Arrays.sort(arStrategies);
						int iSplitCount = 0;
						if ( arStrategies.length > iMaxStgSize){
							while ((iSplitCount * iMaxStgSize) < arStrategies.length ){
								int iLen = 0;
								if ( (arStrategies.length) - (iSplitCount * iMaxStgSize) >=  iMaxStgSize){
									iLen = iMaxStgSize;
								}
								else{

									iLen = (arStrategies.length) - (iSplitCount * iMaxStgSize);
								}
								String arDest[] = new String[iLen];
								System.arraycopy(arStrategies, (iSplitCount * iMaxStgSize), arDest, 0, iLen);
								xml = getClpQueryList(false, sProduct, arDest);
								result.add(xml);
								xml = getClpQueryList(true, sProduct, arDest);
								result.add(xml);
								iSplitCount++;
							}
						}
						else{
							xml = getClpQueryList(false, sProduct, arStrategies);
							result.add(xml);
							xml = getClpQueryList(true, sProduct, arStrategies);
							result.add(xml);
						}
						
					}
				} catch (CGException e) {
					logger.error("getClpQueries: Failed to run Calypso query.EXIT ");
					e.printStackTrace();
				}
			}
		} else {
			ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig(ENV,reconCfg);
			if (config != null) {
				Marshaller marshaller;
				try {
					marshaller = CGSchemaObjects.getMarshaller();
				    ObjectFactory factory;
					factory = CGSchemaObjects.getFactory();
					QueryScctReconType request = factory.createQueryScctReconType();
				
				    List <QueryScctReconType> laQCRT = new ArrayList<QueryScctReconType>(); 
				    laQCRT = (List<QueryScctReconType>) config.getQuery();
				    Iterator<QueryScctReconType> iQue = laQCRT.iterator();
				    int iPos = 0;
					while(iQue.hasNext()){
						xml = null;
						QueryScctReconType qsrt = (QueryScctReconType) iQue.next();
						request.setProduct(qsrt.getProduct());
						request.setFrom(qsrt.getFrom());
						if (qsrt.getTermination() != null){
							request.setTermination(qsrt.getTermination());
						}
						if (qsrt.getWhere() != null && !qsrt.getWhere().equals("")){
							request.setWhere(qsrt.getWhere());
						}
						xml = marshallQuery2(marshaller, request);
						if (xml != null && !xml.equals("")){
							result.add(xml);
						}
						iPos++;
					}
				} catch (CGException cgex) {
					String sErMsg = cgex.getMessage();
					cgex.printStackTrace();
					String sErorMesg ="getClpQueries: Exception while getting Calypso configuration: "+sErMsg;
					logger.debug(sErorMesg);
					Exception ex = new Exception(sErorMesg);
					throw  new CGException(ex);
				}
				
			}
			else{
				String sErorMesg ="getClpQueries: Configuration does not have any query for Calypso";
				logger.debug(sErorMesg);
				Exception ex = new Exception(sErorMesg);
				throw  new CGException(ex);
			}

		}
		return result;
	}

	private static String getClpQueryList(boolean isTerminated, String product,
			String[]  arStrategy) throws CGException {
		String xml = null;
		ScctGemfireConfigType config = ReconUtil.getGemfireConfig(ENV,reconCfg);
		Marshaller marshaller = CGSchemaObjects.getMarshaller();
		ObjectFactory factory = CGSchemaObjects.getFactory();
		QueryScctReconType request = factory.createQueryScctReconType();
		StringBuffer buf = new StringBuffer();
		
		request.setFrom("book_attr_value ba");
		request.setProduct(product);
		buf.append("b.book_id = ba.book_id AND d.product_type = '" +product+"' ");
		if (!isTerminated) {
			if (GEM_SRC.length() > 0 ){
				buf.append("AND (t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T','TPS_RISK_ONLY' , 'TPS_FO_COMPLETE') OR ("+SQL_EXTERNAL+EXTERNAL_IN_SQL+")))) ");
				//Added-ss78999
				 buf.append("AND t.update_date_time <= '"+getDateString(config)+"'");
				//End
			}
			else{
				buf.append("AND t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T', 'EXTERNAL') "); // Old condition
				//Added-ss78999
				 buf.append("AND t.update_date_time <= '"+getDateString(config)+"'");
				//End
			}
		} else {
			buf.append("AND t.trade_status = 'TERMINATED' ");
			buf.append(getTerminationDtOffset(config));
		}
		buf.append("AND ba.attribute_name='CITI_Strategy' AND ba.attribute_value in (");		
 		for ( String sStg : arStrategy){
			buf.append("'").append(sStg).append("',");				
		}
 		// Remove last ,
 		buf.deleteCharAt(buf.length() - 1);
 		// Close where expression for strategies
 		buf.append(") ");
		request.setWhere(buf.toString());
		xml = marshallQuery2(marshaller, request);
		return xml;
	}
		
	private static String getClpQuery(boolean isTerminated, String product,
			String strategy) throws CGException {
		String xml = null;
		Marshaller marshaller = CGSchemaObjects.getMarshaller();
		ObjectFactory factory = CGSchemaObjects.getFactory();
		QueryScctReconType request = factory.createQueryScctReconType();
		StringBuffer buf = new StringBuffer();

		if (!isTerminated) {
			request.setProduct(product);
			request.setFrom("book_attr_value ba");
			  
			buf.append("b.book_id = ba.book_id AND t.trade_id = tk.trade_id AND d.product_type = '" +product+"' ");
			if (GEM_SRC.length() > 0){
				buf.append("AND (t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T') OR ("+SQL_EXTERNAL+EXTERNAL_IN_SQL+")))) ");						
			}
			else{
				buf.append("AND t.trade_status in ('VERIFIED', 'MATCHED_UNSENT', 'UNMATCH_T', 'EXTERNAL') "); // Old condition
			}
			buf.append("AND ba.attribute_name='CITI_Strategy' AND ba.attribute_value='").append(strategy).append("' ");
			request.setWhere(buf.toString());
			xml = marshallQuery2(marshaller, request);
		} else {
			request.setProduct(product);
			request.setFrom("book_attr_value ba");
			buf.append("b.book_id = ba.book_id AND d.product_type = '" +product+"' ");
			buf.append("AND t.trade_status = 'TERMINATED' ");
			buf.append("AND ba.attribute_name='CITI_Strategy' AND ba.attribute_value='").append(strategy).append("' ");

			ScctGemfireConfigType config = ReconUtil.getGemfireConfig(ENV,reconCfg);
			buf.append(getTerminationDtOffset(config));
			request.setWhere(buf.toString());
			xml = marshallQuery2(marshaller, request);
		}
		return xml;
	}

	static int retry;
	static long delay;
	private static void loadGfeTrades(String env) throws ConfigException {

		ScctGemfireConfigType config = ReconUtil.getGemfireConfig(env, reconCfg);
		Map<Integer, ScctReconType> mGreTrades = Collections.synchronizedMap(new TreeMap());
		
		if (config != null) {
			String driver = config.getDriver();
			String url = config.getUrl();
			String region = config.getRegion();
			retry = config.getRetry();
			delay = config.getDelay();
			List<String> queries = getGfeQueries();

			logger.debug("Driver : '" + driver + "'");
			logger.debug("URL : '" + url + "'");
			logger.debug("Region : '" + region + "'");
			ExecutorService exService = Executors.newFixedThreadPool(getPoolSize(env));
			Hashtable<String, Future> status = new Hashtable<String, Future>();
			for (String query : queries) {
				  if (vExclude.size() > 0){
					GemfireDAO gdao = new GemfireDAO(driver, url, 250, vExclude);
					GemfireCallableHandler handler = new GemfireCallableHandler(
							gdao, query, new ObjectFactory(), retry, delay);
					Future future = exService.submit(handler);
					status.put(query, future);
					
				}
				else{
					GemfireDAO gdao = new GemfireDAO(driver, url, 250);
					GemfireCallableHandler handler = new GemfireCallableHandler(
							gdao, query, new ObjectFactory(), retry, delay);
					Future future = exService.submit(handler);
					status.put(query, future);
					
				}
			}

			exService.shutdown();
			logger.debug("Shutting Gemfire Execution Service...");

			while (!exService.isTerminated()) {
				try {
					Thread.currentThread().sleep(3 * 1000);
					logger.debug("Gemfire Execution Service : Waiting for all tasks to complete");
				} catch (InterruptedException e) {
					logger.warn(
							"Gemfire Execution Service : Caught InterruptedException "
									+ e.getMessage(), e);
				}
			}
			logger.debug("Gemfire Execution Service SHUTDOWN COMPLETE ");
			// Building Gemfire trades sorted arraylist
			processResults("Gemfire", status, mGreTrades);
			arlGfeTrades = new ArrayList<ScctReconType>(mGreTrades.values());
			System.out.println("Srini loadGfeTrades() --arlGfeTrades.size()----"+arlGfeTrades.size());
			
		 } 
		else {
			logger.error("Unable to retrieve Gemfire Configuration");
			System.exit(1);
		}
	}
	
	private static void processResults(String type, Hashtable futures, Map mData){
		Iterator itr = futures.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry entry = (Map.Entry) itr.next();
			String query = (String) entry.getKey();
			Future future = (Future) entry.getValue();
			if (future != null) {
				Hashtable htQResult;
				try {
					htQResult = (Hashtable) future.get();
					if (htQResult != null && htQResult.size() > 0) {
						Vector<Integer> vResult = new Vector(htQResult.keySet());
					    Collections.sort(vResult);
					    for(int i=0; i < vResult.size(); i++){
			    	        Integer iNext = (Integer) vResult.get(i);
			    	        Object sVal = null;
					    	if (type.equalsIgnoreCase("Calypso")){
					        	// Clean exclude fields
					        	if (vExclude != null && vExclude.size() > 0){
					        		CGReconTrade cgrtRecord = cleanRecord((CGReconTrade)htQResult.get(iNext));
					        		sVal = cleanRecord((CGReconTrade)htQResult.get(iNext));
					        	}
					        	else{
						        	 sVal = (CGReconTrade)htQResult.get(iNext);
					        	}
					        	mData.put(iNext, (CGReconTrade)sVal);
					        }else{
					        	sVal = (ScctReconType)htQResult.get(iNext);
					        	// Because the nature of Gemfire DB
					        	// Validate Gemfire trade to get the latest.
					        	if (testMData(mData,(ScctReconType) sVal)){
					        		mData.put(iNext,(ScctReconType)sVal);
					        	}
					        }
					    }
					} 
					else {
						logger.error("Query: "+query+" has zero result");
					}
					logger.debug(type + " Query: "+query);
					logger.debug(type +"+++ Result recieved  # " + (htQResult != null ? htQResult.size() : 0));
					logger.debug(type +"+++ Totals collected # " + (mData != null ? mData.size() : 0));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
      
	// Validate Gemfire trade to get the latest.
	private static boolean testMData(Map<Integer, ScctReconType> mTrades, ScctReconType scctVal) {
    	int iTrId = scctVal.getTradeId();
    	Integer ITrId = new Integer(iTrId);
    	boolean bReturn=true;
    	// Test if tradeid exist.
    	Set<Integer> sKey = mTrades.keySet();
    	if (sKey.contains(ITrId)){
    		ScctReconType scctExist = (ScctReconType) mTrades.get(ITrId);
    		String sDateNew = scctVal.getUpdateDt();
    		String sDateOld = scctExist.getUpdateDt();
    		
    		java.sql.Timestamp ts1;
    		java.sql.Timestamp ts2;
    		try {
    			ts1 = java.sql.Timestamp.valueOf(sDateNew);
    			ts2 = java.sql.Timestamp.valueOf(sDateOld);
    			// Convert to long values
    			long lNew  = ts1.getTime();
    			long lOld = ts2.getTime();
    			// Compare with tolerance
    			if (lNew <= lOld ){
    				bReturn=false;
    				}
    			} catch (IllegalArgumentException ie) {
    				System.out.println("ERRROR:testMData: Invalid date format for the method");
    				System.out.println("Check parameters: 1:["+sDateNew+"] and 2:["+sDateOld+"]");
    				ie.printStackTrace();
    			}
    			return bReturn;
    		}
  
		return bReturn;
	}

	private static CGReconTrade cleanRecord(CGReconTrade cgrtTrade) {
		CGReconTrade cgrtReturn = null;
		ScctReconType srtReturn = cgrtTrade.getreconType();
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
                if (sFieldName.equalsIgnoreCase("TradeEffDate")){//Added-ss78999
					
                	srtReturn.setTradeEffDt("");
				}
                if (sFieldName.equalsIgnoreCase("TradeDate")){
					
                	srtReturn.setTradeDt("");
				}
				if (sFieldName.equalsIgnoreCase("SettleDate")){
					srtReturn.setSettlementDt("");
				}
				if (sFieldName.equalsIgnoreCase("Strategy")){
					srtReturn.setStrategy("");
				}
				if (sFieldName.equalsIgnoreCase("MaturityDate")){
					srtReturn.setMaturityDt("");
				}
				if (sFieldName.equalsIgnoreCase("FirstCpnDate")){
					srtReturn.setFirstCpnDt("");
				}
				if (sFieldName.equalsIgnoreCase("LastCpnDate")){
					srtReturn.setLastCpnDt("");
				}
				if (sFieldName.equalsIgnoreCase("FixedRecRate")){
					srtReturn.setFixedRecoveryRate(0);
				}
				if (sFieldName.equalsIgnoreCase("AccruedCrCuId")){
					srtReturn.setActiveCreditCurveId(0);
				}
				if (sFieldName.equalsIgnoreCase("OpsFileName")){
					srtReturn.setOpsFileName("");
				}
				if (sFieldName.equalsIgnoreCase("SrcSystem")){
					srtReturn.setSrcSys("");
				}
				if (sFieldName.equalsIgnoreCase("PremPayFreq")){
					srtReturn.setPremPayFreq("");
				}
				if (sFieldName.equalsIgnoreCase("PremR")){
					srtReturn.setPremRate(0);
				}
				if (sFieldName.equalsIgnoreCase("DaysTyp")){
					srtReturn.setDaysType("");
				}
				if (sFieldName.equalsIgnoreCase("StubRule")){
					srtReturn.setStubRule("");
				}
				if (sFieldName.equalsIgnoreCase("CpnDRollTyp")){
					srtReturn.setCpnRollType("");
				}
				if (sFieldName.equalsIgnoreCase("HldyClndrs")){
					srtReturn.setHldCalenders("");
				}//End
				
				
			}
			cgrtReturn = new CGReconTrade(srtReturn);
		} catch (CGException e) {
			logger.error("cleanRecord: trade "+cgrtTrade.getTradeId()+" failed to modify");
			return null;
		}
		return cgrtReturn;
	}

	private static int getPoolSize(String env) {
		ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig(env, reconCfg);
		int poolSize = config.getPoolSize();
		return poolSize;
	}

	public static void loadClpTrades(String env, String sInstance, String login,
			String passwd) throws CGException {

		ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig(env, reconCfg);
		Map<Integer,CGReconTrade> mCalTrades = Collections.synchronizedMap(new TreeMap());
		
		if (config != null) {
			ExecutorService exCalExecutor = Executors.newFixedThreadPool(config.getPoolSize());
			List<String> queries;
			try {
				queries = getClpQueries();
				Hashtable<String, Future> htCalypsoResult = new Hashtable<String, Future>();
				if (queries != null && queries.size() > 0) {
					int cnt = 0;
					for (String xml : queries) {
						CalypsoTradesDAO dao = new CalypsoTradesDAO();
						if (!CGUtilities.isStringEmpty(xml)) {
							Future future = exCalExecutor.submit(new CalypsoCallableHandler(dao, sInstance, login, passwd, xml, retry, delay));
							htCalypsoResult.put(xml, future);
						}
						if ((cnt%config.getPoolSize())==0) {
							try {
								 Thread.currentThread().sleep(5*1000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						cnt++;
					}
				
				}
	
				exCalExecutor.shutdown();
				logger.debug("Shutting Calypso Execution Service ....... clpTrades cnt = "
								+ arlClpTrades.size());

				while (!exCalExecutor.isTerminated()) {
					try {
						Thread.currentThread().sleep(3 * 1000);
						logger.debug("Calypso Execution Service : Waiting for all tasks to complete");
					} catch (InterruptedException e) {
						logger.warn(
								"Calypso Execution Service : Caught InterruptedException "
										+ e.getMessage(), e);
					}
				}
				logger.debug("Calypso Execution Service SHUTDOWN COMPLETE.");
				// Getting sort list of the Gemfire trades
				processResults("Calypso", htCalypsoResult, mCalTrades);
				arlClpTrades = new ArrayList<CGReconTrade>(mCalTrades.values());
				System.out.println("Srini loadClpTrades arlClpTrades....."+arlClpTrades.size());
				
				
			}catch (CGException e1) {
				e1.printStackTrace();
				String sEMsg = e1.getMessage();
				String sErorMesg ="loadClpTrades: Unable to retrive Calypso Trades because "+ sEMsg;
				logger.error(sErorMesg);
				Exception ex = new Exception(sErorMesg);
				throw  new CGException(ex);
				
			}
		}
		else {
			logger.error("Unable to retrieve Calypso Configuration");
		}
	}

	private static String marshallQuery2(Marshaller marshaller,
			QueryScctReconType object) {
		StringWriter w = new StringWriter();
		try {
			JAXBElement element = new JAXBElement(new QName(
					"ns2:queryScctRecon"), QueryScctReconType.class, object);
			marshaller.marshal(element, w);
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug(w.toString());
		return w.toString();
	}

	private static String marshallQuery(Marshaller marshaller,
			QueryScctReconType object) {
		String s1 = "<ns2:queryScctRecon xmlns:ns2=\"http://www.citigroup.com/scct/cgen\">";
		StringBuffer buf = new StringBuffer(s1);
		String from = object.getFrom();
		if (!CGUtilities.isStringEmpty(from)) {
			buf.append("<from>");
			buf.append(object.getFrom());
			buf.append("</from>");

		}
		String where = object.getWhere();
		if (!CGUtilities.isStringEmpty(where)) {
			buf.append("<where>");
			buf.append(object.getWhere());
			buf.append("</where>");

		}
		buf.append("</ns2:queryScctRecon>");

		logger.debug(buf.toString());
		return buf.toString();
	}

	public static void reportMismatchTrades(String filename) {
		File fReport = null;
		FileWriter fwReport = null;
		BufferedWriter bwReport = null;
		String sRepHeader = "Trade Id|Break Type|Calypso Value|Gemfire Value";
		System.out.println("Srini reportMismatchTrades----");
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
			    System.out.println("Srini report--->"+sMisRecord+"\n");
			    bwReport.flush();
			}
			// Part II List Calypso trade's id missing in Gemfire:
			if ( vMissingCalypsoids.size() > 0){
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
			  for (int i=0; i < vMissingCalypsoids.size(); i++){
				  sbLine.append(vMissingCalypsoids.get(i)).append(", ");
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
			  sbRecord.append("TOTAL trades missing in Gemfire: "+vMissingCalypsoids.size()+"\n");
			  sbRecord.append("==========================================================\n");
			  sbRecord.append("\n");
			  bwReport.write(sbRecord.toString());
			  bwReport.flush();
			}
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
	
	public static void logMismatchTrades(String filename,
			Hashtable<Integer, ArrayList> source) {
		File f = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			f = new File(filename);
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
			List<Integer> tradeIds = new ArrayList<Integer>();
			tradeIds.addAll(source.keySet());
			Collections.sort(tradeIds);
			Iterator itr = tradeIds.iterator();
			StringBuffer buf = new StringBuffer();
			while (itr.hasNext()) {
				buf = new StringBuffer();
				int tradeId = (Integer) itr.next();
				ArrayList<String> arTrades = (ArrayList) source.get(tradeId);
				buf.append(tradeId).append("/");
				buf.append(arTrades.toString()).append("\n");
				bw.write(buf.toString());
				bw.flush();
				buf.delete(0, buf.length());
			}
		} catch (IOException e) {
			logger.warn("Unable to write to '" + filename + "'", e);
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					logger.error("reportMismatchTrades: Error while closing file "+filename);
					e.printStackTrace();
				}
			}
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					logger.error("reportMismatchTrades: Error while closing file "+filename);
					e.printStackTrace();
				}
			}
		}
	}

	public static String getFileName(String env, String type) {

		StringBuffer buf = new StringBuffer();

		if (reconCfg != null) {
			ScctReconConfigEnvType envCfg = (ScctReconConfigEnvType) reconCfg
					.get(env);
			ScctOutputConfigType outCfg = envCfg.getOutputConfig();
			if (type.equals("Calypso")) {
				buf.append(outCfg.getCalypsoOutputPrefix()).append("_");
				buf.append(getTimeStamp(outCfg.getCalypsoOutputSuffix()))
						.append(".csv");
			} else if (type.equals("Gemfire")) {
				buf.append(outCfg.getGemfireOutputPrefix()).append("_");
				buf.append(getTimeStamp(outCfg.getGemfireOutputSuffix()))
						.append(".csv");
			} else if (type.equals("Output")) {
				buf.append(outCfg.getReportOutputPrefix()).append("_");
				buf.append(getTimeStamp(outCfg.getReportOutputSuffix()))
						.append(".csv");
			} else if (type.equals("Mismatch")) {
				buf.append(outCfg.getMismatchOutputPrefix()).append("_");
				buf.append(getTimeStamp(outCfg.getMismatchOutputSuffix()))
						.append(".csv");
			}
		}
		return buf.toString();
	}

	private static String getTimeStamp(String format) {
		String ts = null;

		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			ts = sdf.format(Calendar.getInstance().getTime());
		} catch (RuntimeException e) {
			logger.warn("Unable to format current timestamp format '" + format
					+ "'");
		}
		return ts;
	}

	public static long getTermOffset(boolean start) {
		long offset = 0l;
		Calendar cal = null;
		try {
			cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			if (start) {
				cal = setTimeOffset(true, cal);
				ScctGemfireConfigType config = ReconUtil.getGemfireConfig(ENV,
						reconCfg);
				ScctReconTermType term = config.getTermination();
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
				cal = setTimeOffset(false, cal);
				 cal.setTimeInMillis(startTime);//Added -ss78999
			}
			offset = cal.getTimeInMillis();
			
		} catch (RuntimeException e) {
			logger.warn("Caught Exception '" + e.getMessage(), e);
		}
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		logger.debug("Calendar Offset : " + sdf.format(cal.getTime()));
		
		return offset;
	}

	public static String getTerminationDtOffset(ScctGemfireConfigType config) {
		SimpleDateFormat sdf = null;
		String startDt = null;
		String endDt = null;

		String timeZone = "GMT";
		int dtOffset;
		String dtOffsetUnit;
		String dtOffsetFmt;
		ScctReconTermType term = config.getTermination();
		if (term != null) {
			dtOffset = term.getTermDtOffset();
			dtOffsetUnit = term.getTermDtOffsetUnit();
			dtOffsetFmt = term.getTermDtFormat();
		} else {
			return null;
		}

		try {
			sdf = new SimpleDateFormat(dtOffsetFmt);
		} catch (RuntimeException e1) {
			logger.warn("Caught Exception parsing date format '" + dtOffsetFmt
					+ "'" + e1.getMessage(), e1);
			sdf = new SimpleDateFormat(DATE_FORMAT);
		}

		TimeZone tz = null;
		if (!CGUtilities.isStringEmpty(timeZone)) {
			tz = TimeZone.getTimeZone(timeZone);
		} else {
			tz = TimeZone.getTimeZone("GMT");
		}

		// need indicator for timezone flag
		Calendar cal = Calendar.getInstance(tz);
		sdf.setTimeZone(tz);

		if (dtOffset > 0) {
			startDt = sdf.format(setTimeOffset(true, cal).getTime());
			cal.add(lookupOffsetUnit(dtOffsetUnit), dtOffset);
			endDt = sdf.format(setTimeOffset(false, cal).getTime());
		} else {
			endDt = sdf.format(setTimeOffset(false, cal).getTime());
			cal.add(lookupOffsetUnit(dtOffsetUnit), dtOffset);
			startDt = sdf.format(setTimeOffset(true, cal).getTime());
		}
		StringBuffer buf = new StringBuffer();
		buf.append(" and t.update_date_time between {ts '").append(startDt)
				.append("'} ");
		// buf.append(" and {ts '").append(endDt).append("'} ");
		 buf.append(" and {ts '").append(sdf.format(dNow)).append("'} ");//Added-ss78999
		logger.debug("UPDATE DATE TIME : '" + buf.toString() + "'");
		
		return buf.toString();
	}
	//Added-ss78999
	private static String getDateString(ScctGemfireConfigType config)
	{
		String endDate = null;
		SimpleDateFormat sdf = null;
		String timeZone = "GMT";
		String dtOffsetFmt;
		ScctReconTermType term = config.getTermination();
		if (term != null) {
			dtOffsetFmt = term.getTermDtFormat();
		} else {
			return null;
		}
		sdf = new SimpleDateFormat(dtOffsetFmt);
		TimeZone tz = null;
		if (!CGUtilities.isStringEmpty(timeZone)) {
			tz = TimeZone.getTimeZone(timeZone);
		} else {
			tz = TimeZone.getTimeZone("GMT");
		}
		sdf.setTimeZone(tz);
		endDate = sdf.format(dNow);
		System.out.println("getDateString--->"+endDate);
		
		return endDate;
	}//End

	private static int lookupOffsetUnit(String s) {
		if ("MONTH".equalsIgnoreCase(s)) {
			return Calendar.MONTH;
		} else {
			return Calendar.DAY_OF_MONTH;
		}
	}

	private static Calendar setTimeOffset(boolean start, Calendar cal) {
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
			Vector<Integer> missingTrades, int defaultBatchSize) {
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
					logger.warn("Caught Exception reloading trades batch [" + j
							+ "]");
				}
			}
		} else {
			logger.debug(gateway.getClass().getName()+ " -- Nothing to reload");
		}
	}

	private static boolean reload(String type) {
		return (Boolean.TRUE.toString().equalsIgnoreCase(cmdArgs.getProperty(type, "false")));
	}
	
	// Recon
	// retrieve Gemfire(GF) trades
	// retrieve Calypso(CLP) trades
	// compare GF trades missing in CLP, report exception
	// compare CLP trades missing in GF, report exception
	// for common trades, compare field by field, report exception
	static double gfeLoadingTime = 0d;

	static double clpLoadingTime = 0d;

	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd k:m:s");
	    //Date dNow = new Date(); 
		dNow = new Date();//Added -ss78999
		System.out.println("Started Execution at "+ sdf.format(dNow));
		startTime = dNow.getTime();
		System.out.println("Started Execution at long---->"+startTime);
		String platform = System.getProperty("os.name", "Windows");
		if (platform.startsWith("Windows")) {
			PropertyConfigurator.configure(CGConstants.LOG_FILE);
		} else {
			PropertyConfigurator.configure("../config/log4j.properties");
		}
		System.out.println("#### PLATFORM : '" + platform);

		EntityRegisterHelper entityHelper = new com.citi.rds.model.EntityRegisterHelper();
		entityHelper.registerClassIds();
        // Processing parameters:
		loadCmdArgs(args);
		// Getting parameter for java call:
		sRepFileName = System.getProperty("repname");
		
		szLogin	      = 	CGUtilities.getKeyFromMap(cmdArgs, LOGIN_KEY, CALYPSO_LOGIN);
		//szPasswd	  = 	CGUtilities.getKeyFromMap(cmdArgs, PASSWD_KEY, CALYPSO_PASSWD);
		INSTANCE	  = 	CGUtilities.getKeyFromMap(cmdArgs, INSTANCE_KEY, DEFAULT_INSTANCE);
		//ENV	          = 	CGUtilities.getKeyFromMap(cmdArgs, ENV_KEY, "prod");
		EXCLUDE	      = 	CGUtilities.getKeyFromMap(cmdArgs, EXCLUDE_KEY, "");
		PRODUCTS      = 	CGUtilities.getKeyFromMap(cmdArgs, PRODUCTS_KEY, "");
		PRINT_TO_FILE = 	CGUtilities.getKeyFromMap(cmdArgs, PRINT_KEY, "Y");
		String sDtTl  = 	CGUtilities.getKeyFromMap(cmdArgs, DATE_TLRNC_KEY, "0");
		//GEM_SRC       = 	CGUtilities.getKeyFromMap(cmdArgs, GEM_SRC_KEY, "");
		EXTERNAL_IN   = 	CGUtilities.getKeyFromMap(cmdArgs, EXTERNAL_IN_KEY, "OASYS");
		System.out.println("Srini INSTANCE------>"+INSTANCE);
		System.out.println("Srini EXCLUDE------>"+EXCLUDE);
		System.out.println("Srini sDtTl------>"+sDtTl);
		System.out.println("Srini GEM_SRC------>"+GEM_SRC);
		System.out.println("Srini EXTERNAL_IN------>"+EXTERNAL_IN);
		ENV = "uat";
		szPasswd = "calypso";
		System.out.println("Srin ENV----->"+ENV);
		GEM_SRC ="CALYPSO";
				
		if (!sDtTl.equals("0")){
			//Convert parameter to Long
			Long lTl = new Long(sDtTl);
			DT_TLRNC = lTl.longValue();
		}
		// Test ENV
		if (ENV.length() == 0 && ENV.trim().equals("")){
			logger.error("Parameter env not set. Exit");
			System.exit(1);
		}
		if (EXCLUDE.length() > 0 && !EXCLUDE.trim().equals("")){
			String[] saFields = EXCLUDE.split("~");
			for (int z=0; z < saFields.length; z++){
				vExclude.add(saFields[z]);
			}
		}
		if (PRODUCTS.length() > 0 && !PRODUCTS.trim().equals("")){
			String[] saFields = PRODUCTS.split("~");
			for (int z=0; z < saFields.length; z++){
				vProductList.add(saFields[z]);
			}
		}
		else{
			logger.error("Empty product list.EXIT");
			System.exit(1);
		}
		if (GEM_SRC.length() > 0 && !GEM_SRC.trim().equals("")){
			if (!GEM_SRC.equalsIgnoreCase("CALYPSO") && !GEM_SRC.equalsIgnoreCase("CALYPSOEM")){
				logger.error("Invalid Gemfire source setting => srcSys set to: " + GEM_SRC+" .EXIT");
				System.exit(1);
			}
		}
		if (!EXTERNAL_IN.trim().equals("OASYS")){
			// Parse list
			String[] saEXIN = EXTERNAL_IN.split("~");
			for (int z=0; z < saEXIN.length; z++){
				if (EXTERNAL_IN_SQL.equals("")){
					EXTERNAL_IN_SQL="'"+saEXIN[z]+"'";					
				}
				else{
					EXTERNAL_IN_SQL=EXTERNAL_IN_SQL+", '"+saEXIN[z]+"'";
				}
			}

		}
		else{
			EXTERNAL_IN_SQL="'"+EXTERNAL_IN+"'";
		}
		if (!CGUtilities.isStringEmpty(cmdArgs.getProperty("defaultQuery"))) {
			defaultGfeQuery = Boolean.valueOf(cmdArgs.getProperty("defaultQuery"));
		}
		
		if (PRINT_TO_FILE == "Y"){
			if (sRepFileName.trim().length() == 0 ){
				logger.error("Printing set to: Y but report file name is Empty .EXIT");
				System.exit(1);
			}
		}
		
		double reconTime = 0d;

		try {
			if (platform.startsWith("Windows")) {
				System.out.println("Windows");
				init("./config/gemfireReconConfig.xml");
			} else {
				System.out.println("loading Solaris");
				init("../config/gemfireReconConfig.xml");
			}

			Thread t1 = new Thread(new Runnable() {
				public void run() {
					long x = System.currentTimeMillis();
					try {
						loadGfeTrades(ENV);
					} catch (ConfigException e) {
						logger.warn("Caught Exception '" + e.getMessage(), e);
						System.exit(1);
					}
					long y = System.currentTimeMillis();
					gfeLoadingTime = (y - x) / 1000.0;
					done.put("Gemfire", Boolean.TRUE);
				}
			});
			t1.start();

			Thread t2 = new Thread(new Runnable() {
				public void run() {
					long x = System.currentTimeMillis();
					try {
						loadClpTrades(ENV, INSTANCE, szLogin, szPasswd);
					} catch (CGException e) {
						logger.warn("Caught Exception retrieving Calypso trades '" + e.getMessage(), e);
						System.exit(1);
					}
					long y = System.currentTimeMillis();
					clpLoadingTime = (y - x) / 1000.0;
					done.put("Calypso", Boolean.TRUE);
				}
			});
			t2.start();

			boolean complete = false;
			while (!complete) {
				Boolean b1 = (Boolean) done.get("Gemfire");
				Boolean b2 = (Boolean) done.get("Calypso");
				if (b1 && b2) {
					complete = true;
				} else {
					try {
						Thread.currentThread().sleep(10 * 1000);
					} catch (InterruptedException e) {
						logger.warn("Caught InterruptedException ", e);
					}
				}
			}
 
			// find Gemfire trades missing in Calypso
			long x = System.currentTimeMillis();
			// Added for test 
			// Save all trade recieved:
			//logRecievedTrades("CalypsoTrades.csv", arlClpTrades );
			//logRecievedTrades("GfeTrades.csv", arlGfeTrades );
			//System.exit(0);
			// end test
		
			int iCalIndex = 0;
			int iGFEIndex = 0;
			 // Iterate over both collections but not always in the same pace
			  while( iCalIndex < arlClpTrades.size() 
			      && iGFEIndex < arlGfeTrades.size())  {
				  CGReconTrade cgrt = (CGReconTrade)arlClpTrades.get(iCalIndex);
				  if ( cgrt == null ){
					  iCalIndex++;
					  continue;
				  }
				  ScctReconType ClpTrade = (ScctReconType) cgrt.getreconType();
				  ScctReconType GfeTrade = (ScctReconType) arlGfeTrades.get(iGFEIndex);
				  if ( GfeTrade == null){
					  iGFEIndex++;	
					  continue;
				  }
				  //System.out.println("Calypso ID: "+ClpTrade.getTradeId()+" vs GFE ID: "+GfeTrade.getTradeId());
				  if(ClpTrade.getTradeId() < GfeTrade.getTradeId()) {
					  vMissingCalypsoids.add(ClpTrade.getTradeId());
					  iCalIndex++;
				  }
				  else if(ClpTrade.getTradeId() > GfeTrade.getTradeId()) {
					  vMissingGFEids.add(GfeTrade.getTradeId());
					  iGFEIndex++;					  
				  } else if(ClpTrade.getTradeId() == GfeTrade.getTradeId()) {
					  if (TradeDifference(ClpTrade, GfeTrade) != 0){
					      prepareAndAddRecRecord(ClpTrade, GfeTrade);						  
					  }
				      iCalIndex++;
				      iGFEIndex++;
				  }
			  }
			  // Process any left trades:
			  // Calypso
			  while ( iCalIndex < arlClpTrades.size()){
				  CGReconTrade cgrt = (CGReconTrade)arlClpTrades.get(iCalIndex);
				  ScctReconType ClpTrade = (ScctReconType) cgrt.getreconType();
				  vMissingCalypsoids.add(ClpTrade.getTradeId());
				  iCalIndex++;
			  }
			  // Gemfire
			  while (iGFEIndex < arlGfeTrades.size()){
				  ScctReconType GfeTrade = (ScctReconType) arlGfeTrades.get(iGFEIndex);
				  vMissingGFEids.add(GfeTrade.getTradeId());
				  iGFEIndex++;
			  }
			  
			  // Printing Missing in Gemfire:
			  if ( vMissingCalypsoids.size() > 0){
				  // Print header
				  logger.debug("");				  
				  logger.debug("Trade Ids in Calypso but missing in GFE");
				  logger.debug("==========================================================");
				  logger.debug("");
				  int iMaxLength = 80;
				  StringBuffer sbLine = new StringBuffer();
				  for (int i=0; i < vMissingCalypsoids.size(); i++){
					  sbLine.append(vMissingCalypsoids.get(i)).append(", ");
					  if (sbLine.toString().length() >= iMaxLength){
						  //sbLine.append("\n");
						  logger.debug(sbLine.toString());
						  sbLine.delete(0, sbLine.length());
					  }
				  }
				  // Print the rest if in the buffer
				  if (sbLine.toString().length() > 0 ){
					  String sOut = sbLine.substring(0, sbLine.length()-2).toString()+ "\n";
					  logger.debug(sOut);
				  }
				  logger.debug("TOTAL missing trades in GFE: "+vMissingCalypsoids.size());
				  logger.debug("END of missing trades in GFE");
				  logger.debug("==========================================================");
				  logger.debug("");
				  
			  }
			 
			  if ( arMismatch.size() > 0){
				  if ( PRINT_TO_FILE.equals("Y")){ 
					  reportMismatchTrades(sRepFileName);
				  }
				  else{
					  // Printing Missmatch:
					  String arResults[] = new String[arMismatch.size()];
					  arResults = arMismatch.toArray(arResults);
					  Arrays.sort(arResults);
					  // Print header
					  logger.debug("Trade Id   Break Type    Calypso Value      Gemfire Value");
					  logger.debug("==========================================================");
					  for (String sMisRecord : arResults){
						  logger.debug(sMisRecord+"\n");
					  }
					  logger.debug("==========================================================");
					  logger.debug("====== Total breaks # "+ arMismatch.size()+ " ====");
					  logger.debug("======================DONE================================");				  
				  }
			  }
			  else{
				  logger.debug("No mismatch record(s) found. Printing cancel.");
			  }

			  // End DF new recon#
			 
		    long y = System.currentTimeMillis();
			reconTime = (y - x) / 1000.0;

			//String instance = cmdArgs.getProperty("instance",INSTANCE);
			int batchSz = Integer.parseInt(cmdArgs.getProperty("batchReloadSize", "1000"));
			CreditGatewayInterface gateway = null;
			try {
				gateway = new CreditGatewayImpl(INSTANCE, szLogin, szPasswd);
				if (reload("reloadGfe")) {
					if (vMissingGFEids.size() > 0){
						logger.debug("Reloading Gemfire Trades Missing in Calypso : " + vMissingGFEids.size());
						reloadTrades(gateway, vMissingGFEids, batchSz);						
					}
				}

				if (reload("reloadClp")) {
					if (vMissingCalypsoids.size() > 0){
						logger.debug("Reloading Calypso Trades Missing in Gemfire : " + vMissingCalypsoids.size());
						reloadTrades(gateway, vMissingCalypsoids, batchSz);						
					}

				}

				if (reload("reloadMismatch")) {
					logger.debug("Reloading Mismatch Trades " + arMismatch.size());
					if (arMismatch.size() > 0){
						Vector<Integer> vMissMatchTrades = new Vector<Integer>();
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (gateway != null) {
					try {
						gateway.closeGateway();
					} catch (CGException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} catch (ConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			logger.debug("+++++++++ Calypso - Gemfire Reconcialiation Statistics : ++++++++++");
			logger.debug("Gemfire Loading Time : " + gfeLoadingTime);
			logger.debug("Calypso Loading Time : " + clpLoadingTime);
			logger.debug("Reconciliation  Time : " + reconTime);
			logger
					.debug("===================================================================");

		}
	}
	// Added for test
	private static void logRecievedTrades(String sFl, ArrayList arlTrades) {
		File fReport = null;
		FileWriter fwReport = null;
		BufferedWriter bwReport = null;
		try {
			fReport = new File(sFl);
			fwReport = new FileWriter(fReport);
			bwReport = new BufferedWriter(fwReport);
			ArrayList arReport = new ArrayList();
			arReport.addAll(arlTrades);
			//Collections.sort(arReport);
			Iterator itr = arReport.iterator();
			StringBuffer sbRecord = new StringBuffer();
			String sHeader = "TradeId|Version|Book|B/S|Cpty|Ccy|Notional|Status|Update Date|\n ";
			bwReport.write(sHeader);
			while (itr.hasNext()) {
				sbRecord = new StringBuffer();
				//int iTrId = (Integer) itr.next();
				if (sFl.indexOf("Calypso") >= 0){
			        CGReconTrade sVal = (CGReconTrade)	itr.next();;
		        	sbRecord.append(sVal.getTradeId()).append("|");
		        	sbRecord.append(sVal.getVersion()).append("|");
		        	sbRecord.append(sVal.getBook()).append("|");
		        	sbRecord.append(sVal.getBuySell()).append("|");
		        	sbRecord.append(sVal.getCpty()).append("|");
		        	sbRecord.append(sVal.getCurrency()).append("|");
		        	sbRecord.append(sVal.getNotional()).append("|");
		        	sbRecord.append(sVal.getTradeStatus()).append("|");
		        	sbRecord.append(sVal.getUpdateTime());
				}else{
			    	ScctReconType sVal = (ScctReconType)itr.next();
		        	sbRecord.append(sVal.getTradeId()).append("|");
		        	sbRecord.append(sVal.getVersion()).append("|");
		        	sbRecord.append(sVal.getBook()).append("|");
		        	sbRecord.append(sVal.getBuySell()).append("|");
		        	sbRecord.append(sVal.getCpty()).append("|");
		        	sbRecord.append(sVal.getCurrency()).append("|");
		        	sbRecord.append(sVal.getNotional()).append("|");
		        	sbRecord.append(sVal.getTradeStatus()).append("|");
		        	sbRecord.append(sVal.getUpdateDt());
			    }
				sbRecord.append("\n");
				bwReport.write(sbRecord.toString());
				bwReport.flush();
			}
			bwReport.close();
		}
		catch(Exception e){
			logger.error("logRecievedTrades: Failed to process");
			e.getStackTrace();
		}
	
		
	}

	public static int  TradeDifference(ScctReconType cgrtCollection, ScctReconType sctrCollection) {
		int iReturn = -1;
		try{
			ScctTradeComparator stc = new ScctTradeComparator();	
			iReturn=stc.compare(cgrtCollection, sctrCollection );
		}
		catch(Exception e){
			logger.error("TradeDifference: Problem to compare: Calypso "+cgrtCollection.getTradeId()+" and Gfe"+sctrCollection.getTradeId());
			
		}
		return iReturn;
	}
	
	public static void prepareAndAddRecRecord(ScctReconType cgrtCollection, ScctReconType sctrCollection){
		// All records will have same format:
		// Trade Id|Break Type| Calypso Value| Gemfire Value
		int iCalypsoTrId = cgrtCollection.getTradeId();
		int iCalypsoVer = cgrtCollection.getVersion();
		String sCalypsoBS = cgrtCollection.getBuySell();
		double dCalypsoNotional = doubleRound(cgrtCollection.getNotional(), iScale);
		String sCalypsoCcy = cgrtCollection.getCurrency();
		String sCalypsoCpty = trimLEBO(cgrtCollection.getCpty());
		String sCalypsoBook = cgrtCollection.getBook();
		String sCalypsoStatus = cgrtCollection.getTradeStatus();
		String sCalypsoUpDate = cgrtCollection.getUpdateDt();
		//START-ss78999
		//List<ScctFeeType> sCalypsoFeeList = cgrtCollection.getFees();
		String sCalypsoSettlementDate = cgrtCollection.getSettlementDt();
		String sCalypsoTradeEffDate = cgrtCollection.getTradeEffDt();
		String sCalypsoTradeDate = cgrtCollection.getTradeDt();
		String sCalypsoStrategy = cgrtCollection.getStrategy();
		String sCalypsoMaturityDate = cgrtCollection.getMaturityDt();
		String sCalypsoFirstCpnDate = cgrtCollection.getFirstCpnDt();
		String sCalypsoLastCpnDate = cgrtCollection.getLastCpnDt();
		double sCalypsoFixedRcvrRate = cgrtCollection.getFixedRecoveryRate();
		long sCalypsoActiveCrCuId = cgrtCollection.getActiveCreditCurveId();
		String sCalypsoopsFileName = cgrtCollection.getOpsFileName();
		//String sCalypsoSrcSys = cgrtCollection.getSrcSys();
		String sCalypsoPremPayFrq = cgrtCollection.getPremPayFreq();
		//double sCalypsoPremRate = cgrtCollection.getPremRate();
		double sCalypsopRate = cgrtCollection.getPremRate();
		BigDecimal dec = new BigDecimal(sCalypsopRate);
		double sCalypsoPremRate= dec.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
		String sCalypsoDaysTyp = cgrtCollection.getDaysType();
		String sCalypsoStubRule = cgrtCollection.getStubRule();
		String sCalypsoCpnDRollType = cgrtCollection.getCpnRollType();
		String sCalypsoHldCalds = cgrtCollection.getHldCalenders();
		//End
				
		int iGFETrId = sctrCollection.getTradeId();        
		int iGFEVer = sctrCollection.getVersion();         
		String sGFEBS = sctrCollection.getBuySell();       
		double dGFENotional = doubleRound(sctrCollection.getNotional(), iScale);
		String sGFECcy = cgrtCollection.getCurrency();
		String sGFECpty = sctrCollection.getCpty();         
		String sGFEBook = sctrCollection.getBook();        
		String sGFEStatus = sctrCollection.getTradeStatus();
		String sGFEUpDate = sctrCollection.getUpdateDt();
		//START-ss78999
		//List<ScctFeeType> sGEFFeesList = sctrCollection.getFees();
		String sGFESettlementDate = sctrCollection.getSettlementDt();
		String sGFETradeEffDate = sctrCollection.getTradeEffDt();
		String sGFETradeDate = sctrCollection.getTradeDt();
		String sGFEStrategy = sctrCollection.getStrategy();
		String sGFEMaturityDate = sctrCollection.getMaturityDt();
		String sGFEFirstCpnDate = sctrCollection.getFirstCpnDt();
		String sGFELastCpnDate = sctrCollection.getLastCpnDt();
		double sGFEFixedRcvrRate = sctrCollection.getFixedRecoveryRate();
		long sGFEActiveCrCuId = sctrCollection.getActiveCreditCurveId();
		String sGFEopsFileName = sctrCollection.getOpsFileName();
		//String sGFESrcSys = sctrCollection.getSrcSys();
		String sGFEPremPayFrq = sctrCollection.getPremPayFreq();
		double sGFEpremRate = sctrCollection.getPremRate();
		String sGFEDaysTyp = sctrCollection.getDaysType();
		String sGFEStubRule = sctrCollection.getStubRule();
		String sGFECpnDRollType = sctrCollection.getCpnRollType();
		String sGFEHldCalds = sctrCollection.getHldCalenders();
		//End
		
		if ( iCalypsoTrId  != iGFETrId ){
			logger.error("prepareAndAddRecRecord found missmatch trade ids Calypso: "+iCalypsoTrId+" GFE: "+iGFETrId);
			return;
		}
		if (iCalypsoVer != iGFEVer){
			String sDiff = iCalypsoTrId+"|Version|"+iCalypsoVer+"|"+iGFEVer;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (!sCalypsoBS.equals(sGFEBS)){
			String sDiff = iCalypsoTrId+"|BuySell|"+sCalypsoBS+"|"+sGFEBS;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (Double.compare(dCalypsoNotional,dGFENotional) != 0){
			String sDiff = iCalypsoTrId+"|Notional|"+dCalypsoNotional+"|"+dGFENotional;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if ( !sCalypsoCcy.equals(sGFECcy)){
			String sDiff = iCalypsoTrId+"|Ccy|"+sCalypsoCcy+"|"+sGFECcy;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if ( !sCalypsoCpty.equals(sGFECpty)){
			String sDiff = iCalypsoTrId+"|Cpty|"+sCalypsoCpty+"|"+sGFECpty;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if ( !sCalypsoBook.equals(sGFEBook)){
			String sDiff = iCalypsoTrId+"|Book|"+sCalypsoBook+"|"+sGFEBook;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if ( !sCalypsoStatus.equals(sGFEStatus)){
			String sDiff = iCalypsoTrId+"|Status|"+sCalypsoStatus+"|"+sGFEStatus;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		String sCalUpDtNoSS= sCalypsoUpDate.substring(0, sCalypsoUpDate.lastIndexOf('.'));
		String sGfeUpDtNoSS= sGFEUpDate.substring(0, sGFEUpDate.lastIndexOf('.'));		
		if ( matchDates(sCalUpDtNoSS, sGfeUpDtNoSS) != 0 ){
			String sDiff = iCalypsoTrId+"|TradeUpdateDate|"+sCalypsoUpDate+"|"+sGFEUpDate;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		//START-ss78999
		System.out.println("Srini sCalypsoSettlementDate---->"+sCalypsoSettlementDate);
		System.out.println("Srini sGFESettlementDate @@@---->"+sGFESettlementDate);
		if( sCalypsoSettlementDate != "" && sCalypsoSettlementDate != null && sCalypsoSettlementDate.length() != 0 && sGFESettlementDate.length() !=0 && sGFESettlementDate!= null)
		{
			System.out.println("Srini sCalypsoSettlementDate Inside---->"+sCalypsoSettlementDate);
			System.out.println("Srini sGFESettlementDate Inside---->"+sGFESettlementDate);
			String sCalSettleDate = sCalypsoSettlementDate.substring(0,sCalypsoSettlementDate.lastIndexOf('.'));//STARTED-ss78999
			String sGfeSettleDate = sGFESettlementDate.substring(0, sGFESettlementDate.lastIndexOf('.'));
			if(matchDates(sCalSettleDate,sGfeSettleDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|TradeSettleDate|"+sCalSettleDate+"|"+sGfeSettleDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
	   }
		System.out.println("Srini sCalypsoTradeEffDate---->"+sCalypsoTradeEffDate);
		System.out.println("Srini sCalypsoTradeEffDate @@@---->"+sCalypsoTradeEffDate);
		if(sCalypsoTradeEffDate != "" && sCalypsoTradeEffDate != null  && sCalypsoTradeEffDate.length() != 0 &&  sGFETradeEffDate!= "" && sGFETradeEffDate!= null && sGFETradeEffDate.length() !=0 )
		{
			String sCalTradeEffDate = sCalypsoTradeEffDate.substring(0,sCalypsoTradeEffDate.lastIndexOf('.'));
			String sGfeTradeEffDate = sGFETradeEffDate.substring(0, sGFETradeEffDate.lastIndexOf('.'));
			if(matchDates(sCalTradeEffDate,sGfeTradeEffDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|TradeEffectiveDate|"+sCalTradeEffDate+"|"+sGfeTradeEffDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
		}
		System.out.println("Srini sCalypsoTradeDate---->"+sCalypsoTradeDate);
		System.out.println("Srini sGFETradeDate $$$$$---->"+sGFETradeDate);
		if(sCalypsoTradeDate != "" && sCalypsoTradeDate != null && sCalypsoTradeDate.length() != 0  && sGFETradeDate !=  ""  && sGFETradeDate!= null && sGFETradeDate.length() !=0)
		{
			String sCalTradeDate = sCalypsoTradeDate.substring(0,sCalypsoTradeDate.lastIndexOf('.'));
			String sGfeTradeDate = sGFETradeDate.substring(0, sGFETradeDate.lastIndexOf('.'));
			if(matchDates(sCalTradeDate,sGfeTradeDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|TradeDate|"+sCalTradeDate+"|"+sGfeTradeDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
		}
		if ( sCalypsoStrategy != "" && sCalypsoStrategy != null && sGFEStrategy != ""  && sGFEStrategy != null && !sCalypsoStrategy.equals(sGFEStrategy)){
			String sDiff = iCalypsoTrId+"|Strategy|"+sCalypsoStrategy+"|"+sGFEStrategy;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if(sCalypsoMaturityDate != "" && sCalypsoMaturityDate != null && sCalypsoMaturityDate.length() != 0  &&  sGFEMaturityDate != ""  && sGFEMaturityDate!= null && sGFEMaturityDate.length() !=0)
		{
			String sCalMaturityDate = sCalypsoMaturityDate.substring(0,sCalypsoMaturityDate.lastIndexOf('.'));
			String sGfeMaturityDate = sGFEMaturityDate.substring(0, sGFEMaturityDate.lastIndexOf('.'));
			if(matchDates(sCalMaturityDate,sGfeMaturityDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|TradeMaturityDate|"+sCalMaturityDate+"|"+sGfeMaturityDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
		}
		if(sCalypsoFirstCpnDate != "" && sCalypsoFirstCpnDate != null && sCalypsoFirstCpnDate.length() != 0 && sGFEFirstCpnDate !=  "" && sGFEFirstCpnDate != null && sGFEFirstCpnDate.length() !=0)
		{
			
			String sCalFirstCpnDate = sCalypsoFirstCpnDate.substring(0,sCalypsoFirstCpnDate.lastIndexOf('.'));
			String sGfeFirstCpnDate = sGFEFirstCpnDate.substring(0, sGFEFirstCpnDate.lastIndexOf('.'));
			if(matchDates(sCalFirstCpnDate,sGfeFirstCpnDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|FirstCouponDate|"+sCalFirstCpnDate+"|"+sGfeFirstCpnDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
		}
		if(sCalypsoLastCpnDate != null && sCalypsoLastCpnDate != "" && sCalypsoLastCpnDate.length() !=0 && sGFELastCpnDate != null && sGFELastCpnDate != "" && sGFELastCpnDate.length() != 0)
		{
			String sCalLastCpnDate = sCalypsoLastCpnDate.substring(0,sCalypsoLastCpnDate.lastIndexOf('.'));
			String sGfeLastCpnDate = sGFELastCpnDate.substring(0, sGFELastCpnDate.lastIndexOf('.'));
			if(matchDates(sCalLastCpnDate,sGfeLastCpnDate) != 0)
			{
				String sDiff = iCalypsoTrId+"|LastCouponDate|"+sCalLastCpnDate+"|"+sGfeLastCpnDate;
				//Update htMismatch wit diff
				arMismatch.add(sDiff);
			}
		}
		if ( Double.compare(sCalypsoFixedRcvrRate,sGFEFixedRcvrRate) != 0){
			String sDiff = iCalypsoTrId+"|FixedRecoveryRate|"+sCalypsoFixedRcvrRate+"|"+sGFEFixedRcvrRate;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoActiveCrCuId != sGFEActiveCrCuId){
			String sDiff = iCalypsoTrId+"|ActiveCurveId|"+sCalypsoActiveCrCuId+"|"+sGFEActiveCrCuId;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoopsFileName != "" && sCalypsoopsFileName != null  && sGFEopsFileName != "" && sGFEopsFileName != null && !sCalypsoopsFileName.equals(sGFEopsFileName)){
			String sDiff = iCalypsoTrId+"|OpsFileName|"+sCalypsoopsFileName+"|"+sGFEopsFileName;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		/*if ( !sCalypsoSrcSys.equals(sGFESrcSys)){
			String sDiff = iCalypsoTrId+"|SrcSystem|"+sCalypsoSrcSys+"|"+sGFESrcSys;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}*/
		if (sCalypsoPremPayFrq != "" && sCalypsoPremPayFrq != null && sGFEPremPayFrq != "" && sGFEPremPayFrq != null &&  !sCalypsoPremPayFrq.equals(sGFEPremPayFrq)){
			String sDiff = iCalypsoTrId+"|PremPayFrq|"+sCalypsoPremPayFrq+"|"+sGFEPremPayFrq;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (Double.compare(sCalypsoPremRate,sGFEpremRate) != 0){
			String sDiff = iCalypsoTrId+"|PremRate|"+sCalypsoPremRate+"|"+sGFEpremRate;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoDaysTyp != "" && sCalypsoDaysTyp != null && sGFEDaysTyp != "" && sGFEDaysTyp != null && !sCalypsoDaysTyp.equals(sGFEDaysTyp)){
			String sDiff = iCalypsoTrId+"|DayCount|"+sCalypsoDaysTyp+"|"+sGFEDaysTyp;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoStubRule != "" && sCalypsoStubRule != null && sGFEStubRule != "" && sGFEStubRule != null && !sCalypsoStubRule.equals(sGFEStubRule)){
			String sDiff = iCalypsoTrId+"|StubRule|"+sCalypsoStubRule+"|"+sGFEStubRule;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoCpnDRollType != "" && sCalypsoCpnDRollType != null && sGFECpnDRollType != "" && sGFECpnDRollType != null && !sCalypsoCpnDRollType.equals(sGFECpnDRollType)){
			String sDiff = iCalypsoTrId+"|CouponRollType|"+sCalypsoCpnDRollType+"|"+sGFECpnDRollType;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		if (sCalypsoHldCalds != "" && sCalypsoHldCalds != null && sGFEHldCalds != "" && sGFEHldCalds != null && !sCalypsoHldCalds.equals(sGFEHldCalds)){
			String sDiff = iCalypsoTrId+"|HolidayCalendars|"+sCalypsoHldCalds+"|"+sGFEHldCalds;
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}
		/*if(!sCalypsoFeeList.isEmpty() && !sGEFFeesList.isEmpty() && sCalypsoFeeList.size() == sGEFFeesList.size())
		{
			for(int k = 0;k<sCalypsoFeeList.size();k++)
			{
				ScctFeeType feeScct = (ScctFeeType)sCalypsoFeeList.get(k);
				String sCalyFeeType = feeScct.getFeeType();
				String sCalFeeDate = feeScct.getStartDate();
				double sCalFeeAmt = feeScct.getAmount().doubleValue();
				String sCalFeePayRec = feeScct.getPayRec();
				String sCalFeeCcy = feeScct.getCurrency();
				String sCalFeeLegalEntity = feeScct.getCounterParty().getCode();
				ScctFeeType sGfeeScct = (ScctFeeType)sGEFFeesList.get(k);
				String sGfeFeeType = sGfeeScct.getFeeType();
				String sGfeFeeDate = sGfeeScct.getStartDate();
				double sGfeFeeAmt = sGfeeScct.getAmount().doubleValue();
				String sGfeFeePayRec = sGfeeScct.getPayRec();
				String sGfeFeeCcy = sGfeeScct.getCurrency();
				String sGfeFeeLegalEntity = sGfeeScct.getCounterParty().getCode();
				
				if ( !sCalyFeeType.equals(sGfeFeeType)){
					String sDiff = iCalypsoTrId+"|FeeType|"+sCalyFeeType+"|"+sGfeFeeType;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
				if ( !sCalFeeCcy.equals(sGfeFeeCcy)){
					String sDiff = iCalypsoTrId+"|FeeCcy|"+sCalFeeCcy+"|"+sGfeFeeCcy;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
				String sGfefeeDate = sGfeFeeDate.substring(0, sGfeFeeDate.lastIndexOf('-')+3);
				if(!sCalFeeDate.equals(sGfefeeDate))
				{					
					String sDiff = iCalypsoTrId+"|FeeDate|"+sCalFeeDate+"|"+sGfefeeDate;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
				if (Double.compare(sCalFeeAmt,sGfeFeeAmt) != 0){
					String sDiff = iCalypsoTrId+"|FeeAmount|"+sCalFeeAmt+"|"+sGfeFeeAmt;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
				if( !sCalFeePayRec.equals(sGfeFeePayRec)){
					String sDiff = iCalypsoTrId+"|FeePayRec|"+sCalFeePayRec+"|"+sGfeFeePayRec;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
			  if(sGfeFeeLegalEntity != null && sGfeFeeLegalEntity != "" && sGfeFeeLegalEntity.length() != 0 && sCalFeeLegalEntity != null && sCalFeeLegalEntity != "" && sCalFeeLegalEntity.length() != 0)
			  {
				if( !sGfeFeeLegalEntity.equals(sCalFeeLegalEntity)){
					String sDiff = iCalypsoTrId+"|FeeLegalEntity|"+sGfeFeeLegalEntity+"|"+sCalFeeLegalEntity;
					//Update htMismatch wit diff
					arMismatch.add(sDiff);
				}
			  }
							
			}
		}
		else if(sCalypsoFeeList.size() !=0 && sGEFFeesList.size() !=0)
		{
			String sDiff = iCalypsoTrId+"|Number of Fees not matched|"+sCalypsoFeeList.size()+"|"+sGEFFeesList.size();
			//Update htMismatch wit diff
			arMismatch.add(sDiff);
		}*///End-ss78999
	}
	
	private static String trimLEBO(String cpty) {
		String val = (!CGUtilities.isStringEmpty(cpty) ? cpty : "");
		int idx = cpty.lastIndexOf("_BO");
		if (idx>0) {
			val = cpty.substring(0, idx);
		}
		return val;
	}
	
	private static double doubleRound(double dIn, int iPl){
	    double dReturn = dIn;
	    BigDecimal bd = new BigDecimal(dReturn);
	    bd = bd.setScale(iPl,BigDecimal.ROUND_HALF_UP);
	    dReturn = bd.doubleValue();
	    return dReturn;
	}
	
	private static int matchDates(String sDt1, String sDt2){
		java.sql.Timestamp ts1;
		java.sql.Timestamp ts2;
		int iRet = -1;
		try {
			ts1 = java.sql.Timestamp.valueOf(sDt1);
			ts2 = java.sql.Timestamp.valueOf(sDt2);
			// Convert to long values
			long lFirst  = ts1.getTime();
			long lSecond = ts2.getTime();
			// Compare with tolerance
			if ( Math.abs(lFirst - lSecond) <= CalypsoGemfireRecon.DT_TLRNC ){
				iRet = 0;
			}
		} catch (IllegalArgumentException ie) {
			System.out.println("ERRROR:ScctTradeComparator: Invalid date format for the method");
			System.out.println("Check parameters: 1:["+sDt1+"] and 2:["+sDt2+"]");
			ie.printStackTrace();
		}
		return iRet;
	}
}

