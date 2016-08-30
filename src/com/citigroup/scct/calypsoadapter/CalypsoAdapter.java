package com.citigroup.scct.calypsoadapter;

import calypsox.apps.citi.partialassignment.PANode;
import com.calypso.tk.core.*;
import com.calypso.tk.refdata.*;
import com.calypso.tk.service.*;
import com.calypso.tk.util.*;
import com.calypso.tk.bo.workflow.TradeWorkflow;
import javax.jms.*;
import javax.xml.bind.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.xml.sax.*;
import org.apache.log4j.*;
import com.citigroup.apps.trading.tradeamendment.ActionToReasonMap;
import com.citigroup.project.sql.IndexTradeCaptureData;
import com.citigroup.project.util.DBUtil;
import com.citigroup.scct.calypsoadapter.converter.*;
import com.citigroup.scct.calypsoadapter.findtool.*;
import com.citigroup.scct.calypsoadapter.monitor.*;
import com.citigroup.scct.calypsoadapter.publisher.*;
import com.citigroup.scct.cgen.*;
import com.citigroup.scct.server.ScctServer;
import com.citigroup.scct.util.*;
import com.citigroup.scct.util.DateUtil;
import com.citigroup.scct.valueobject.PartialAssignmentEntry;
import com.citigroup.scct.valueobject.ScctPANode;

import java.sql.Timestamp;
import java.sql.Connection;

/*
 * Change History (most recent first):
 *		
 *		11/12/2008	kt60981		:	Add QueryScctBookStrategyType
 *		11/05/2008	kt60981		:	Add QueryScctTradeGemfireType
 *		10/08/2008	kt60981		:	Add QueryScctReconType
 *		09/30/2008	kt60981		:	Add QueryScctTradePredicate
 *		08/11/2008	kt60981		:	Refactor Timer ctor
 *		07/31/2008	kt60981		:	Support additional non-streaming query
 *		07/21/2008	kt60981		:	Initialize validateTrade
 *		07/21/2008	ig91761		:	Moved property-reload logging to separate logger, corrected try/catch usage with DBUtil
 *		07/09/2008	ig91761		:	Changed DB logging to use connection pool
 *		06/20/2008	ig91761		:	Added database logging features
 *	 	06/02/2008	kt60981		:	Partial Assignment - update response to include trade contents
 *		05/13/2008	kt60981		:	Configure Alive Timer	
*/

public class CalypsoAdapter extends ScctServer {

	private static final int USER_SESSIONS=200;
	private static final String SUCCESS_MSG = "SUCCESS";
	private static final String ERROR_MSG = "ERROR";
	private static final String FAILURE_MSG = "FAILURE";
	private Logger logger;
	private Logger auditLogger;
	private Logger tradeEventsLogger;
	private Logger propsReloadLogger;
	private String calypsoUsername;
	private String calypsoPassword;
	private String calypsoEnv;
	private boolean isProcessing;
	private Object value = new Object();
	private List updatedValues = new LinkedList();
	private CalypsoDSConnection ds;
	private CalypsoPSConnection ps;
	private Hashtable userSessions;
	private CalypsoAdapterCache cache;
	private TradeEventHandler tradeEventHandler;
	private Destination login;
	private Destination query;
	private Destination tradeUpdate;
	private Destination tradeEvent;
	//private static final String dbConnectionsSql = "select count(*) from master..sysprocesses where dbid = 13";
	private static final TradeMonitorMBean tradeMBean = new TradeMonitor();
	private CalypsoAdapterAgent agent;
	private RemoteAccess remoteAccess;
	private String calypsoDBURL;
	private String calypsoDBUser;
	private String calypsoDBPassword;
	private boolean validateTrade;
	private static int clientTimeout = 60;
	
	protected void initialize() {
		try {
			agent = new CalypsoAdapterAgent(tradeMBean);
			userSessions = new Hashtable(USER_SESSIONS);
			logger = Logger.getLogger(this.getClass().getName());//log4j logger
			auditLogger = Logger.getLogger("AuditLogger");
			auditLogger.setAdditivity(false);
			tradeEventsLogger = Logger.getLogger("TradeEventsLogger");
			tradeEventsLogger.setAdditivity(false);
			propsReloadLogger = Logger.getLogger("PropsReloadLogger");
			propsReloadLogger.setAdditivity(false);
			setUpdateValues();
			Thread propertyReloader = 
				new Thread(new CalypsoAdapterLogPropertyReloader(properties, propsReloadLogger));
			propertyReloader.start();
			createCalypsoConnections();
			/* this must be done after calypso connection is established, but before worker threads start up
				because we get props from calypso and we can't start processing msgs till we have them */ 	
			if(!validateDBSettings()){
				logger.error("***Unable to properly read logging database info, exiting CalypsoAdapter program***");
				System.exit(1);
			}
			createCache();
			createSubscriptions();
			// login to Calyps
		//	testCache();
		//	testCDOTrade();
			postInitialize();
			remoteAccess = ds.getDSConnection().getRemoteAccess();
						
		} catch (Throwable ex) {
			logger.error("initialization failed " +ex.getMessage());
			logger.error(ScctUtil.getStackTrace(ex));
			printAndExit(1, "initialization failed " +ex );
		}
	}

	public void postInitialize() throws Exception {
		boolean reconcile = false;
		if (isServiceOn(CalypsoAdapterConstants.EVENT_SERVICE)) {
			reconcile = true;
		}

		tradeEventHandler = new TradeEventHandler(ds.getDSConnection(), reconcile, tradeMBean, properties);
		if (tradeEventHandler !=null) {
			if (isServiceOn(CalypsoAdapterConstants.EVENT_SERVICE)) {
				logger.debug("Starting EVENT Service for '" + getInstanceId()
						+ "'");
				tradeEventHandler.start();
				logger.debug("EVENT Service started for '" + getInstanceId()
						+ "'");
			} else {
				logger.debug("Skipping EVENT Service for '" + getInstanceId()
						+ "'");
			}
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					dispose();
				}
			});
			;
			// Refactor Timer ctor
			if (isServiceOn(CalypsoAdapterConstants.EVENT_SERVICE)) {
				Thread timer = new Thread(new CalypsoAdapterAliveTimer(ds
						.getDSConnection(), tradeEventHandler));
				timer.start();
			} else {
				logger.debug("Skipping CalypsoAdapterAliveTimer for '"
						+ getInstanceId() + "'......");
			}
			if (properties != null) {
				try {
					validateTrade = Boolean
							.valueOf(properties
									.getProperty(CalypsoAdapterConstants.VALIDATE_EVENT));
				} catch (RuntimeException e) {
					e.printStackTrace();
				}
			}
		} else {
			String msg = "Unable to initialize TradeEventHandler....Shutting down.....";
			logger.fatal(msg);
			printAndExit(-1, msg);
		}
	}
	
	private void setUpdateValues(){
		if(!isProcessing){
			if(updatedValues == null){
				updatedValues = new LinkedList();
			}
			updatedValues.clear();
			updatedValues.add(value);
		}
	}
	
	public void createCalypsoConnections() throws Exception {
		calypsoEnv = properties.getProperty("CalypsoEnv.env");
		calypsoUsername = System.getProperty("CalypsoEnv.username");
		calypsoPassword = System.getProperty("CalypsoEnv.password");
		if ( calypsoEnv == null || calypsoUsername == null || calypsoPassword == null){
			logger.error("CalypsoEnv.env/CalypsoEnv.username/CalypsoEnv.password setting missing");
			throw new Exception("CalypsoEnv.env/CalypsoEnv.username/CalypsoEnv.password setting missing");
		}

		try {
		//
		logger.debug("Logging to Calypso ....");
		// creating the main DataServer connection
		ds = new CalypsoDSConnection(calypsoEnv, calypsoUsername, calypsoPassword);
		ds.connect();
		logger.debug("Logged in to Calypso");
		
		// set the default connection
		DSConnection.setDefault(ds.getDSConnection());
		// print ds connection
		String appname = DSConnection.getDefault().getAppName();
		if ( appname != null )
			logger.debug("appname " + appname);
		else
			logger.debug("appname is null ");
		// create the main event server
		logger.debug("Create a ps server connection ");

		ps = new CalypsoPSConnection(ds);
		ps.start();

		   // Starts connection to DataServer.
	//  String passwd = com.citigroup.project.util.pwp.runGetPassword(calypsoLogin,env,null);
		} catch (Exception ex){
			logger.error("Exception " + ex);
			throw ex;
		}
	}

	public static CalypsoAdapter getAdapter() {
		return (CalypsoAdapter) server;
	}
	
	public static TradeMonitorMBean getTradeMbean() {
		return tradeMBean;
	}
	
	public CalypsoDSConnection getConnection(){
		return ds;
	}

	public boolean handleBrokerMessage(Message message) throws Exception {
		int tradeId = -1;
		String auditId;
		String messageDirection= "REQUEST";
		if (message != null && message.toString().length() > 0)
			auditId = message.getJMSMessageID();
		else
			auditId = "COULD NOT DETERMINE";
		if ( !(message instanceof TextMessage) ){
			logger.debug("Received Non-text message - ignored");
			auditLog(auditId, messageDirection, "UNKNOWN", message, 
					FAILURE_MSG, "Received a Non-text message of type "
					+ message.getClass().getSimpleName() + " - ignored",tradeId);
			return false;
		}
		boolean isComplete = false;
		MessageProducer currProducer = null;
		//logger.debug("Received Message " + message + " header=" + message.getStringProperty("loginTime"));
		long before=System.currentTimeMillis();
		boolean handled = false;
		Destination retValue = message.getJMSReplyTo();
		long u1 = System.currentTimeMillis();
		Unmarshaller unmarshaller = ScctUtil.createUnmarshaller();
		long u2 = System.currentTimeMillis();
		logger.debug("Creating Unmarshaller time = " + (u2-u1)/1000.0);
		u2 = System.currentTimeMillis();
		Marshaller marshaller = ScctUtil.createMarshaller();
		long u3 = System.currentTimeMillis();
		logger.debug("Creating Marshaller time = " + (u3-u2)/1000.0);

		JAXBElement response = null;
		int [] tradeArrays = null;
		StreamingTradeArray stradeArrays = null;
		
		try {
			String xml = ((TextMessage)message).getText();
			JAXBElement<Object>  eObject = null;
			
			if (xml != null && !"".equals(xml) && xml.length()>0) {
				// unmarshaller the message
				long x = System.currentTimeMillis();
				InputSource is = new InputSource(new StringReader(xml));
				eObject = (JAXBElement<Object>)unmarshaller.unmarshal(is);
				
				long y = System.currentTimeMillis();
				if (eObject.getValue() instanceof ScctTradeNewType ||
					eObject.getValue() instanceof ScctTradeUpdateType) {
					logger.debug("Unmarshalling " + eObject.getValue().getClass() + " time = " + (y-x)/1000.0);
				}
				auditLog(auditId, messageDirection, eObject.getValue().getClass().getSimpleName(), message, 
						SUCCESS_MSG, "Message received successfully",tradeId);
			} else {
				logger.debug("Received Invalid/Empty Message - ignored");
				auditLog(auditId, messageDirection, eObject.getValue().getClass().getSimpleName(), message,
						FAILURE_MSG, "Received Invalid/Empty Message - ignored",tradeId);
				return false;
			}
			
			currProducer = getProducer();
			logger.debug("object  " + eObject.getValue().getClass().getName());

			Object obj = eObject.getValue();

			String username = message.getStringProperty("username");
			String loginTime = message.getStringProperty("loginTime");
			String password = message.getStringProperty("password");
			StringBuffer tmp = new StringBuffer();

			if (username != null)
				tmp.append(username);

			if (loginTime != null)
				tmp.append("_").append(loginTime);

			String sessionId = tmp.toString();

			if ( !(obj instanceof ScctLoginRequestType)){

				AdapterUserSession usession = getUserSession(sessionId);
				if (usession == null ){
					if(username != null && password != null){
						AdapterUserSession session = new AdapterUserSession(username,password,validateTrade);
						session.login();
						usession = session;
						handleUserSession(session,sessionId);
					}
				}
				if(usession == null){
						throw new Exception("User not logged in " +username);
				}
			}

			/* Vector vConn = remoteAccess.executeSelectSQL(dbConnectionsSql);
			if ( vConn != null && vConn.size() > 0) {
				Vector result = (Vector)vConn.get(2);
				logger.debug("Number of database connections: " + (Integer)result.get(0) +
							" before request " + eObject.getValue().getClass().getSimpleName());
			} */
			//at this point the incoming request is done, we begin response generation
			messageDirection = "RESPONSE";
			
			if ( obj instanceof ScctLoginRequestType){
				Boolean isServer = message.getBooleanProperty("isServer");
				if ( false && !isServer.booleanValue() ){
					logger.debug("Login Message ignored - not from server");
					handled = false;
				} else {
				response = handleLoginRequest((ScctLoginRequestType)obj, loginTime, auditId);
				handled=true;
				}
			} else if ( obj instanceof ScctLogoutRequestType){
				Boolean isServer = message.getBooleanProperty("isServer");
				if ( false && !isServer.booleanValue() ){
					logger.debug("Logout Message ignored - not from server");
					handled = false;
				} else {
				response = handleLogoutRequest((ScctLogoutRequestType)obj, sessionId, auditId);
				handled=true;
				}
			} else if ( obj instanceof QueryScctBookType){
				response = handleBookQuery(username, (QueryScctBookType)obj, sessionId);
				handled=true;
			} else if ( obj instanceof QueryStaticDataType){
				response = handleQueryStaticData(username, (QueryStaticDataType)obj, sessionId);
				handled = true;
			} else if ( obj instanceof QueryStaticDataCustomType){
				response = handleQueryStaticDataCustom(username, (QueryStaticDataCustomType)obj, sessionId);
				handled = true;
			} else if ( obj instanceof QueryScctLegalEntityType){
				response = handleQueryLegalEntity( (QueryScctLegalEntityType)obj);
				handled=true;
			} else if ( obj instanceof QueryScctTradeType){
				String ver = (message!=null ? message.getStringProperty(CalypsoAdapterConstants.PUBLISHER_VERSION) : null);
				// support non-streaming case
				if (!Util.isEmptyString(ver) && StreamingPublisher.supportedVersion.equals(ver)) {
					tradeArrays = handleQueryScctTradeArray(username, (QueryScctTradeType) obj, sessionId,auditId);
					handled = false;
				} else {
					response = handleQueryScctTrade(username, (QueryScctTradeType) obj, sessionId,auditId);
					handled = true;
				}
			} else if ( obj instanceof QueryScctTradePredicateType){
				String ver = (message!=null ? message.getStringProperty(CalypsoAdapterConstants.PUBLISHER_VERSION) : null);
				if (!Util.isEmptyString(ver) && StreamingPublisher.supportedVersion.equals(ver)) {
					tradeArrays = handleQueryScctTradeArray(username, (QueryScctTradePredicateType) obj, sessionId,auditId);
					handled = false;
				} else {
					response = handleQueryScctTrade(username, (QueryScctTradePredicateType) obj, sessionId,auditId);
					handled = true;
				}
			} else if ( obj instanceof ScctTradeUpdateType ){
				response = handleScctTradeUpdate(username, (ScctTradeUpdateType)obj, sessionId,auditId);
				handled = true;
				JAXBElement<ScctTradeUpdateResponseType> elem = response;
				tradeId = elem.getValue().getTradeId();
			} else if ( obj instanceof ScctTradeNewType){
				response = handleScctTradeNew(username, (ScctTradeNewType)obj, false,sessionId, auditId);
				handled=true;
				JAXBElement<ScctTradeNewResponseType> elem = response;
				tradeId = elem.getValue().getTradeId();
			} else if ( obj instanceof ScctPortfolioLoadType){
				response = handleScctPortfolioLoad((ScctPortfolioLoadType)obj);
				handled=true;
			} else if ( obj instanceof ScctPortfolioNewType){
				response = handleScctPortfolioNew((ScctPortfolioNewType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctPortfolioUpdateType){
				response = handleScctPortfolioUpdate((ScctPortfolioUpdateType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctTradeTemplateUpdateType){
				response = handleScctTradeTemplateUpdate((ScctTradeTemplateUpdateType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctTradeTemplateLoadType){
				response = handleScctTradeTemplateLoad((ScctTradeTemplateLoadType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof QueryScctTradeHistoryType){
				response = handleQueryScctTradeHistory((QueryScctTradeHistoryType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctTradeGroupNewType){
				response = handleScctTradeGroupNew((ScctTradeGroupNewType)obj, username, sessionId, auditId);
				handled=true;
			} else if ( obj instanceof ScctFavoritesLoadType){
				response = handleScctFavoritesLoad((ScctFavoritesLoadType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctAmendActionToReasonLoadType){
				response = handleScctAmendActionToReasonLoad((ScctAmendActionToReasonLoadType)obj);
				handled=true;
			} else if ( obj instanceof ScctIndexTradeCaptureDataLoadType){
				response = handleScctIndexTradeCaptureDataLoad((ScctIndexTradeCaptureDataLoadType)obj);
				handled=true;
			} else if ( obj instanceof QueryTodaysTradesType ){
				response = this.handleQueryTodaysTrades(username, sessionId);
				handled=true;
			} else if ( obj instanceof QueryTodaysTradesByBookType ){
				response = this.handleQueryTodaysTradesByBook((QueryTodaysTradesByBookType) obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof QueryTodaysTradesByUserType ){
				response = this.handleQueryTodaysTradesByUser((QueryTodaysTradesByUserType) obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof QueryTradesByNumberOfDaysType ){
				response = this.handleQueryTradesByNumberOfDays((QueryTradesByNumberOfDaysType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof QueryScctTradeArraysType ){
				response = this.handleQueryTradeArrays((QueryScctTradeArraysType)obj, username, sessionId,auditId);
				handled=true;
			} else if ( obj instanceof ScctUserConfigLoadType ){
				response = this.handleScctUserConfigLoad((ScctUserConfigLoadType)obj, username);
				handled=true;
			} else if ( obj instanceof ScctUserConfigUpdateType ){
				response = this.handleScctUserConfigUpdate((ScctUserConfigUpdateType)obj, username);
				handled=true;
			} else if ( obj instanceof QueryRefObType ){
				response = this.handleQueryRefOb((QueryRefObType)obj, username, sessionId);
				handled=true;
			} else if ( obj instanceof ScctTradePAUpdateType) {
				response = this.handleScctTradePAUpdate((ScctTradePAUpdateType)obj, username, sessionId);
				handled = true;
			} else if ( obj instanceof QueryScctPartialAssignmentTradeType) {
				response = this.handleQueryScctPATrade(((QueryScctPartialAssignmentTradeType) obj), username, sessionId);
				handled = true;
			} else if ( obj instanceof QueryScctReconType) {
				tradeArrays = this.handleScctRecon((QueryScctReconType)obj, username, sessionId);
				handled = false;
			} else if ( obj instanceof QueryScctTradeGemfireType) {
				tradeArrays = this.handleScctGemfire((QueryScctTradeGemfireType) obj, username, sessionId,auditId);
				handled = false;
			} else if ( obj instanceof QueryScctBookStrategyType) {
				response = this.handleQueryBookStrategy((QueryScctBookStrategyType) obj, username, sessionId);
				handled = true;
			}

			
			Message responseMessage = null;
			if (handled && StreamingPublisher.accept(response, message)) {
				StreamingPublisher.getPublisher(response).publish(response, currProducer, session, retValue, region);
				responseMessage = createTextMessage(response);
				auditLog(auditId, messageDirection, eObject.getValue().getClass().getSimpleName(),responseMessage,
						SUCCESS_MSG, "StreamPublished response",tradeId);
			} else if (StreamingPublisher.accept(tradeArrays)) {
				AdapterUserSession userSession = getUserSession(sessionId);
				if (obj instanceof QueryScctReconType ) {
					ReconPublisher publisher = new ReconPublisher();
					publisher.publish(tradeArrays, userSession, currProducer, session, retValue, region, obj);
				} else if (obj instanceof QueryScctTradeGemfireType) {
					GemfirePublisher publisher = new GemfirePublisher(userSession, currProducer, session, retValue, region, new ObjectFactory());
					publisher.publish(tradeArrays, obj);
				} else {
					TradePublisher publisher = new TradePublisher();
					publisher.publish(tradeArrays, userSession, currProducer,
							session, retValue, region);
				} 
				auditLog(auditId, messageDirection, eObject.getValue().getClass().getSimpleName(),responseMessage, 
						SUCCESS_MSG, "StreamPublished response",tradeId);
				tradeArrays = null;
			} else {
					if ( handled && response != null ){
						responseMessage = createTextMessage(response);
						
						//session.createProducer(reply).send(responseMessage);
						long y = System.currentTimeMillis();
						publishMessage(retValue, responseMessage);
						long z = System.currentTimeMillis();
						logger.debug("Publishing response : " + eObject.getValue() + " time = " + (z-y)/1000.0);
						auditLog(auditId, messageDirection, responseMessage.getStringProperty("messagetype")
								, responseMessage, SUCCESS_MSG, "Successfully returned response",tradeId);
					} 
					else if (handled && response == null){
						publishMessage(retValue,createTextMessage(getValue(tradeId)));
						auditLog(auditId, messageDirection, "COULD NOT DETERMINE",
							responseMessage, FAILURE_MSG, "Response NULL, error returned from adapter",tradeId);
					}
					//!handled implies login/logout error
					else if (!handled && response != null) {
						publishMessage(retValue,createTextMessage(response));
						auditLog(auditId, messageDirection, responseMessage.getStringProperty("messagetype"),
							responseMessage, FAILURE_MSG, 
							"Error returned from adapter - Login/out Message ignored - not from server",tradeId);
					}
					else if ((!handled && tradeArrays==null)) {
						if (obj instanceof QueryScctReconType) {
							ReconPublisher publisher = new ReconPublisher();
							publisher.publishEmptyMessage(producer, session, retValue, region, "");
						} else if (obj instanceof QueryScctTradeGemfireType) {
							GemfirePublisher publisher = new GemfirePublisher();
							publisher.publishEmptyMessage(producer, session, retValue, region, "");
						} else {
						    TradePublisher publisher = new TradePublisher();
						    publisher.publishEmptyMessage(currProducer, session, retValue, region, "");
						}
						auditLog(auditId, messageDirection, eObject.getValue().getClass().getSimpleName(),
								responseMessage, SUCCESS_MSG, "StreamPublished response",tradeId);
					}
 					else{
 						publishMessage(retValue,createTextMessage(getValue(tradeId)));
						auditLog(auditId, messageDirection, "COULD NOT DETERMINE",responseMessage, FAILURE_MSG, 
							"Response could NOT be returned from adapter - Login/out Message ignored - not from server",tradeId);
 					}
			}
			cleanup(response);
			if (tradeArrays !=null) {
				tradeArrays = null;
			}
			long after=System.currentTimeMillis();
			logger.debug("Request " + eObject.getValue().getClass().getSimpleName() +
					" processed in " + ((double)(after-before))/1000.0
					+ " seconds.");
			isComplete = true;
		} catch (Throwable ex) {
			try{
				logger.error(ex.getMessage());
				performErrorProcessing(ex.getMessage());
				messageDirection = "RESPONSE";
				ex.printStackTrace();
				// send back an error message
				ObjectFactory factory = new ObjectFactory();
				ScctErrorType errType = factory.createScctErrorType();
				StringBuffer code = new StringBuffer("Unable to handle request : ");
				if (ex.getMessage() != null && ex.getMessage().length()>0 ) {
					code.append("'").append(ex.getMessage()).append("'");
				}
				errType.setCode(code.toString());
				//errType.setText(ScctUtil.getStackTrace(ex));
				logger.error("Error in handleBrokerMessage: " + ex.getMessage(), ex);
				JAXBElement<ScctErrorType> outer = factory.createScctError(errType);
				Message responseMessage = createTextMessage(outer);
				publishMessage(retValue, responseMessage);
				auditLog(auditId, messageDirection, responseMessage.getStringProperty("messagetype"), 
						responseMessage, ERROR_MSG, 
					"Exception occurred but response was returned: " + code.toString(),tradeId);
				isComplete = true;
			}
			catch(Exception e){
				logger.error(e.getMessage());
				performErrorProcessing(e.getMessage());
				publishMessage(retValue,createTextMessage(getValue(0)));
				isComplete = true;
			}
		}
		finally{
			performOperation(isComplete,retValue,currProducer,message);
		}
		return handled;
	}
	
	
	private JAXBElement getValue(int value){
		ObjectFactory factory = new ObjectFactory();
		ScctErrorType errType = factory.createScctErrorType();
		StringBuffer code = new StringBuffer("Unable to handle request : "+value);
		errType.setCode(code.toString());
		JAXBElement<ScctErrorType> outer = factory.createScctError(errType);
		return outer;
	}
	
	public String getUserName(Message msg ) throws Exception{
		return msg.getStringProperty("username");
	}

	// helper to hint JVM to GC these large size objects faster
	private void cleanup(JAXBElement response) {
		if (response != null) {
			Object object = response.getValue();

			if (object instanceof QueryScctTradeResponseType) {
				QueryScctTradeResponseType type = (QueryScctTradeResponseType) object;
				List<ScctTradeType> trades = type.getScctTrade();
				trades.clear();
				trades = null;
			} else if (object instanceof QueryScctTradeArraysResponseType) {
				QueryScctTradeArraysResponseType type = (QueryScctTradeArraysResponseType) object;
				List<ScctTradeType> trades = type.getScctTrade();
				trades.clear();
				trades = null;
			} else if (object instanceof ScctTradeTemplateLoadResponseType) {
				ScctTradeTemplateLoadResponseType type = (ScctTradeTemplateLoadResponseType) object;
				List<ScctTradeTemplateType> templates = type.getScctTradeTemplate();
				templates.clear();
				templates = null;
			} else if (object instanceof QueryStaticDataResponseType) {
				QueryStaticDataResponseType type = (QueryStaticDataResponseType) object;
				if (type.getQueryTickerResponse() !=null) {
					List<ScctTickerType> tickers = type.getQueryTickerResponse().getTicker();
					tickers.clear();
					tickers = null;
				}
 			}
		}
	}
	
	private void performErrorProcessing(String msg){
		if(msg != null){
			if(msg.equalsIgnoreCase(CalypsoAdapterConstants.ERR_MSG) ||
					msg.equalsIgnoreCase(CalypsoAdapterConstants.ERR_MSG_2)){
				if(!isProcessing){
					updateValues();
				}
			}
		}
	}

	public JAXBElement<QueryScctBookResponseType> handleBookQuery(String username, QueryScctBookType request, String sessionId) throws Exception{
		ObjectFactory factory = new ObjectFactory();
		QueryScctBookResponseType response = factory.createQueryScctBookResponseType();
		JAXBElement<QueryScctBookResponseType> outer = factory.createQueryScctBookResponse(response);
		AdapterUserSession usession = getUserSession(sessionId);
		
		int requestedId = (request.getId() == null) ? -1 : request.getId().intValue();
		String requestedName = request.getName();

		boolean idProvided = (requestedId != -1);
		boolean nameProvided = (requestedName != null);

		ScctBookType queryResult = null;
		if(nameProvided)
		{
			queryResult = usession.getUserProfile().getBook(requestedName);
			if(queryResult != null && idProvided)
			{
				if(queryResult.getId() != requestedId)
				{
					// result does not match on both name and ID criteria
					queryResult = null;
				}
			}
		}
		else if(idProvided)
		{
			queryResult = usession.getUserProfile().getBook(requestedId);
		}
		else
		{
			// Non-specific request: Return all books for this user
			response.getBook().addAll(usession.getBooks());
		}

		if(queryResult != null)
		{
			response.getBook().add(queryResult);
		}

		if ( usession == null ){
			throw new Exception("client not logged in ");
		}
		return outer;
	}


		public void createSubscriptions() throws Exception {
		logger.debug("Creating subscriptions");
		login = (Destination) jndiContext.lookup(getLogin());
		query = (Destination) jndiContext.lookup(getTradeQuery());
		tradeUpdate = (Destination)jndiContext.lookup(getTradeUpdate());
		tradeEvent = (Destination)jndiContext.lookup(getTradeEvent());
		List tradeDests = new LinkedList();
		tradeDests.add(query);
		tradeDests.add(tradeUpdate);
		String selector = new String("region = " + "'" + region + "'");
/*		TopicSubscriber subscriber = session.createDurableSubscriber(login, login+region, selector, false);
	//	TopicSubscriber subscriber = session.createDurableSubscriber(login, "TempdapterLogin");

		subscriber.setMessageListener(this);
		TopicSubscriber subscriber2 = session.createDurableSubscriber(query, query+region, selector, false);
		subscriber2.setMessageListener(this);

		TopicSubscriber subscriber3 = session.createDurableSubscriber(tradeUpdate, tradeUpdate+region, selector, false);
		subscriber3.setMessageListener(this);
//		TopicSubscriber subscriber4 = session.createDurableSubscriber(tradeEvent, "TempEvent");
//		subscriber4.setMessageListener(this);


//		consumer.setMessageListener(this);
//		MessageConsumer consumer2= session.createConsumer(query, "AdapterQueryConsumer");
//		consumer2.setMessageListener(this);
//		MessageConsumer consumer3= session.createConsumer(tradeUpdate, "AdapterUpdateConsumer");
//		consumer3.setMessageListener(this);
//		MessageConsumer consumer4= session.createConsumer(tradeEvent, "AdapterEventConsumer");
//		consumer4.setMessageListener(this);

		// session.createDurableSubscriber(login, selector,
		// "AdapterloginSub", true);
		//TopicSubscriber subscriber = session.createDurableSubscriber(
	//			login, StringUtilities.stripPackageName(getClass().getName()));
		//ses
*/
		if (isServiceOn(CalypsoAdapterConstants.TRADE_MGMT_SERVICE)) {
			MessageConsumer consumer = session.createConsumer(login, selector, false);
			consumer.setMessageListener(this);
			if(isFailover){
				createSubscribers(tradeDests,selector,THREAD_POOL_SIZE);
			}
			else{
				MessageConsumer consumer2 = session.createConsumer(query, selector, false);
				consumer2.setMessageListener(this);
				MessageConsumer consumer3 = session.createConsumer(tradeUpdate, selector, false);
				consumer3.setMessageListener(this);
			}
			logger.debug("Trade Management Service Started for '" + getInstanceId() + "'");
		} else {
			logger.debug("Skipping Trade Management Service for '" + getInstanceId() + "'");
		}
	}
		
	private void createSubscribers(List destValues,String selector,int number) throws JMSException{
		for(int i=0;i<number;i++){
			if(destValues != null){
				for(int j=0;j<destValues.size();j++){
					getSession().createConsumer((Destination)destValues.get(j),
					selector,false).setMessageListener(this);
				}
			}
		}
	}

	public String getCalypsoEnv(){
		return  calypsoEnv;
	}

	protected void createCache() throws Exception {
		cache = new CalypsoAdapterCache(this.ds);
		cache.initialize();
	}


	/**
	 * Request(s)
	 * @param loginTime TODO
	 *
	 */

	public JAXBElement<ScctLoginResponseType> handleLoginRequest(ScctLoginRequestType request, String loginTime, String auditId) {
		try {
			ObjectFactory factory = new ObjectFactory();
			ScctLoginResponseType response = factory.createScctLoginResponseType();
			JAXBElement<ScctLoginResponseType> outer = factory.createScctLoginResponse(response);
			String username = request.getUsername();
			String password = request.getPassword();
			String calypsoEnv = CalypsoAdapter.getAdapter().getCalypsoEnv();
			response.setUsername(username);
			response.setPassword(password);
			response.setSuccess(false);
			logger.debug("Login Request : " + username);

			if (username == null || password == null
					|| username.trim().length() == 0) {
				response.setMessage("Username/password is empty");
				Message temp = createTextMessage(outer);
				auditLog(auditId, "RESPONSE", "ScctLoginResponseType", temp, ERROR_MSG, 
						"Returning response that login username/password is empty",-1);
			} else {
				try {
				// re-use user session if it already exists ie. if the user is already logged in
				StringBuffer tmp = new StringBuffer(username);
				tmp.append("_").append(loginTime);
				AdapterUserSession session = getUserSession(tmp.toString());
				if (session == null) {
					session = new AdapterUserSession(username, password, validateTrade);
					session.login();
					handleUserSession(session,tmp.toString());
				}
				response.setSuccess(true);
				response.setMessage("Successfully logged in ");
				response.setClientTimeout(getClientTimeout());
				} catch (com.calypso.tk.util.ConnectException ex ){
					response.setSuccess(false);
					response.setMessage("Login failed: " + ex.getMessage());
					logger.error("Login failed - ConnectException: " + ex.getMessage());
					logger.error(ScctUtil.getStackTrace(ex));
					Message temp = createTextMessage(outer);
					auditLog(auditId, "RESPONSE", "ScctLoginResponseType", temp, ERROR_MSG, 
						"Returning response that login failed - ConnectException: " + ex.getMessage(),-1); 
				} catch (Exception ex ){
					response.setSuccess(false);
					response.setMessage("Login Failed: " + ex.getMessage());
					logger.error("Login Failed: " + ex.getMessage());
					logger.error(ScctUtil.getStackTrace(ex));
					Message temp = createTextMessage(outer);
					auditLog(auditId, "RESPONSE", "ScctLoginResponseType", temp, ERROR_MSG, 
						"Returning response that login failed: " + ex.getMessage(),-1);
				}
			}
			return outer;
		} catch (Exception ex) {
			logger.error(ex);
			System.out.println("Calypso Login Exception " + ex);
			return null; // fix me later.
		}
	}
	
	private synchronized void handleUserSession(AdapterUserSession uSession,String sessionId){
		ArrayList list = (ArrayList)userSessions.get(sessionId);
		if(list == null){
			list = new ArrayList();
		}
		list.add(uSession);
		userSessions.put(sessionId,list);
	}
	
	private void performOperation(boolean isRequired,Destination value,MessageProducer producer,Message message) throws JMSException{
		if(!isRequired && value != null){
			try{
				publishMessage(value,createTextMessage(getValue(0)));
			}
			catch(Exception e){
				logger.debug("For information purposes only");
			}
		}
		if(producer != null){
			producer.close();
		}
		if(message != null){
			message.acknowledge();
		}
	}
	
	public String getRequiredValue(String type){
		String reqValue = null;
		if(properties != null){
			reqValue = properties.getProperty(type);
		}
		return reqValue;
	}
	
	public int getValueFromInput(String inputValue, int iValue){
		int value = iValue;
		if(inputValue != null){
			try{
				value = Integer.parseInt(inputValue);
			}
			catch(Exception e){
			}
		}
		return value;
	}
	
	
	private AdapterUserSession getUserSession(String sessionId){
		AdapterUserSession uSession = null;
		List list = (List)userSessions.get(sessionId);
		if(list != null && list.size()>0){
			uSession = (AdapterUserSession)list.get(0);
		}
		return uSession;
	}
	
	private Map getOnlyAdapterUserSessions(){
		Map adapterUserSessions = new HashMap();
		Iterator sessionIdsGroup = userSessions.keySet().iterator();
		while(sessionIdsGroup.hasNext()){
			String sessionId = (String)sessionIdsGroup.next();
			List list = (List)userSessions.get(sessionId);
			if(list != null && list.size() > 0){
				adapterUserSessions.put(sessionId,list.get(0));
			}
		}
		return adapterUserSessions;
	}
	
	private int getClientTimeout() {
		String tmp = properties.getProperty(CalypsoAdapterConstants.CLIENT_TIMEOUT);
		try {
			clientTimeout = Integer.parseInt(tmp);
		} catch (NumberFormatException e) {
			logger.warn("Unable to parse '" + CalypsoAdapterConstants.CLIENT_TIMEOUT + "' val=" + tmp);
		}
		return clientTimeout; 
	}
	
	private void handleUserSessionLogout(String sessionId) throws Exception{
		List list = (List)userSessions.get(sessionId);
		if(list != null){
			for(int i=0;i<list.size();i++){
				AdapterUserSession uSession = (AdapterUserSession)list.get(i);
				if(uSession != null){
					uSession.logout();
				}
			}
			userSessions.remove(sessionId);
		}
	}

	public JAXBElement<ScctLogoutResponseType> handleLogoutRequest(ScctLogoutRequestType request, String sessionId, String auditId) {
		try {
			ObjectFactory factory = new ObjectFactory();
			ScctLogoutResponseType response = factory.createScctLogoutResponseType();
			JAXBElement<ScctLogoutResponseType> outer = factory.createScctLogoutResponse(response);
			String username = request.getUsername();
			String calypsoEnv = CalypsoAdapter.getAdapter().getCalypsoEnv();
			response.setUsername(username);
			response.setSuccess(false);

			if (username == null || username.trim().length() == 0) {
				response.setMessage("Logout username is empty");
				Message temp = createTextMessage(outer);
				auditLog(auditId, "RESPONSE", "ScctLogoutResponseType", temp, ERROR_MSG, 
						"Returning response that logout username/password is empty",-1);
			} else {
				try {
					handleUserSessionLogout(sessionId);
					response.setSuccess(true);
					response.setMessage("Successfully logged out user " + username);
					userSessions.remove(sessionId);
					logger.debug("Successfully logged out user " + username);
				} catch (Exception ex ){
					response.setSuccess(false);
					response.setMessage("Logout Failed: " + ex.getMessage());
					logger.error("Logout Failed: " + ex.getMessage() + ", " + ex);
					Message temp = createTextMessage(outer);
					auditLog(auditId, "RESPONSE", "ScctLogoutResponseType", temp, ERROR_MSG, 
						"Returning response that logout failed: " + ex.getMessage(),-1); 
				}
			}
			return outer;
		} catch (Exception ex) {
			logger.error(ex);
			System.out.println("Calypso Logout Exception " + ex);
			return null;
		}
	}

	public JAXBElement<QueryStaticDataResponseType> handleQueryStaticDataCustom(String username, QueryStaticDataCustomType request, String sessionId ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryStaticDataResponseType response = factory.createQueryStaticDataResponseType();
		JAXBElement<QueryStaticDataResponseType> outer = factory.createQueryStaticDataResponse(response);

		ValidStaticDataRequests vr = request.getStaticDataType();
		ValidOperators vo = request.getCriteriaOperator();

		List<ScctStaticDataQueryCriteriaType> criteria = request.getCriteria();

		Vector<String> errors = new Vector<String>();

		if(vr == null)
		{
			String errString = "Custom static data request does not explicitly request a type of static data";
			errors.add(errString);
			throw new Exception(errString);
		}
		if(!criteria.isEmpty() && vo == null)
		{
			// There is criteria, but we don't know how to process it.
			String errString = "Criteria provided on custom static data request, but operator is missing";
			errors.add(errString);
			throw new Exception(errString);
		}

		Vector<Sorter> sorts = new Vector<Sorter>();

		Collection unfilteredStaticData = new ArrayList();
		Map unfilteredStaticDataMap = new LinkedHashMap();

		List writeTarget = null;
		Converter converter = null;

		switch(vr) {
			case COUNTRY:
				errors.addAll(CountryData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getCountries();
				response.setQueryCountryResponse(new QueryCountryResponseType());
				writeTarget = response.getQueryCountryResponse().getCountry();
				converter = null;
			break;
			case CURRENCY:
				errors.addAll(CCYData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getCurrencies();
				response.setQueryCurrencyResponse(new QueryCurrencyResponseType());
				writeTarget = response.getQueryCurrencyResponse().getCurrency();
				converter = null;
			break;
			case DAYCOUNT:
				errors.addAll(DaycountData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getDayCounts();
				response.setQueryDayCountResponse(new QueryDayCountResponseType());
				writeTarget = response.getQueryDayCountResponse().getDayCount();
				converter = null;

			break;
			case DRAFT:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getDraftTypes();
				response.setQueryDraftTypeResponse(new QueryDraftTypeResponseType());
				writeTarget = response.getQueryDraftTypeResponse().getDraftType();
			break;
			case FEEDEFINITION:
				errors.addAll(FeeDefinitionData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getFeeDefintions();
				response.setQueryFeeDefinitionResponse(new QueryFeeDefinitionResponseType());
				writeTarget = response.getQueryFeeDefinitionResponse().getFeeDefinition();
				converter = ConverterFactory.getInstance().getFeeDefinitionConverter();
			break;
			case FORWARD:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getForwardTypes();
				response.setQueryForwardTypeResponse(new QueryForwardTypeResponseType());
				writeTarget = response.getQueryForwardTypeResponse().getForwardType();
			break;
			case HEDGEFUNDMNEMONIC:
				errors.addAll(HedgeFundMnemonicData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getHedgeFundMnemonics();
				response.setQueryHedgeFundMnemonicResponse(new QueryHedgeFundMnemonicResponseType());
				writeTarget = response.getQueryHedgeFundMnemonicResponse().getHedgeFundMnemonic();
				converter = null;
			break;
			case HOLIDAYCALENDAR:
				errors.addAll(HolidayCalendarData.parseArguments(criteria, sorts));
				unfilteredStaticDataMap = cache.getHolidayData();
				response.setQueryHolidayCalendarResponse(new QueryHolidayCalendarResponseType());
				writeTarget = response.getQueryHolidayCalendarResponse().getHoliday();
				converter = null;
			break;
			case INITIALMARGINPERCENT:
				errors.addAll(InitialMarginPercentData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getInitialMarginPercent();
				response.setQueryInitialMarginPercentResponse(new QueryInitialMarginPercentResponseType());
				writeTarget = response.getQueryInitialMarginPercentResponse().getInitialMarginPercent();
				converter = null;
			break;
			case INITIALMARGINROLE:
				errors.addAll(InitialMarginRoleData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getInitialMarginRole();
				response.setQueryInitialMarginRoleResponse(new QueryInitialMarginRoleResponseType());
				writeTarget = response.getQueryInitialMarginRoleResponse().getInitialMarginRole();
				converter = null;
			break;
			case PORTFOLIO:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getPortfolios();
				response.setQueryPortfolioResponse(new QueryPortfolioResponseType());
				writeTarget = response.getQueryPortfolioResponse().getPortfolio();
			break;
			case PREMIUMLEGFREQUENCY:
				errors.addAll(PremLegFrequencyData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getPremLegFrequency();
				response.setQueryPremiumLegFrequencyResponse(new QueryPremiumLegFrequencyResponseType());
				writeTarget = response.getQueryPremiumLegFrequencyResponse().getPremiumLegFrequency();
				converter = null;
			break;
			case PREMIUMSTUBRULE:
				errors.addAll(PremiumStubRuleData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getPremiumStubRule();
				response.setQueryPremiumStubRuleResponse(new QueryPremiumStubRuleResponseType());
				writeTarget = response.getQueryPremiumStubRuleResponse().getPremiumStubRule();
				converter = null;
			break;
			case RATINGAGENCY:
				// key: Agency(Str)  Value:  Agency's Rating Values as string obj
				//unfilteredStaticDataMap = cache.getRatingAgency();  //Implement a new converter for this one day.  It'll be slightly faster
				errors.addAll(RatingAgencyData.parseArguments(criteria, sorts));
				unfilteredStaticData = doQueryRatingAgency();
				response.setQueryRatingAgencyResponse(new QueryRatingAgencyResponseType());
				writeTarget = response.getQueryRatingAgencyResponse().getRatingAgency();
				converter = null;
			break;
			case ROLECONVENTION:
				errors.addAll(RoleConventionData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getRollConvetions();
				response.setQueryRoleConventionResponse(new QueryRoleConventionResponseType());
				writeTarget = response.getQueryRoleConventionResponse().getRoleConvention();
				converter = null;
			break;
			case RTTFREQUENCY:
				errors.addAll(RTTFrequencyData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getRTTFrequency();
				response.setQueryRTTFrequencyResponse(new QueryRTTFrequencyResponseType());
				writeTarget = response.getQueryRTTFrequencyResponse().getRTTFrequency();
				converter = null;
			break;
			case RTTRULE:
				errors.addAll(RTTRuleData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getRTTRule();
				response.setQueryRTTRuleResponse(new QueryRTTRuleResponseType());
				writeTarget = response.getQueryRTTRuleResponse().getRTTRule();
				converter = null;
			break;
			case SALESPERSON:
				errors.addAll(SalesPersonData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getSalesPersons();
				response.setQuerySalespersonResponse(new QuerySalespersonResponseType());
				writeTarget = response.getQuerySalespersonResponse().getSalesperson();
				converter = null;
			break;
			case SCCTBOOK:
				errors.addAll(ScctBookData.parseArguments(criteria, sorts));
				unfilteredStaticDataMap = cache.getBookNames();
				response.setQueryScctBookResponse(new QueryScctBookResponseType());
				writeTarget = response.getQueryScctBookResponse().getBook();
				converter = ConverterFactory.getInstance().getBookConverter();
			break;
			case SCCTLEGALENTITY:
				errors.addAll(ScctLegalEntityData.parseArguments(criteria, sorts));
				unfilteredStaticDataMap = cache.getLegalEntities();
				//unfilteredStaticData = cache.getLegalEntities().values();
				response.setQueryScctLegalEntityResponse(new QueryScctLegalEntityResponseType());
				writeTarget = response.getQueryScctLegalEntityResponse().getLegalEntity();
				converter = ConverterFactory.getInstance().getLegalEntityConverter();
			break;
			case SCCTSYNTHETICCDOCLASSNAME:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getCDOClassNames();
				response.setQueryScctSyntheticCDOClassNameResponse(new QueryScctSyntheticCDOClassNameResponseType());
				writeTarget = response.getQueryScctSyntheticCDOClassNameResponse().getCdoClassName();
			break;
			case SCCTSYNTHETICCDODEFAULTLEG:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getCDODefaultLegTypes();
				response.setQueryScctSyntheticCDODefaultLegResponse(new QueryScctSyntheticCDODefaultLegResponseType());
				writeTarget = response.getQueryScctSyntheticCDODefaultLegResponse().getDefaultLeg();
			break;
			case SCCTSYNTHETICCDOPREMIUMLEG:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getCDOPremiumLegTypes();
				response.setQueryScctSyntheticCDOPremiumLegResponse(new QueryScctSyntheticCDOPremiumLegResponseType());
				writeTarget = response.getQueryScctSyntheticCDOPremiumLegResponse().getPremiumLeg();
			break;
			case SKEW:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getSkewTypes();
				response.setQuerySkewTypeResponse(new QuerySkewTypeResponseType());
				writeTarget = response.getQuerySkewTypeResponse().getSkewType();
			break;
			case STRUCTURE:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getStructureTypes();
				response.setQueryStructureTypeResponse(new QueryStructureTypeResponseType());
				writeTarget = response.getQueryStructureTypeResponse().getStructureType();
			break;
			case STRUCTUREDESK:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getStructureDesks();
				response.setQueryStructureDeskResponse(new QueryStructureDeskResponseType());
				writeTarget = response.getQueryStructureDeskResponse().getStructureDesk();
			break;
			case STRUCTURER:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getStructurers();
				response.setQueryStructurerResponse(new QueryStructurerResponseType());
				writeTarget = response.getQueryStructurerResponse().getStructurer();
			break;
			case TEMPLATE:
				errors.addAll(TemplateData.parseArguments(criteria, sorts));
				Map tmp = new LinkedHashMap();
				tmp.putAll(cache.getCDSTradeTemplates());
				tmp.putAll(cache.getCDOTradeTemplates());
				tmp.putAll(cache.getCDSABSTradeTemplates());
				tmp.putAll(cache.getCDSABXTradeTemplates());
				unfilteredStaticDataMap = tmp;
				response.setScctTradeTemplateLoadResponse(new ScctTradeTemplateLoadResponseType());
				writeTarget = response.getScctTradeTemplateLoadResponse().getScctTradeTemplate();
				converter = ConverterFactory.getInstance().getTradeTemplateConverter();
			break;
			case TERMINATION:
				// Support disabled.
				//errors.addAll(TerminationData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getTerminationTypes();
				response.setQueryTerminationTypeResponse(new QueryTerminationTypeResponseType());
				writeTarget = response.getQueryTerminationTypeResponse().getTerminationType();
				converter = null;
			break;
			case TERMINATIONREASON:
				// Support disabled.
				//errors.addAll(TerminationReasonData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getTerminationReason();
				response.setQueryTerminationReasonResponse(new QueryTerminationReasonResponseType());
				writeTarget = response.getQueryTerminationReasonResponse().getTerminationReason();
				converter = null;
			break;
			case TICKER:
				errors.addAll(TickerData.parseArguments(criteria, sorts));
				unfilteredStaticDataMap = cache.getTickerNames();
				response.setQueryTickerResponse(new QueryTickerResponseType());
				writeTarget = response.getQueryTickerResponse().getTicker();
				converter = ConverterFactory.getInstance().getTickerConverter();
			break;
			case ABXDEF:
				unfilteredStaticData = cache.getABXDefs();
				response.setQueryABXDefResponse(new QueryABXDefResponseType());
				writeTarget = response.getQueryABXDefResponse().getAbxDef();
				converter=ConverterFactory.getInstance().getABXDefConverter();
			break;
			case TRADEGROUP:
				//no action.  Not supported quite yet
				unfilteredStaticData = cache.getTradeGroups();
				response.setQueryTradeGroupResponse(new QueryTradeGroupResponseType());
				writeTarget = response.getQueryTradeGroupResponse().getScctTradeGroup();
			break;
			case TRADER:
				errors.addAll(TraderData.parseArguments(criteria, sorts));
				unfilteredStaticData = cache.getTraders();
				response.setQueryTraderResponse(new QueryTraderResponseType());
				writeTarget = response.getQueryTraderResponse().getTrader();
				converter = null;
			break;
			case USERDEFAULT:
				errors.addAll(UserDefaultData.parseArguments(criteria, sorts));
				unfilteredStaticDataMap = cache.getUserDefaults();
				response.setQueryUserDefaultResponse(new QueryUserDefaultResponseType());
				writeTarget = response.getQueryUserDefaultResponse().getUserDefault();
				converter = ConverterFactory.getInstance().getUserDefaultConverter();
			break;
			default:
				unfilteredStaticData.clear();
				unfilteredStaticDataMap.clear();
			break;
		}

		Map sourceMap = new LinkedHashMap(unfilteredStaticDataMap);
		Collection sourceCollection = new ArrayList(unfilteredStaticData);
		Map destMap = new LinkedHashMap();
		Collection destCollection = new ArrayList();

		for(Sorter sort : sorts)
		{
			sort.preprocess(sourceCollection,
							sourceMap,
							destCollection,
							destMap);

			if(!destMap.isEmpty())
			{
				sourceMap.clear();
				sourceMap = destMap;
			}

			if(!destCollection.isEmpty())
			{
				sourceCollection.clear();
				sourceCollection = destCollection;
			}

			String fieldName = sort.getFieldNameCriteria().toString();
			String fieldValue = sort.getFieldValueCriteria();

			if(fieldValue == null)
			{
				errors.add("Invalid value provided for " + fieldName + " field");
				continue;
			}

			Collection filterResults = new Vector();
			switch(vo)
			{
			case REGEXP:
				Pattern p = null;
				boolean isPatternValid = false;
				try 
				{
					p = Pattern.compile(fieldValue);
					isPatternValid = true;
				}
				catch (PatternSyntaxException pse)
				{
					errors.add("Not a valid regular expression: " + fieldValue);
					isPatternValid = false;
				}
				
				if(isPatternValid)
				{
					if(!sourceMap.isEmpty())
					{
						for(Object o : sourceMap.keySet())
						{
							Matcher m = p.matcher(o.toString());
							if(m.matches())
							{
								filterResults.add(sourceMap.get(o));
							}
						}
					}
					else if(!sourceCollection.isEmpty())					
					{
						for(Object o : sourceCollection) 
						{
							Matcher m = p.matcher(o.toString());
							if(m.matches())
							{
								filterResults.add(o);
							}
						}
					}
					System.out.println("Done this");
				}
				break;
				
			case EXACTMATCH:
			default:
				if(!sourceMap.isEmpty())
				{
					Object o = sourceMap.get(fieldValue);
					if(o != null)
					{
						filterResults.add(o);
					}
					errors.add("Filtering Complete for " + fieldName + "=" + fieldValue
							+ " Old size=[" + sourceMap.size() +"]."
							+ " New Size=[" + filterResults.size() + "]");
				}
				else if(!sourceCollection.isEmpty())
				{
					for(Object o : sourceCollection) {
						if(fieldValue.equals(o.toString()))
						{
							filterResults.add(o);
							break; //don't iterate through whole list.  Assume only one match.
						}
					}
					errors.add("Filtering Complete for " + fieldName + "=" + fieldValue
							+ " Old size=[" + sourceCollection.size() +"]."
							+ " New Size=[" + filterResults.size() + "]");
				}
				break;
			}

			destMap.clear();
			sourceMap.clear();
			destCollection.clear();
			sourceCollection.clear();
			sourceCollection = filterResults;

		}
		//After all sorts are done, the final filtered data set
		// should be in sourceCollection or sourceMap
		if(!sourceCollection.isEmpty())
		{
			writeFilterResults(writeTarget, converter, sourceCollection);

		}
		else if(!sourceMap.isEmpty())
		{
			writeFilterResults(writeTarget, converter, sourceMap.values());
		}
		return outer;
	}

	private void writeFilterResults(List writeTarget, Converter converter, Collection sourceCollection) throws ConverterException {
		if(converter == null)
		{
			writeTarget.addAll(sourceCollection);
		}
		else for(Object unconverted : sourceCollection)
		{
			Object converted = converter.convertFromCalypso(unconverted);
			writeTarget.add(converted);
		}
	}

	public JAXBElement<QueryStaticDataResponseType> handleQueryStaticData(String username, QueryStaticDataType request, String sessionId ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryStaticDataResponseType response = factory.createQueryStaticDataResponseType();
		JAXBElement<QueryStaticDataResponseType> outer = factory.createQueryStaticDataResponse(response);

		if ( request.getQueryCalendar() != null ){
			QueryCalendarResponseType calendarResponse = factory.createQueryCalendarResponseType();
			calendarResponse.getCalendar().addAll(cache.getHolidayCalendars());
			response.setQueryCalendarResponse(calendarResponse);
		}

		if ( request.getQueryHolidayCalendar() != null ){
			QueryHolidayCalendarResponseType calendarResponse = factory.createQueryHolidayCalendarResponseType();
			doQueryUserCalendar(username, calendarResponse);
			response.setQueryHolidayCalendarResponse(calendarResponse);
		}

		if ( request.getQueryCountry() != null ){
			QueryCountryResponseType countryResponse = factory.createQueryCountryResponseType();
			countryResponse.getCountry().addAll(cache.getCountries());
			response.setQueryCountryResponse(countryResponse);
		}

		if ( request.getQueryCurrency() != null ){
			QueryCurrencyResponseType ccy = factory.createQueryCurrencyResponseType();
			ccy.getCurrency().addAll(cache.getCurrencies());
			response.setQueryCurrencyResponse(ccy);
		}

		if ( request.getQueryDayCount() != null ){
			QueryDayCountResponseType dayCount = factory.createQueryDayCountResponseType();
			dayCount.getDayCount().addAll(cache.getDayCounts());
			response.setQueryDayCountResponse(dayCount);
		}

		if ( request.getQueryFeeDefinition() != null ){
			// TODO
			QueryFeeDefinitionResponseType defs = factory.createQueryFeeDefinitionResponseType();
			defs.getFeeDefinition().addAll(cache.getScctFeeDefinitions());
			response.setQueryFeeDefinitionResponse(defs);
		}

		if ( request.getQueryRoleConvention() != null ){
			QueryRoleConventionResponseType convention = factory.createQueryRoleConventionResponseType();
			convention.getRoleConvention().addAll(cache.getRollConvetions());
			response.setQueryRoleConventionResponse(convention);
		}

		if ( request.getQuerySalesperson() != null ){

			QuerySalespersonResponseType sales = factory.createQuerySalespersonResponseType();
			sales.getSalesperson().addAll(cache.getSalesPersons());
			response.setQuerySalespersonResponse(sales);
		}

		if ( request.getQueryScctBook() != null ){
			QueryScctBookResponseType books = factory.createQueryScctBookResponseType();
			AdapterUserSession usession = getUserSession(sessionId);
			books.getBook().addAll(usession.getBooks());
			response.setQueryScctBookResponse(books);
		}

		if ( request.getQuerySkewType() != null ){
			QuerySkewTypeResponseType skew = factory.createQuerySkewTypeResponseType();
			skew.getSkewType().addAll(cache.getSkewTypes());
			response.setQuerySkewTypeResponse(skew);
		}

		if ( request.getQueryStructureDesk() != null ){
			QueryStructureDeskResponseType struct = factory.createQueryStructureDeskResponseType();
			struct.getStructureDesk().addAll(cache.getStructureDesks());
			response.setQueryStructureDeskResponse(struct);
		}

		if ( request.getQueryStructurer() != null ){
			QueryStructurerResponseType struct = factory.createQueryStructurerResponseType();
			struct.getStructurer().addAll(cache.getStructurers());
			response.setQueryStructurerResponse(struct);
		}

		if ( request.getQueryStructureType() != null ){
			QueryStructureTypeResponseType struct = factory.createQueryStructureTypeResponseType();
			struct.getStructureType().addAll(cache.getStructureTypes());
			response.setQueryStructureTypeResponse(struct);
		}

		if ( request.getQueryTradeGroup() != null ){
			QueryTradeGroupResponseType group = factory.createQueryTradeGroupResponseType();
			group.getScctTradeGroup().addAll(cache.getTradeGroups());
			response.setQueryTradeGroupResponse(group);
		}

		if ( request.getQueryTrader() != null ){
			QueryTraderResponseType traders = factory.createQueryTraderResponseType();
			traders.getTrader().addAll(cache.getTraders());
			response.setQueryTraderResponse(traders);
		}

		if ( request.getQueryFeeDefinition() != null ){
			QueryFeeDefinitionResponseType feeDefs = factory.createQueryFeeDefinitionResponseType();
			cache.getFeeDefinition("UPFRONT");
		}

		if ( request.getQueryScctSyntheticCDODefaultLeg() != null ){
			QueryScctSyntheticCDODefaultLegResponseType type = factory.createQueryScctSyntheticCDODefaultLegResponseType();
			type.getDefaultLeg().addAll(cache.getCDODefaultLegTypes());
			response.setQueryScctSyntheticCDODefaultLegResponse(type);
		}

		if ( request.getQueryScctSyntheticCDOPremiumLeg() != null ){
			QueryScctSyntheticCDOPremiumLegResponseType type = factory.createQueryScctSyntheticCDOPremiumLegResponseType();
			type.getPremiumLeg().addAll(cache.getCDOPremiumLegTypes());
			response.setQueryScctSyntheticCDOPremiumLegResponse(type);
		}

		if ( request.getQueryDraftType() != null ){
			QueryDraftTypeResponseType draft = factory.createQueryDraftTypeResponseType();
			draft.getDraftType().addAll(cache.getDraftTypes());
			response.setQueryDraftTypeResponse(draft);
		}

		if ( request.getQueryTerminationType() != null ){
			QueryTerminationTypeResponseType termination = factory.createQueryTerminationTypeResponseType();
			termination.getTerminationType().addAll(cache.getTerminationTypes());
			response.setQueryTerminationTypeResponse(termination);
		}

		if ( request.getQueryPortfolio() != null ){
			QueryPortfolioResponseType portfolioResponse = factory.createQueryPortfolioResponseType();
			portfolioResponse.getPortfolio().addAll(cache.getPortfolios());
			response.setQueryPortfolioResponse(portfolioResponse);
		}

		if ( request.getQueryTicker() != null ){
			QueryTickerResponseType tickerResponse = factory.createQueryTickerResponseType();
			tickerResponse.getTicker().addAll(cache.getTickers());
			response.setQueryTickerResponse(tickerResponse);
		}
		
		if(request.getQueryABXDef() != null){
			QueryABXDefResponseType abxResponse = factory.createQueryABXDefResponseType();
			abxResponse.getAbxDef().addAll(cache.getABXDefs());
			response.setQueryABXDefResponse(abxResponse);
		}

		if ( request.getQueryInitialMarginPercent() != null ){
			QueryInitialMarginPercentResponseType initialMarginPercentResponse = factory.createQueryInitialMarginPercentResponseType();
			initialMarginPercentResponse.getInitialMarginPercent().addAll(cache.getInitialMarginPercent());
			response.setQueryInitialMarginPercentResponse(initialMarginPercentResponse);
		}

		if ( request.getQueryInitialMarginRole() != null ){
			QueryInitialMarginRoleResponseType initialMarginRoleResponse = factory.createQueryInitialMarginRoleResponseType();
			initialMarginRoleResponse.getInitialMarginRole().addAll(cache.getInitialMarginRole());
			response.setQueryInitialMarginRoleResponse(initialMarginRoleResponse);
		}

		if ( request.getQueryRTTRule() != null ){
			QueryRTTRuleResponseType rttRuleResponse = factory.createQueryRTTRuleResponseType();
			rttRuleResponse.getRTTRule().addAll(cache.getRTTRule());
			response.setQueryRTTRuleResponse(rttRuleResponse);
		}

		if ( request.getQueryRTTFrequency() != null ){
			QueryRTTFrequencyResponseType rttFrequencyResponse = factory.createQueryRTTFrequencyResponseType();
			rttFrequencyResponse.getRTTFrequency().addAll(cache.getRTTFrequency());
			response.setQueryRTTFrequencyResponse(rttFrequencyResponse);
		}

		if ( request.getQueryScctLegalEntity() != null ){
			QueryScctLegalEntityResponseType legalEntityResponse = doQueryLegalEntity(request.getQueryScctLegalEntity());
			response.setQueryScctLegalEntityResponse(legalEntityResponse);
		}

		if ( request.getQueryPremiumLegFrequency() != null ){
			QueryPremiumLegFrequencyResponseType premLegFreqResponse = factory.createQueryPremiumLegFrequencyResponseType();
			premLegFreqResponse.getPremiumLegFrequency().addAll(cache.getPremLegFrequency());
			response.setQueryPremiumLegFrequencyResponse(premLegFreqResponse);
		}

		if ( request.getQueryPremiumStubRule() != null ){
			QueryPremiumStubRuleResponseType premStubRuleResponse = factory.createQueryPremiumStubRuleResponseType();
			premStubRuleResponse.getPremiumStubRule().addAll(cache.getPremiumStubRule());
			response.setQueryPremiumStubRuleResponse(premStubRuleResponse);
		}

		if ( request.getQueryScctSyntheticCDOClassName() != null ){
			QueryScctSyntheticCDOClassNameResponseType type = factory.createQueryScctSyntheticCDOClassNameResponseType();
			type.getCdoClassName().addAll(cache.getCDOClassNames());
			response.setQueryScctSyntheticCDOClassNameResponse(type);
		}

		if ( request.getQueryForwardType() != null ){
			QueryForwardTypeResponseType forwardTypeResponse = factory.createQueryForwardTypeResponseType();
			forwardTypeResponse.getForwardType().addAll(cache.getForwardTypes());
			response.setQueryForwardTypeResponse(forwardTypeResponse);
		}

		if ( request.getQueryTerminationReason() != null ){
			QueryTerminationReasonResponseType terminationReasonResponse = factory.createQueryTerminationReasonResponseType();
			terminationReasonResponse.getTerminationReason().addAll(cache.getTerminationReason());
			response.setQueryTerminationReasonResponse(terminationReasonResponse);
		}

		if ( request.getQueryHedgeFundMnemonic() != null ) {
			QueryHedgeFundMnemonicResponseType hedgeFundMnemonicResponse = factory.createQueryHedgeFundMnemonicResponseType();
			hedgeFundMnemonicResponse.getHedgeFundMnemonic().addAll(cache.getHedgeFundMnemonics());
			response.setQueryHedgeFundMnemonicResponse(hedgeFundMnemonicResponse);
		}

		if ( request.getQueryUserDefault() != null ) {
			QueryUserDefaultResponseType userDefaultResponse = factory.createQueryUserDefaultResponseType();
			userDefaultResponse.getUserDefault().addAll(doQueryUserDefault(username));
			response.setQueryUserDefaultResponse(userDefaultResponse);
		}

		if ( request.getQueryRatingAgency() != null ) {
			QueryRatingAgencyResponseType ratingAgencyResponse = factory.createQueryRatingAgencyResponseType();
			ratingAgencyResponse.getRatingAgency().addAll(doQueryRatingAgency());
			response.setQueryRatingAgencyResponse(ratingAgencyResponse);
		}

		return outer;
	}

	public JAXBElement<QueryScctLegalEntityResponseType> handleQueryLegalEntity(QueryScctLegalEntityType query ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctLegalEntityResponseType response = doQueryLegalEntity(query);
		JAXBElement<QueryScctLegalEntityResponseType> outer = factory.createQueryScctLegalEntityResponse(response);
		return outer;
	}

	public QueryScctLegalEntityResponseType doQueryLegalEntity(QueryScctLegalEntityType query ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctLegalEntityResponseType response = factory.createQueryScctLegalEntityResponseType();

		String role = query.getRole();
		FilterModeType mode = query.getFilterMode();
		if ( mode != null && mode.equals(FilterModeType.CODE_LIKE)){
			String codeLike = query.getCodeLike();
			List list = cache.getLegalEntityByCodeLike(codeLike, role);
			if (list != null && list.size() > 0) {
				Iterator iter = list.iterator();
				while (iter.hasNext()) {
					LegalEntity le = (LegalEntity)iter.next();
					response.getLegalEntity().add(ConverterFactory.getInstance().getLegalEntityConverter().convertFromCalypso(le));
				}
			}

		} else if (mode != null && mode.equals(FilterModeType.ID)) {
			int id = query.getId();
			LegalEntity le = cache.getLegalEntity(id);
			if ( le != null ){
				response.getLegalEntity().add(ConverterFactory.getInstance().getLegalEntityConverter().convertFromCalypso(le));
			}
		} else {
			// get all legalentities
			List list = cache.getLegalEntities(role);
			Iterator iter = list.iterator();
			while (iter.hasNext()) {
				LegalEntity le = (LegalEntity) iter.next();
					ScctLegalEntityType sle = ConverterFactory.getInstance()
							.getLegalEntityConverter().convertFromCalypso(le);
					response.getLegalEntity().add(sle);
			}
			list.clear();
		}

		return response;
	}

	public JAXBElement<QueryScctTradeResponseType>  handleQueryScctTrade(String username, Object query, String sessionId,String auditId) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctTradeResponseType response = factory.createQueryScctTradeResponseType();
		JAXBElement<QueryScctTradeResponseType> outer = factory.createQueryScctTradeResponse(response);

		AdapterUserSession usersession = getUserSession(sessionId);
		StringBuffer status = new StringBuffer();
		Vector trades = usersession.queryTrade(query, status,auditId);
		if (trades != null && trades.size() > 0) {
			Iterator iter = trades.iterator();
			while (iter.hasNext()) {
				ScctTradeType t = (ScctTradeType) iter.next();
				response.getScctTrade().add(t);
			}
		}
		return outer;
	}
	

	public JAXBElement<QueryScctPartialAssignmentTradeResponseType>  handleQueryScctPATrade(QueryScctPartialAssignmentTradeType query, String username, String sessionId ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctPartialAssignmentTradeResponseType response = factory.createQueryScctPartialAssignmentTradeResponseType();
		JAXBElement<QueryScctPartialAssignmentTradeResponseType> outer = factory.createQueryScctPartialAssignmentTradeResponse(response);

		AdapterUserSession usersession = getUserSession(sessionId);
		StringBuffer status = new StringBuffer();
		if (query!=null) {
			Vector<PATradeResponseType> nodes = usersession.queryPATrade2(
					query, status, factory);
			if (!Util.isEmptyVector(nodes)) {
				for (PATradeResponseType p : nodes) {
					response.getResults().add(p);
				}
			}
		}		
		return outer;
	}

	public int [] handleQueryScctTradeArray(String username, Object query, String sessionId,String auditId) throws Exception {
		AdapterUserSession usersession = getUserSession(sessionId);
		return usersession.getTradeIds(query,auditId);
	}
	
	public StreamingTradeArray handleQueryScctTradeStreamingArray(String username, QueryScctTradeType query, String sessionId) throws Exception {
		AdapterUserSession usersession = getUserSession(sessionId);
		return usersession.getStreamingTradeArray(query);
	}

	public int[] handleScctRecon(QueryScctReconType query, String username, String sessionId) throws Exception {
		AdapterUserSession usersession = getUserSession(sessionId);
		int [] result = null;
		if (query!=null) {
			result = usersession.getTradesMetadata(query);			
		} 
		return result;
	}
	
	public int[] handleScctGemfire(QueryScctTradeGemfireType query, String username, String sessionId,String auditId) throws Exception {
		QueryScctTradePredicateType predicate = query.getTradePred();
		AdapterUserSession usersession = getUserSession(sessionId);
		int [] tradeIds = usersession.getGemfireTrades(query,auditId);
		return tradeIds;
	}
	
	public JAXBElement<QueryScctBookStrategyResponseType> handleQueryBookStrategy(QueryScctBookStrategyType query, String username, String sessionId) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctBookStrategyResponseType response = factory.createQueryScctBookStrategyResponseType();
		JAXBElement<QueryScctBookStrategyResponseType> outer = factory.createQueryScctBookStrategyResponse(response);
		AdapterUserSession usersession = getUserSession(sessionId);
		List<String> strategys = usersession.loadBookStrategy();
		for (String s : strategys) {
			response.getBookStrategy().add(s);
		}
		return outer;
	}
	
	
	
	/**
	 public JAXBElement<QueryScctMinimalTradeResponseType>  handleQueryScctMinimalTrade(String username, QueryScctTradeType query, String sessionId ) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctMinimalTradeResponseType response = factory.createQueryScctMinimalTradeResponseType();
		JAXBElement<QueryScctMinimalTradeResponseType> outer = factory.createQueryScctMinimalTradeResponse(response);

		AdapterUserSession usersession = getUserSession(sessionId);
		StringBuffer status = new StringBuffer();
		Vector trades = usersession.queryTrade(query, status);
		ScctMinimalTradeType trade;
		if (trades != null && trades.size() > 0) {
			Iterator iter = trades.iterator();
			while (iter.hasNext()) {
				ScctTradeType t = (ScctTradeType) iter.next();
				trade = factory.createScctMinimalTradeType();
				trade.setTradeId(t.getId());
				trade.setBookName(t.getBookName());
				trade.setCounterPartyMnemonic(t.getCounterParty().getCode());
				trade.setOasysDealId(t.getOasysTradeId());
				trade.setOpsReference(t.getOpsReference());
				trade.setTradeDate(t.getTradeDate());
				trade.setTradeState(t.getTradeState());
				trade.setUpdatedTime(Long.valueOf(t.getUpdatedTime()));

				// trade.setTickerName(t.getProduct().getType());

				response.getScctMinimalTrade().add(trade);
			}
		}
		return outer;
	} */
	public CalypsoAdapterCache getCache(){
		return cache;
	}

	public void testCache() throws Exception{
	//	logger.debug("Books size " + cache.getBooks().size());
		List list = cache.getCountries();
		logger.debug("Countries " + list);
		logger.debug("countries type " + list.get(0).getClass().getName());
		list = cache.getCurrencies();
		logger.debug("currencies " + list);
		logger.debug("currencies type " + list.get(0).getClass().getName());

		list = cache.getTraders();
		logger.debug("traders " + list);
		logger.debug("traders type " + list.get(0).getClass().getName());


		list = cache.getSalesPersons();
		logger.debug("sales " + list);
		logger.debug("sales type " + list.get(0).getClass().getName());

		list = cache.getRollConvetions();
		logger.debug("conventions " + list);
		logger.debug("conventions type " + list.get(0).getClass().getName());

		list = cache.getDayCounts();
		logger.debug("day counts " + list);
		logger.debug("day counts type " + list.get(0).getClass().getName());

		/*list = cache.getHolidayCalendars();
		logger.debug("calendars " + list);
		logger.debug("calendars type " + list.get(0).getClass().getName());
		*/

		list = cache.getSkewTypes();
		logger.debug("skew types " + list);
		logger.debug("skew types type " + list.get(0).getClass().getName());

		list = cache.getStructureDesks();
		logger.debug("structure desks " + list);
		logger.debug("structure desks " + list.get(0).getClass().getName());

		list = cache.getStructurers();
		logger.debug("structures  " + list);
		logger.debug("structures type " + list.get(0).getClass().getName());

		list = cache.getStructureTypes();
		logger.debug("structures types " + list);
		logger.debug("structures types type " + list.get(0).getClass().getName());

	}

	public JAXBElement<ScctTradeUpdateResponseType> handleScctTradeUpdate(
			String username, ScctTradeUpdateType update, String sessionId,String auditId)
			throws Exception {
		ObjectFactory factory = new ObjectFactory();

		ScctTradeUpdateResponseType response = factory.createScctTradeUpdateResponseType();
		JAXBElement<ScctTradeUpdateResponseType> outer = factory.createScctTradeUpdateResponse(response);

		// get the first trade
		ScctTradeType trade = update.getScctTrade();
		AdapterUserSession usession = getUserSession(sessionId);
		int tradeId = usession.updateTrade(trade,auditId);
		response.setMessage("Successful");
		response.setTradeId(tradeId);
		response.setSuccess(true);
		//tradeMBean.incUpdate();
		return outer;
	}

	public JAXBElement<ScctTradePAUpdateResponseType> handleScctTradePAUpdate(
			ScctTradePAUpdateType update, String username, String sessionId)
			throws Exception {
		ObjectFactory factory = new ObjectFactory();

		ScctTradePAUpdateResponseType response = factory.createScctTradePAUpdateResponseType();
		JAXBElement<ScctTradePAUpdateResponseType> outer = factory.createScctTradePAUpdateResponse(response);

		ScctTradeType trade = update.getScctTrade();
		AdapterUserSession usession = getUserSession(sessionId);
		Vector<String>status = usession.partialAssignTrade(trade);
		if (status!=null && status.size()>0) {
			response.getMessage().addAll(status);
			List<Integer>childTrades = new ArrayList();
			for (String s: status) {
				if (!Util.isEmptyString(s) && s.indexOf("Trade(s) saved.")>=0) {
					// Trade(s) saved. 933079/933070
					// 
					Pattern p = Pattern.compile("[0-9]+");
					Matcher m = p.matcher(s);
					if (m.find()) {
						int i = m.start();
						String [] a = (s.substring(i)!=null ) ? s.substring(i).split("/") : null;
						if (a!=null && !Util.isEmptyString(a[0])) {
						  try {
							childTrades.add(Integer.parseInt(a[0]));
						} catch (RuntimeException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						}
					}
				}
			}
			ScctTradeType strade = convertTradeResponse(trade.getId());
			if (strade!=null) {
				response.setParentTrade(strade);
			}
			for (Integer tradeId : childTrades) {
				strade = convertTradeResponse(tradeId);
				if (strade!=null) {
					response.getChildTrades().add(strade);
				}
			}
			
		}

		return outer;
	}

	public JAXBElement<ScctTradeNewResponseType> handleScctTradeNew(
			String username, ScctTradeNewType newTrade, boolean isReevaluation,String sessionId, String auditId)
			throws Exception {
		ObjectFactory factory = new ObjectFactory();
		ScctTradeNewResponseType response = factory.createScctTradeNewResponseType();
		JAXBElement<ScctTradeNewResponseType> outer = factory.createScctTradeNewResponse(response);
		AdapterUserSession usession = getUserSession(sessionId);
		int tradeId= usession.newTrade(newTrade.getScctTrade(),auditId);
		if(tradeId > 0){
			ScctTradeType strade = convertTradeResponse(tradeId);
			try {
		        if (strade !=null ) {
			        response.setTradeId(strade.getId());
					response.setSuccess(true);
					response.setMessage("Successful");
					response.setContent(strade);
					//tradeMBean.incSave();
		        }
			} catch (Exception ex) {
				logger.error("Error converting trade " + tradeId, ex);
				Message temp = createTextMessage(outer);
				auditLog(auditId, "RESPONSE", "ScctTradeNewResponseType", temp, ERROR_MSG, 
					"Returning response Error converting trade " + tradeId + ex, tradeId); 
			}
		} else {
			response.setSuccess(false);
			response.setMessage("Failure");
			Message temp = createTextMessage(outer);
			auditLog(auditId, "RESPONSE", "ScctTradeNewResponseType", temp, ERROR_MSG, 
				"Returning response 'Failure' because tradeId was not >0 ",tradeId); 
		}
		return outer;
	}

	public  JAXBElement<ScctFavoritesLoadResponseType>  handleScctFavoritesLoad(
			ScctFavoritesLoadType update, String username, String sessionId) throws Exception{
		ObjectFactory factory = new ObjectFactory();

		ScctFavoritesLoadResponseType response = factory.createScctFavoritesLoadResponseType();
		JAXBElement<ScctFavoritesLoadResponseType> outer = factory.createScctFavoritesLoadResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		ScctFavoritesType favorites = factory.createScctFavoritesType();
		usession.loadFavorites(favorites);
		response.setScctFavorites(favorites);
		return outer;
	}


	public void handleTradeEvent(ScctTradeEventType tradeEventType) throws Exception {
		try{
		    ObjectFactory factory = new ObjectFactory();
			JAXBElement<ScctTradeEventType> outer = factory
					.createScctTradeEvent(tradeEventType);
			Message message = createTextMessage(outer);
			TradeQueryHelper.addQueryAttributesToMessage(message, tradeEventType.getScctTrade());
	        logger.debug("Query attributes added to message header: " + message.toString());
	        
	        int tradeId = tradeEventType.getScctTrade().getId();
	        int version = tradeEventType.getScctTrade().getVersion();
	        
	        String auditTradeEventsType = properties.getProperty("AuditLog.auditTradeEventsType");
	        if("none".equalsIgnoreCase(auditTradeEventsType)){
	        	logger.debug("Logging is off for trade events, event was not logged");
	        	publishMessage(tradeEvent, message);
	        	return;
	        }
	        if(CalypsoAdapterConstants.FILE.equalsIgnoreCase(auditTradeEventsType) || 
	        		CalypsoAdapterConstants.BOTH.equalsIgnoreCase(auditTradeEventsType)){
	        	tradeEventsLogger.debug("tradeId: " + tradeId + "  version: " + version);
	        	logger.debug("Logged trade event to file successfully");
	        }
	        if(!CalypsoAdapterConstants.NONE.equalsIgnoreCase(auditTradeEventsType)){
	        	String errMsg = null;
	        	try {
		        	Timestamp stamp = com.citigroup.scct.util.DateUtil.getNowAsSybaseTimestamp("GMT");
					Connection conn = DBUtil.getConnection(calypsoDBURL, calypsoDBUser, calypsoDBPassword);
		        	String sql = "INSERT INTO " + properties.getProperty("AuditLog.auditDb") + ".." +
					 					properties.getProperty("AuditLog.auditDbTblTradeEvents") +  
					 			 " VALUES (" + tradeId + "," + version + ",'" + stamp + "')";
		        	if (!DBUtil.executeDML(sql, conn)){
		        		errMsg = "ERROR occurred when attempting to EXECUTE trade update logging SQL";
		        	}
		        	if (!DBUtil.closeConnection(conn)){
		        		errMsg = "ERROR occurred when attempting to CLOSE CONNECTION for trade update logging SQL";
		        	}
	        	} catch (Exception e) {
	        		errMsg = "EXCEPTION occurred when attempting to CREATE CONNECTION for trade update logging SQL:\n" 
	        			+ e + "\n" + ScctUtil.getStackTrace(e);	
	        	} finally {
	        		if (errMsg != null){
	        			logger.error(errMsg);
	        			tradeEventsLogger.debug("tradeId: " + tradeId + "  version: " + version 
	        				+ "  ***Note - Tried to log this event to db but exception occurred - "
	        				+ " see primary logfile for details");
	        			logger.error("Wrote message to trade event log FILE due to db error.");
	        		}
	        		else{
	        			logger.debug("Logged trade event to DB successfully");
	        		}
	        	}
	        }
			publishMessage(tradeEvent, message);
		}
		catch(Exception e){
			performErrorProcessing(e.getMessage());
			throw e;
		}
	}


	public void handlePortfolioEvent(ScctPortfolioType portfolio) throws Exception {
	    ObjectFactory factory = new ObjectFactory();
    	// update the portfolio in cache and publish event
    	getCache().setPortfolio(portfolio.getPortfolioID(), portfolio);
		ScctPortfolioEventType response = factory.createScctPortfolioEventType();
		JAXBElement<ScctPortfolioEventType> outer = factory.createScctPortfolioEvent(response);
		response.setScctPortfolio(portfolio);
		Message message = createTextMessage(outer);
		publishMessage(tradeEvent, message);
	}


	public JAXBElement<ScctPortfolioLoadResponseType> handleScctPortfolioLoad(ScctPortfolioLoadType portfolioLoadType)
											throws Exception {
	    ObjectFactory factory = new ObjectFactory();
		StringWriter xmlDocument = new StringWriter();

		ScctPortfolioLoadResponseType response = factory.createScctPortfolioLoadResponseType();
		JAXBElement<ScctPortfolioLoadResponseType> outer = factory.createScctPortfolioLoadResponse(response);

		Vector portfolios = getCache().getPortfolios();

		Iterator iter = portfolios.iterator();
		while (iter.hasNext()) {
			ScctPortfolioType p = (ScctPortfolioType)iter.next();
			response.getScctPortfolio().add(p);
		}
		logger.debug("Number of portfolios loaded: " + portfolios.size());
		return outer;

	}

	public JAXBElement<ScctPortfolioNewResponseType> handleScctPortfolioNew(
			ScctPortfolioNewType portfolioNewType, String username,
			String sessionId) throws Exception {
	    ObjectFactory factory = new ObjectFactory();
		StringWriter xmlDocument = new StringWriter();

		ScctPortfolioNewResponseType response = factory.createScctPortfolioNewResponseType();
		JAXBElement<ScctPortfolioNewResponseType> outer = factory.createScctPortfolioNewResponse(response);

		ScctPortfolioType portfolio = portfolioNewType.getScctPortfolio();
		AdapterUserSession usession = getUserSession(sessionId);
		long portfolioId = usession.updatePortfolio(portfolio);
		logger.debug("Created new portfolio with id: " + portfolioId);
		response.setMessage("Successful");
		response.setPortfolioID(portfolioId);
		response.setSuccess(true);
		return outer;
	}


	public JAXBElement<ScctPortfolioUpdateResponseType> handleScctPortfolioUpdate(
			ScctPortfolioUpdateType portfolioUpdateType, String username,
			String sessionId) throws Exception {
	    ObjectFactory factory = new ObjectFactory();
		StringWriter xmlDocument = new StringWriter();

		ScctPortfolioUpdateResponseType response = factory.createScctPortfolioUpdateResponseType();
		JAXBElement<ScctPortfolioUpdateResponseType> outer = factory.createScctPortfolioUpdateResponse(response);

		ScctPortfolioType portfolio = portfolioUpdateType.getScctPortfolio();
		AdapterUserSession usession = getUserSession(sessionId);
		long portfolioId = usession.updatePortfolio(portfolio);
		logger.debug("Updated portfolio with id: " + portfolioId);
		response.setMessage("Successful");
		response.setPortfolioID(portfolioId);
		response.setSuccess(true);
		return outer;

	}

	public JAXBElement<ScctTradeTemplateUpdateResponseType> handleScctTradeTemplateUpdate(
			ScctTradeTemplateUpdateType update, String username,
			String sessionId) throws Exception {
		ObjectFactory factory = new ObjectFactory();

		ScctTradeTemplateUpdateResponseType response = factory.createScctTradeTemplateUpdateResponseType();
		JAXBElement<ScctTradeTemplateUpdateResponseType> outer = factory.createScctTradeTemplateUpdateResponse(response);

		ScctTradeTemplateType tradeTemplate = update.getScctTradeTemplate();
		AdapterUserSession usession = getUserSession(sessionId);
		usession.updateTradeTemplate(tradeTemplate);
		response.setMessage("Successful");
		response.setSuccess(true);
		return outer;
	}

	public JAXBElement<ScctTradeTemplateLoadResponseType> handleScctTradeTemplateLoad(
			ScctTradeTemplateLoadType load, String username, String sessionId)
			throws Exception {
		ObjectFactory factory = new ObjectFactory();

		ScctTradeTemplateLoadResponseType response = factory.createScctTradeTemplateLoadResponseType();
		JAXBElement<ScctTradeTemplateLoadResponseType> outer = factory.createScctTradeTemplateLoadResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		Vector templates = usession.loadTradeTemplate(load.getProductType(), load.getTemplateName());
		Iterator iter = templates.iterator();
		while (iter.hasNext()) {
			ScctTradeTemplateType tt = (ScctTradeTemplateType)iter.next();
			response.getScctTradeTemplate().add(tt);
		}
		return outer;
	}

	public JAXBElement<QueryScctTradeHistoryResponseType>  handleQueryScctTradeHistory(QueryScctTradeHistoryType query, String username, String sessionId) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		QueryScctTradeHistoryResponseType response = factory.createQueryScctTradeHistoryResponseType();
		JAXBElement<QueryScctTradeHistoryResponseType> outer = factory.createQueryScctTradeHistoryResponse(response);

		AdapterUserSession usersession = getUserSession(sessionId);
		usersession.queryTradeHistory(query.getId(), response);
		return outer;
	}

	public JAXBElement<ScctTradeGroupNewResponseType> handleScctTradeGroupNew(
			ScctTradeGroupNewType newTradeGroupType, String username,
			String sessionId, String auditId) throws Exception {
		ObjectFactory factory = new ObjectFactory();
		ScctTradeGroupNewResponseType response = factory.createScctTradeGroupNewResponseType();
		JAXBElement<ScctTradeGroupNewResponseType> outer = factory.createScctTradeGroupNewResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		if (usession.newTradeGroup(newTradeGroupType.getScctTradeGroup())) {
			response.setMessage("Successful");
			response.setSuccess(true);
		}
		else {
			response.setMessage("Unable to add new trade group");
			response.setSuccess(false);
			Message temp = createTextMessage(outer);
			auditLog(auditId, "RESPONSE", "ScctTradeGroupNewResponseType", temp, ERROR_MSG, 
				"Returning response 'unable to add new trade group'",-1); 
		}
		return outer;
	}

	public JAXBElement<ScctAmendActionToReasonLoadResponseType> handleScctAmendActionToReasonLoad(
			ScctAmendActionToReasonLoadType loadType) throws Exception {
	    ObjectFactory factory = new ObjectFactory();
		StringWriter xmlDocument = new StringWriter();

		ScctAmendActionToReasonLoadResponseType response = factory.createScctAmendActionToReasonLoadResponseType();
		JAXBElement<ScctAmendActionToReasonLoadResponseType> outer = factory.createScctAmendActionToReasonLoadResponse(response);

		Iterator iter = ActionToReasonMap.getInstance().keySet().iterator();
		while (iter.hasNext()) {
			String action = (String)iter.next();
			ArrayList reasonList = (ArrayList)ActionToReasonMap.getInstance().get(action);
			logger.debug("action/reason: " + action + " - " + reasonList);
			ScctAmendActionToReasonType type = factory.createScctAmendActionToReasonType();
			type.setAction(action);
			type.getReason().addAll(reasonList);
			response.getMap().add(type);
		}
		return outer;

	}

	public JAXBElement<ScctIndexTradeCaptureDataLoadResponseType> handleScctIndexTradeCaptureDataLoad(
			ScctIndexTradeCaptureDataLoadType loadType) throws Exception {
	    ObjectFactory factory = new ObjectFactory();
		StringWriter xmlDocument = new StringWriter();

		ScctIndexTradeCaptureDataLoadResponseType response = factory.createScctIndexTradeCaptureDataLoadResponseType();
		JAXBElement<ScctIndexTradeCaptureDataLoadResponseType> outer = factory.createScctIndexTradeCaptureDataLoadResponse(response);

		HashMap itcdMap = getCache().getAllIndexTradeCaptureData();
		Iterator iter = itcdMap.keySet().iterator();
		while (iter.hasNext()) {
			String issuerCode = (String)iter.next();
			IndexTradeCaptureData itcd = (IndexTradeCaptureData)itcdMap.get(issuerCode);
			ScctIndexTradeCaptureDataType type = factory.createScctIndexTradeCaptureDataType();
			type.setIssuerCode(issuerCode);
			if (itcd.getFrontStubDate() > 0)
				type.setFrontStubDate(com.citigroup.scct.util.DateUtil.intDatetoString(itcd.getFrontStubDate()));
			if (itcd.getIssueDate() > 0)
				type.setIssueDate(DateUtil.intDatetoString(itcd.getIssueDate()));
			if (itcd.getMaturityDate() > 0)
				type.setMaturityDate(DateUtil.intDatetoString(itcd.getMaturityDate()));
			type.setSpread(itcd.getSpread());
			response.getScctIndexTradeCaptureData().add(type);
		}
		return outer;

	}

	public JAXBElement<QueryTodaysTradesResponseType> handleQueryTodaysTrades(
			String username, String sessionId) throws Exception {
			ObjectFactory factory = new ObjectFactory();
			StringWriter xmlDocument = new StringWriter();

			QueryTodaysTradesResponseType response = factory
					.createQueryTodaysTradesResponseType();
			JAXBElement<QueryTodaysTradesResponseType> outer = factory
					.createQueryTodaysTradesResponse(response);

			AdapterUserSession usession = getUserSession(sessionId);
			StringBuffer status = new StringBuffer();
			Vector trades = usession.queryTodaysTrades(status);
			if (trades != null && trades.size() > 0)
				response.getScctTrade().addAll(trades);
			return outer;
		}

	public JAXBElement<QueryTodaysTradesByBookResponseType> handleQueryTodaysTradesByBook(
			QueryTodaysTradesByBookType query, String username, String sessionId) throws Exception {
			ObjectFactory factory = new ObjectFactory();
			StringWriter xmlDocument = new StringWriter();

			QueryTodaysTradesByBookResponseType response = factory
					.createQueryTodaysTradesByBookResponseType();
			JAXBElement<QueryTodaysTradesByBookResponseType> outer = factory
					.createQueryTodaysTradesByBookResponse(response);

			AdapterUserSession usession = getUserSession(sessionId);
			StringBuffer status = new StringBuffer();
			Vector trades = usession.queryTodaysTradesByBook(query.getBookName(), status);
			if (trades != null && trades.size() > 0)
				response.getScctTrade().addAll(trades);
			return outer;
		}

	public JAXBElement<QueryTodaysTradesByUserResponseType> handleQueryTodaysTradesByUser(
			QueryTodaysTradesByUserType query, String username, String sessionId) throws Exception {
			ObjectFactory factory = new ObjectFactory();
			StringWriter xmlDocument = new StringWriter();

			QueryTodaysTradesByUserResponseType response = factory
					.createQueryTodaysTradesByUserResponseType();
			JAXBElement<QueryTodaysTradesByUserResponseType> outer = factory
					.createQueryTodaysTradesByUserResponse(response);

			AdapterUserSession usession = getUserSession(sessionId);
			StringBuffer status = new StringBuffer();
			Vector trades = usession.queryTodaysTradesByUser(query.getUserName(), status);
			if (trades != null && trades.size() > 0)
				response.getScctTrade().addAll(trades);
			return outer;
		}


	public JAXBElement<QueryTradesByNumberOfDaysResponseType> handleQueryTradesByNumberOfDays(
		QueryTradesByNumberOfDaysType query, String username, String sessionId) throws Exception {
		ObjectFactory factory = new ObjectFactory();

		QueryTradesByNumberOfDaysResponseType response = factory
				.createQueryTradesByNumberOfDaysResponseType();
		JAXBElement<QueryTradesByNumberOfDaysResponseType> outer = factory
				.createQueryTradesByNumberOfDaysResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		StringBuffer status = new StringBuffer();
		Vector trades = usession.queryTradesByNumberOfDays(query, status);
		if (trades != null && trades.size() > 0)
			response.getScctTrade().addAll(trades);
		return outer;
	}

	public JAXBElement<QueryScctTradeArraysResponseType> handleQueryTradeArrays(QueryScctTradeArraysType query, String username, String sessionId,String auditId) throws Exception {
		ObjectFactory factory = new ObjectFactory();

		QueryScctTradeArraysResponseType response = factory.createQueryScctTradeArraysResponseType();
		JAXBElement<QueryScctTradeArraysResponseType> outer = factory
				.createQueryScctTradeArraysResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		StringBuffer status = new StringBuffer();
		Vector trades = usession.queryTradeArrays(query, status,auditId);
		if (trades != null && trades.size() > 0)
			response.getScctTrade().addAll(trades);
		return outer;
	}

	public void handleBookEvent(Book book, String action) throws Exception {
	    ObjectFactory factory = new ObjectFactory();

		ScctBookEventType response = factory.createScctBookEventType();
		JAXBElement<ScctBookEventType> outer = factory.createScctBookEvent(response);

		response.setAction(action);
		//isaac - Situations like this should throw some error if it is null
		if (book != null)
			response.setScctBook(ConverterFactory.getInstance().getBookConverter().convertFromCalypso(book));
		Message message = createTextMessage(outer);
        logger.debug("Publishing book event message: " + message.toString());
		publishMessage(tradeEvent, message);
	}

	public void handleLegalEntityEvent(LegalEntity le, String action) throws Exception {
	    ObjectFactory factory = new ObjectFactory();

		ScctLegalEntityEventType response = factory.createScctLegalEntityEventType();
		JAXBElement<ScctLegalEntityEventType> outer = factory.createScctLegalEntityEvent(response);

		response.setAction(action);
		if (le != null)
			response.setScctLegalEntity(ConverterFactory.getInstance().getLegalEntityConverter().convertFromCalypso(le));
		Message message = createTextMessage(outer);
        logger.debug("Publishing Legal Entity event message: " + message.toString());
		publishMessage(tradeEvent, message);
	}

	protected void dispose(){
		logger.fatal("Exiting application...");
        try {
        	// TODO
        	logger.debug("Closing calypso connections on exit ");
        	Iterator iter = userSessions.keySet().iterator();
        	while ( iter.hasNext()){
        		handleUserSessionLogout((String)iter.next());
        	}
        	this.ds.disconnect();
        	this.ps.ps.stop();
        }catch(Throwable e){
        logger.fatal("Exited application.");
        }
	}

	protected Vector getUserBookNames(String username) {
		Map userValues = getOnlyAdapterUserSessions();
		Set entries = userValues.entrySet();
		Map.Entry entry;
		AdapterUserSession usession = null;
		Iterator itr = entries.iterator();
		while (itr.hasNext()) {
		  entry = (Map.Entry) itr.next();
		  if (((String)entry.getKey()).startsWith(username)) {
			  usession = (AdapterUserSession) entry.getValue();
		  }
		}
		return Util.list2Vector(usession.getBookNames());
	}

	public boolean isAnyBookValidForUser(String username) {
		boolean result = false;
		Map userValues = getOnlyAdapterUserSessions();
		Set entries = userValues.entrySet();
		Map.Entry entry;
		AdapterUserSession usession = null;
		Iterator itr = entries.iterator();
		while (itr.hasNext()) {
			entry = (Map.Entry) itr.next();
			if (((String) entry.getKey()).startsWith(username)) {
				usession = (AdapterUserSession) entry.getValue();
			}
		}
		if (usession != null) {
			result = usession.getUserProfile().isReadWriteBook(
					UserAccessPermission.BOOK_ANY_NAME);
		}
		return result;
	}


/*	public AdapterUserSession getAdapterUserSession(String sessionId) {
		return getUserSession(sessionId);

	}
*/
	public  JAXBElement<ScctUserConfigLoadResponseType>  handleScctUserConfigLoad(ScctUserConfigLoadType load,
			String username) throws Exception{
		ObjectFactory factory = new ObjectFactory();

		ScctUserConfigLoadResponseType response = factory.createScctUserConfigLoadResponseType();
		JAXBElement<ScctUserConfigLoadResponseType> outer = factory.createScctUserConfigLoadResponse(response);

		String xml = ScctUtil.getUserConfig(username);
		response.setXml(xml);
		return outer;
	}

	public  JAXBElement<ScctUserConfigUpdateResponseType>  handleScctUserConfigUpdate(ScctUserConfigUpdateType update,
			String username) throws Exception {
		ObjectFactory factory = new ObjectFactory();

		ScctUserConfigUpdateResponseType response = factory.createScctUserConfigUpdateResponseType();
		JAXBElement<ScctUserConfigUpdateResponseType> outer = factory.createScctUserConfigUpdateResponse(response);

		String xml = update.getXml();
		ScctUtil.saveUserConfig(username, xml);
		logger.debug("User Config updated for " + username);
		response.setMessage("Successful");
		response.setSuccess(true);
		return outer;
	}

	public  JAXBElement<QueryRefObResponseType>  handleQueryRefOb(QueryRefObType query,
			String username, String sessionId) throws Exception {
		ObjectFactory factory = new ObjectFactory();

		QueryRefObResponseType response = factory.createQueryRefObResponseType();
		JAXBElement<QueryRefObResponseType> outer = factory.createQueryRefObResponse(response);

		AdapterUserSession usession = getUserSession(sessionId);
		long x = System.currentTimeMillis();
		Vector result = usession.queryRefOb(query);
		long y = System.currentTimeMillis();
		logger.debug("queryRefOb : " + query.getProductCode() + "/" + query.getCodeValue() + " processed time = " + (y-x)/1000.0);
		if (result != null && result.size() > 0)
			response.getBondResult().addAll(result);
		return outer;
	}

	private void doQueryUserCalendar(String username, QueryHolidayCalendarResponseType calendarResponse) {
		UserDefaults userDefaults = CalypsoAdapter.getAdapter().getCache()
				.getUserDefaults(username);
		if (userDefaults != null && userDefaults.getHolidays() != null) {
			Vector codes = userDefaults.getHolidays();
			List<ScctHolidayCalendarType> calendars = cache.getHolidayCalendars(codes);
			for(ScctHolidayCalendarType calendar : calendars) {
				Collections.sort(calendar.getDate());
				calendarResponse.getHoliday().add(calendar);
			}
		}
	}
	
	public void updateValues(){
		logger.debug("Entered updateValues");
		boolean isRequired = true;
		boolean isChecked = false;
		if(!isProcessing){
			if(updatedValues.remove(value)){
				isProcessing = true;
				isChecked = true;
				int count = 0;
				boolean isNotCompleted = true;
				logger.debug("Performing update");
				while(count < 2 && isNotCompleted){
					try{
						if(ds != null && ds.getDSConnection() != null){
							try{
								ds.getDSConnection().disconnect();
								ds=null;
							}
							catch(Throwable e){
								ds = null;
							}
						}
						preInitialize();
						initialize();
						isNotCompleted = false;
					}
					catch(Throwable e){
						logger.debug("update not successfull");
					}
					count = count+1;
				}
				logger.debug("Update complete");
				isProcessing = false;
				isRequired = true;
				setUpdateValues();
			}
			else if(isRequired){
				isRequired = false;
			}
		}
		else if(isRequired){
			isRequired = false;
		}
		if(!isRequired){
			while(isProcessing){
				int count = 0;
				boolean isReq = false;
				if(count%100 == 0){
					isReq = true;
				}
				print(isReq);
				count=count+1;
				isReq = false;
			}
		}
		if(isChecked){
			logger.debug("Exiting updateValues");
		}
	}

	private List doQueryUserDefault(String username) throws Exception{
		List results = new ArrayList();
		UserDefaults userDefaults = CalypsoAdapter.getAdapter().getCache().getUserDefaults(username);
		if (userDefaults !=null ) {
			ScctUserDefaultType scctUserDefaults = ConverterFactory.getInstance().getUserDefaultConverter().convertFromCalypso(userDefaults);
			results.add(scctUserDefaults);
		}
		return results;
	}
	
	public String getDBURL(){
		return calypsoDBURL; 
	}
	
	public String getDBUser(){
		return calypsoDBUser;
	}
	
	public String getDBPassword(){
		return calypsoDBPassword;
	}

	private List doQueryRatingAgency() {
		List results = new ArrayList();
		Hashtable agencies = cache.getRatingAgency();
		if (agencies !=null) {
			Set<Map.Entry> tmp = agencies.entrySet();
			for (Map.Entry entry : tmp) {
				String key = (String) entry.getKey();
				Vector<String> values = (Vector) entry.getValue();
				if (values !=null) {
					for (String value : values) {
						ScctRatingAgencyType agency = new ScctRatingAgencyType();
						agency.setAgency(key);
						agency.setRatingValue(value);
						results.add(agency);
					}
				}
			}
		}
		return results;
	}

	protected RemoteAccess getRemoteAccess() {
		return remoteAccess;
	}
	
	private ScctTradeType convertTradeResponse(int tradeId) {
	  ScctTradeType strade = null;
		try {
			Trade trade = this.ds.getDSConnection().getRemoteTrade().getTrade(tradeId);
			TradeConverter converter = ConverterFactory.getInstance().getTradeConverter(trade);
			if (trade != null && converter != null) {
				strade = converter.convertFromCalypso(trade);
				Vector actions = TradeWorkflow.getTradeActions(trade, this.ds.getDSConnection());
				if (actions != null && actions.size() > 0) {
				  strade.getAvailableActions().addAll(actions);
				}
			}
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		return strade;
	}
	
	private void print(boolean isReq){
		try{
			if(isReq){
				logger.debug("Processing to continue");
			}
			Thread.sleep(1000);
		}
		catch(Exception e){
		}
	}



	private void auditLog(String id, String requestOrResponse, String messageType, 
			Message message, String success, String note, int tradeId){
		//we need the variable responseFlag in handleBrokerMessage because the try block in that method is so big
		// that if theres an error and fall into the catch block, we don't know if we should be auditing a failed
		// request or a failed response.  This massive try/catch in handleBrokerMessage should be redesigned.
		String auditType = properties.getProperty("AuditLog.auditType");
		if (CalypsoAdapterConstants.NONE.equalsIgnoreCase(auditType)){
			logger.debug("Audit logging turned off, nothing logged for this event.");
			return;
		}	
		if (CalypsoAdapterConstants.FILE.equalsIgnoreCase(auditType) || 
				CalypsoAdapterConstants.BOTH.equalsIgnoreCase(auditType)){
			String msgNoPassword = null;
			if (message != null){
				msgNoPassword = message.toString().replaceAll("<password>.*</password>","");
			}
			String print;
			String commonFields = "\nPart of ID: " + id + "\nStatus: " + success + "\nNote: " + note + "\n";
			if ("REQUEST".equals(requestOrResponse)) {	
				String user = null, host = null;
				if (message != null && message.toString().length() > 0){
					try {
						user = message.getStringProperty("username");
						host = message.getStringProperty("hostName");
					} catch (JMSException e) {
						logger.error("JMS Exception getting username/hostname in audit log process - ");
						logger.error(ScctUtil.getStackTrace(e));
					}
				}
				print = "\nReceived REQUEST type: " + messageType
				+ commonFields
				+ "From user: " + user
				+ "\nFrom host: " + host
				+ "\nMessage follows:\n" + msgNoPassword;
			}
			//message is a response	
			else { 
				print = "\nSent RESPONSE type: " + messageType + commonFields + "Trade ID: " + tradeId;
				//If the response was problematic, also log the message
				if (!SUCCESS_MSG.equals(success)){ 
					print += "\nMessage follows:\n" + msgNoPassword + "\n\n";
				}
			}
			auditLogger.debug(print + "\n");
		}
		//destinationType is "db", "both", or something else
		if (!auditType.equalsIgnoreCase(CalypsoAdapterConstants.FILE)) {
			boolean writeToFile = false;
			Timestamp stamp = com.citigroup.scct.util.DateUtil.getNowAsSybaseTimestamp("GMT");
			logger.debug("Logging " + requestOrResponse + " to DB");
			//add a single quote to all single quotes so sybase will be able to insert as one string	
			if (note != null){
				note = note.replace("'","''");
			}
			
			if ("REQUEST".equals(requestOrResponse)){ 
				String msgNoPassword = null, user = null, host = null;
				if (message != null && message.toString().length() > 0){
					msgNoPassword = message.toString().replaceAll("<password>.*</password>","");
					msgNoPassword = msgNoPassword.replace("'","''");
					try {
						user = message.getStringProperty("username");
						host = message.getStringProperty("hostName");
					} catch (JMSException e) {
						logger.error("JMS Exception getting username/hostname in audit log process - ");
						logger.error(ScctUtil.getStackTrace(e));
					}
				}	
				String errMsg = null;
				try {
					Connection conn = DBUtil.getConnection(calypsoDBURL, calypsoDBUser, calypsoDBPassword);
					String dbName = properties.getProperty("AuditLog.auditDb");
					String tblName = properties.getProperty("AuditLog.auditDbTblIn");
					
					String sql = "INSERT INTO " + dbName + ".." + tblName +  
								" VALUES ('" + id + "','" + messageType + "','" + success + "','"  + user + "','" + host + 
							  			  "','" + note + "','" + stamp + "','"  + msgNoPassword + "')"; 
					if(!DBUtil.executeDML(sql, conn)){
						errMsg = "ERROR occurred when attempting to EXECUTE incoming message SQL";
					}
					if(!DBUtil.closeConnection(conn)){
						errMsg = "ERROR occurred when attempting to CLOSE CONNECTION for incoming message SQL";
					}
				} catch (Exception e) {
					errMsg = "EXCEPTION occurred when attempting to CREATE CONNECTION for incoming message SQL:\n" 
	        			+ e + "\n" + ScctUtil.getStackTrace(e);
				} finally {
					if (errMsg != null){
						logger.error(errMsg);
						auditLogger.debug("Received REQUEST type: " + messageType + "\nPart of ID: " + id + 
							"\ntrade id: " + tradeId + "\nStatus: " + success + "\nNote: " + note + 
							"\nfrom user: " + user + "\nfrom host: " + host + "\nMessage follows:\n" + msgNoPassword);
						logger.error("Wrote message to Audit log FILE due to error, check there for more detail.");
					}
				}
			}	
			//message is a response
			else{
				//If the response was problematic, also log the message
				String msgNoPassword = null;
				if (!SUCCESS_MSG.equals(success) && message != null && message.toString().length() > 0){	
					msgNoPassword = message.toString().replaceAll("<password>.*</password>","");
					msgNoPassword = msgNoPassword.replace("'","''");
				}
				String errMsg = null;
				try {		   
					Connection conn = DBUtil.getConnection(calypsoDBURL, calypsoDBUser, calypsoDBPassword);
					String dbName = properties.getProperty("AuditLog.auditDb");
					String tblName = properties.getProperty("AuditLog.auditDbTblOut");
					
					String sql = "INSERT INTO " + dbName + ".." + tblName +
								" VALUES ('" + id + "'," + tradeId + ",'" + messageType + "','" + success + "','" +    
										  note + "','" + stamp + "','" + msgNoPassword + "')";
					if(!DBUtil.executeDML(sql, conn)){
						errMsg = "ERROR occurred when attempting to EXECUTE outgoing message SQL";
					}
					if(!DBUtil.closeConnection(conn)){
						errMsg = "ERROR occurred when attempting to CLOSE CONNECTION for outgoing message SQL";
					}
				} catch (Exception e) {
					errMsg = "EXCEPTION occurred when attempting to CREATE CONNECTION for outgoing message SQL:\n" 
	        			+ e + "\n" + ScctUtil.getStackTrace(e);
				} finally {
					if (errMsg != null){
						logger.error(errMsg);
						auditLogger.debug("Sent RESPONSE type: " + messageType + "\nPart of ID: " + id + 
									"\ntrade id: " + tradeId + "\nStatus: " + success + "\nNote: " + note + 
									"\nMessage follows:\n" + msgNoPassword);
						logger.error("Wrote message to Audit log FILE due to error, check there for more detail.");
					}
				}
			}//end response code
		}//end destination type = db
	}	

		
	
	private boolean validateDBSettings() {
		 logger.debug("Validating DB properties...");	 
	     try {    	 
	    	 calypsoDBURL = Defaults.getProperty(Defaults.DBURL);
	    	 calypsoDBUser = Defaults.getProperty(Defaults.DBUSER);
	    	 calypsoDBPassword = Defaults.getProperty(Defaults.DBPASSWORD);
	    	 //String driver = Defaults.getProperty("DB.driver");
	    	 String dbName = properties.getProperty("AuditLog.auditDb");
	    	 String dbTableIn = properties.getProperty("AuditLog.auditDbTblIn");
	    	 String dbTableOut = properties.getProperty("AuditLog.auditDbTblOut");
	    	 String dbTableTradeEvents = properties.getProperty("AuditLog.auditDbTblTradeEvents");
	    	 String genLogType = properties.getProperty("AuditLog.auditType"); 
	    	 String tradeEventsLogType = properties.getProperty("AuditLog.auditTradeEventsType");

	    	 logger.debug("Will connect to the DB with the following properties - " +
	    			 //"\ndriver: " + driver + 
	    			 "\nurl: " + calypsoDBURL + 
	    			 "\ndbUser: " + calypsoDBUser + 
	    			 "\ndbPasswd: [blocked]" + 
	    			 "\ndbName: " + dbName + 
	    			 "\ndbTableIn: " + dbTableIn +
	    			 "\ndbTableOut: " + dbTableOut +
	    			 "\ndbTradeEvents: " + dbTableTradeEvents +
	    			 "\ngenLogType: " + genLogType + 
	    			 "\ntradeEventsLogType: " + tradeEventsLogType);

	    	 if (	 //driver.equals("")    			|| driver == null   		  ||
	    			 calypsoDBURL.equals("") 	  	|| calypsoDBURL == null		  ||
	    			 calypsoDBUser.equals("") 	  	|| calypsoDBUser == null	  ||
	    			 calypsoDBPassword.equals("") 	|| calypsoDBPassword == null  ||
	    			 dbName.equals("") 	  			|| dbName == null			  ||
	    			 dbTableIn.equals("") 			|| dbTableIn == null		  ||
	    			 dbTableOut.equals("")			|| dbTableOut == null 	 	  ||
	    			 dbTableTradeEvents.equals("")  || dbTableTradeEvents == null ||
	    			 genLogType.equals("")			|| genLogType == null 		  ||
	    			 tradeEventsLogType.equals("")	|| tradeEventsLogType == null){
	    		 logger.error("ERROR: Unable to read one or more DB properties");
	    		 return false;
	    	 } 
	    	 return true;
	   
	     } catch (Exception e) {
	    	 logger.error("Exception " + e.getMessage());
	    	 logger.error(ScctUtil.getStackTrace(e));
	    	 return false;
	     }
	}
	
} //end class