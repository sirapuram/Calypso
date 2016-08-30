/*
 * Created on Oct 20, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.citigroup.scct.server;


import java.io.*;
import java.net.*;
import java.util.*;

import javax.jms.*;
import javax.naming.*;
import javax.xml.bind.*;

import org.apache.log4j.*;


import com.calypso.tk.core.Util;
import com.citigroup.project.util.PropsUtil;
import com.citigroup.scct.calypsoadapter.CalypsoAdapterConstants;
import com.citigroup.scct.cgen.ScctErrorType;
import com.citigroup.scct.cgen.ScctHashMapType;
import com.citigroup.scct.cgen.ScctTradeEventType;
import com.citigroup.scct.cgen.ScctTradeType;
import com.citigroup.scct.server.Destinations;
import com.citigroup.scct.util.ScctProperties;
import com.citigroup.scct.util.ScctUtil;
import com.citigroup.scct.util.ThreadManager;

/**
 * @author mi54678
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public abstract class ScctServer implements MessageListener, Destinations{

	// Mail error levels
	public static final String WARNING = "warning";
	public static final String ERROR = "error";
	public static final String CRITICAL = "critical";
	  
	protected static int THREAD_POOL_SIZE = 25;
	protected ThreadManager threadManager;
	protected Properties properties;
	protected static ScctServer server;
	protected Logger logger;
	protected InitialContext jndiContext;
	protected boolean isFailover;
	protected Connection connection ;
	protected Session session;
	protected MessageProducer producer;
	protected String region;
	protected String instanceId;
	

	protected static ScctServer createInstance(String className) {
		server = null;
		try {
			server = (ScctServer) Class.forName(className).newInstance();
		} catch (Throwable e) {
			printUsageAndExit(1, e);
		}
		return server;
	}
	
	public ScctServer getInstance(){
		return server;
	}
	
	public void setInstanceId(String id) {
		instanceId = id;
	}
	
	public String getInstanceId() {
		return instanceId;
	}
	
	protected void preInitialize() throws Exception{
		if ( System.getProperty("log4j.configuration") != null )
			PropertyConfigurator.configure(System.getProperty("log4j.configuration"));
		else 
			BasicConfigurator.configure();
		
		logger = Logger.getLogger(this.getClass().getName());
		
		int poolSize = THREAD_POOL_SIZE;
		String tmp = (String) properties.get(CalypsoAdapterConstants.THREAD_POOL_SIZE);
		if (tmp != null) {
			try {
				poolSize = Integer.parseInt(tmp);
				if(poolSize > 0){
					THREAD_POOL_SIZE=poolSize;
				}
			} catch (RuntimeException e) {
				logger.warn("Unable to parse properties " + CalypsoAdapterConstants.THREAD_POOL_SIZE + "'" + tmp + "'");
			}
		}
		String value = properties.getProperty(CalypsoAdapterConstants.FAILOVER);
		if(value != null && value.equalsIgnoreCase("YES")){
			isFailover = true;
		}
		threadManager= new ThreadManager(poolSize, "MessageHandler");
		threadManager.setFailover(isFailover);
		createSessions();
	}
	
	/*
	 * initialize method implemented by subclasses
	 */
	protected abstract void initialize();
	
	/*
	 * handle broker messages. Implemented by subclasses
	 * returns whether handled by the individual server or not
	 */
	public abstract boolean handleBrokerMessage( Message message)throws Exception;

	public void start(Properties props) {
		try {
			
		
		this.properties = props;
		preInitialize();
		//  ((GatewayProperties)properties).normalizeProperties();
		logger.debug("Loaded proeprties \n " + properties);

		String type = (String) properties.get(ScctProperties.SERVER_TYPE);
		if (type == null) {
			printAndExit(1, "Server Type is not specified.");
		}
		region = (String) properties.get(ScctProperties.REGION);
		if (region == null) {
			printAndExit(1, "region is not specified.");
		}
		initialize();
		}catch (Exception ex){
			logger.error(ex);
		}
		
	}
	
	protected Session getSession() throws JMSException{
		Session session=null;
		if(connection != null){
			session = connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
		}
		return session;
	}
	
	protected MessageProducer getProducer() throws JMSException{
		MessageProducer producer = null;
		if(session != null){
			producer = session.createProducer(null);
		}
		return producer;
	}


	public static void printUsageAndExit(int exitStatus, Throwable ex) {
		if (ex != null)
			System.err.println("Server Exception" + ex);
		printAndExit(exitStatus, "Usage: java  " + ScctServer.class.getName()
				+ " <properties file>");
	}

	public static void printAndExit(int status, String msg) {
		System.out.println("\nExiting: " + msg);
		System.exit(status);
	}

	public ScctServer() {
	};

	protected void initialize(Properties props) {
		properties = props;
		threadManager = new ThreadManager(3);
	}

		
	 public static void main(String[] args) {
		if (args.length < 1 || args.length > 3) {
			printUsageAndExit(1, null);
		}
		
		// load server properties from a file
		ScctProperties props = new ScctProperties();
		try {
			props.load(args[0]);
			props.loadSecurity(args[1]);
			props.setProperty("Config.home", args[1]);
		} catch (Exception ex) {
			System.err.println("Exception loading properties"+ex);
			printUsageAndExit(1, ex);
		}
		
		String timezone = props.getProperty("System.timezone");
		if (timezone != null && !timezone.equals("")) {
			TimeZone.setDefault(TimeZone.getTimeZone(timezone));
			System.out.println("Set system timezone to " +timezone);
		} else {
			TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		}

		String refTimezone = props.getProperty("System.referenceTimeZone");
		if (refTimezone != null && !refTimezone.equals("")) {
			com.citigroup.scct.util.DateUtil.setReferenceTimeZone(refTimezone);
			System.out.println("Set ref timezone to " +refTimezone);
		}
		
		String serverClass = props.getProperty("Application.class");
		if (serverClass == null)
			printAndExit(1, "No Application.class property found in " + args[0]);
		
		String JMSbroker = props.getProperty("Broker.brokerURL");
		String localHost = getLocalhostname();
		String securityCheckResponse = securityCheck(props); 
		if (!"OK".equals(securityCheckResponse)) 
			printAndExit(1, "\nSECURITY VIOLATION - " + securityCheckResponse
					+ "\nAttempting to connect to Calypso environment " + props.getProperty("CalypsoEnv.env")
					+ "\nActual host: " + getLocalhostname() + " vs. Security host: " + props.getProperty("Broker.securityHost")
					+ "\nActual broker: " + props.getProperty("Broker.brokerURL") 
					+ " vs. Security broker: " + props.getProperty("Broker.securityBroker"));
		
		System.out.println("\nConnecting to Calypso environment " + props.getProperty("CalypsoEnv.env"));
		System.out.println("Local host " + localHost + " connecting to JMS broker " + JMSbroker);
		System.out.println("Security host is " + props.getProperty("Broker.securityHost") 
				+ " and Security JMS broker is " + props.getProperty("Broker.securityBroker"));	
		server = createInstance(serverClass);
		
		if (!Util.isEmptyString(args[2]) && getId(args[2])>0) {
			server.setInstanceId(CalypsoAdapterConstants.SERVER_INSTANCE + getId(args[2]));
		} else {
			printAndExit(-1, "Unable to start CalypsoAdapter. Need to supply instance id");
		}
		server.start(props);
	} 
	 
	private static int getId(String s) {
		int id = 0;
		try {
			id = Integer.parseInt(s);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	
	protected boolean isServiceOn(String service) {
		StringBuffer buf = new StringBuffer(getInstanceId()).append(".");
		buf.append(service);
		String svc = buf.toString();
		boolean res = false; 
		
		if (properties.getProperty(svc)!=null) {
			res = Boolean.valueOf(properties.getProperty(svc));
		} else {
			buf = new StringBuffer(CalypsoAdapterConstants.GENERIC_INSTANCE).append(".");
			buf.append(service);
			res = Boolean.valueOf(properties.getProperty(buf.toString()));
		}
		return res; 
	}
	
	private static String securityCheck (ScctProperties props) {
		try{
			String actualHost = getLocalhostname();
			String actualBroker =  props.getProperty("Broker.brokerURL");
			String actualEnv = props.getProperty("CalypsoEnv.env");
			String secureBroker = props.getProperty("Broker.securityBroker");
			String secureHostList = props.getProperty("Broker.securityHost");
			String secureEnvs;
			String prodHostList = props.getProperty("Host.securityProd");
			String uatHostList = props.getProperty("Host.securityUat");
		
			if (actualHost.equals("") || actualHost == null ||
					secureBroker.equals("") || secureBroker == null ||
					secureHostList.equals("") || secureHostList == null ||
					prodHostList.equals("") || prodHostList == null ||
					uatHostList.equals("") || uatHostList == null)
					printAndExit(1, "SECURITY VIOLATION - security property missing\n"
						+ "One of the following is blank or null:"
						+ "\nAdapter host: " + actualHost
						+ "\nBroker.securityBroker: " + secureBroker
						+ "\nBroker.securityHost: " + secureHostList
						+ "\nHost.securityProd: " + prodHostList
						+ "\nHost.securityUat: " + uatHostList + "\n\n");
		
			//first check adapter is connecting to proper JMS broker and is running on a valid host
			//non prod/uat region specified, should be developer machine and default broker
			if ("localDeveloperMachine".equals(secureHostList)){
				String secureDefaultBroker = props.getProperty("Broker.defaultSecurityBroker");
				System.out.println("Got the secure default broker - " + secureDefaultBroker);
				if (!secureBroker.equals(actualBroker) || !secureBroker.equals(secureDefaultBroker))
					return "DEV BROKER MISMATCH";
				//actual host shouldn't be on prod or uat list
				if (prodHostList.indexOf(actualHost) != -1 || uatHostList.indexOf(actualHost) != -1)
					return "DEV HOST MISMATCH";
			}
			//prod/uat region was specified, must be a proper machine and brokers must match
			else if (secureHostList.indexOf(actualHost) == -1 || !secureBroker.equals(actualBroker))
				return "BROKER OR HOST MISMATCH";
			
			//now check that adapter is connecting to a proper calypso environment
			if (prodHostList.indexOf(actualHost) != -1) 
				secureEnvs = props.getProperty("CalypsoEnv.securityProd");
			else if (uatHostList.indexOf(actualHost) != -1) 
				secureEnvs = props.getProperty("CalypsoEnv.securityUat");
			else 
				secureEnvs = props.getProperty("CalypsoEnv.securityDev");
			
			if (secureEnvs.equals("") || secureEnvs == null)
				printAndExit(1, "SECURITY VIOLATION - security property missing\n"
						+ "CalypsoEnv.security[Prod|Uat|Dev] is blank or null\n");
			
			if (secureEnvs.indexOf(actualEnv) == -1)
				return "CALYPSO ENV MISMATCH\nList of valid environments for this host/broker are: " + secureEnvs;		
			
			System.out.println("Calypso environment found on list of valid envs - " + secureEnvs);
			return "OK";
			
		} catch(Exception e){
			System.out.println("Exception when reading broker/host/env properties - \n");
			e.printStackTrace();
			return "Exception when reading broker/host/env properties.";
		}
	}
	 
	private static String getLocalhostname() {
		String host = "";
		try {
			InetAddress inet = InetAddress.getLocalHost();
			host = inet.getCanonicalHostName();
		} catch (UnknownHostException e) {
			System.out.println("Unable to retrieve local host name : '" + e.getMessage());
		}
		return host;
	}

	public synchronized String getProperty(String key) {
		return getProperty(key, null);
	}

	public synchronized String getProperty(String key, String defaultValue) {
		if (properties == null)
			return defaultValue;
		return properties.getProperty(key, defaultValue);
	}

	public Properties getProperties() {
		return properties;
	}
	
	/* (non-Javadoc)
	 * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
	 */
	public void onMessage(Message message) {
		logger.debug("Received Message ");
		try {
			   threadManager.process(new ServerRunnableRequest( message, this));	
		} catch (Exception ex) {
			System.out.println("Exception " + ex);

		}
	}
	
	protected void createSessions() throws JMSException {
		try {
			// Loading initial Context
			Hashtable env = new Hashtable();
			env.put(Context.INITIAL_CONTEXT_FACTORY, properties
					.getProperty("Broker.brokerContextFactory"));
			env.put(Context.PROVIDER_URL, properties
					.getProperty("Broker.brokerURL"));
			jndiContext = new InitialContext(env);
			ConnectionFactory connectionFactory = (javax.jms.ConnectionFactory) jndiContext
					.lookup(properties
							.getProperty("Broker.brokerConnectionFactory"));

			connection = connectionFactory.createConnection();
			connection.setExceptionListener(new PAdapterUpdater());
			session = connection.createSession(false,
					javax.jms.Session.AUTO_ACKNOWLEDGE);		
			// probably need to move this to a postiniatilize method
			
			producer = session.createProducer(null);
			if (isServiceOn(CalypsoAdapterConstants.TRADE_MGMT_SERVICE)) {
				logger.debug("Starting Trade Management Service for '" + getInstanceId() + "'");
				connection.start();
			} else {
				logger.debug("Trade Management Service Not Started");
			}
		} catch (NamingException e) {
			System.out.println("Naming  Exception" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		} catch (JMSException e) {
			System.out.println("JMS Exception" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}


	/* (non-Javadoc)
	 * @see com.citigroup.scct.server.Destinations#getLogin()
	 */
	public String getLogin() {
		return properties.getProperty(Destinations.LOGIN_KEY);
	}
	/* (non-Javadoc)
	 * @see com.citigroup.scct.server.Destinations#getTradeEventTopicName()
	 */
	public String getTradeEvent() {
		return properties.getProperty(Destinations.TRADE_EVENT_KEY);
	}
	/* (non-Javadoc)
	 * @see com.citigroup.scct.server.Destinations#getTradeQuery()
	 */
	public String getTradeQuery() {
		return properties.getProperty(Destinations.TRADE_QUERY_KEY);
	}
	/* (non-Javadoc)
	 * @see com.citigroup.scct.server.Destinations#getTradeUpdateTopicName()
	 */
	public String getTradeUpdate() {
		return properties.getProperty(Destinations.TRADE_UPDATE_KEY);
	}

	protected void publishMessage(Destination dest, Message msg) throws JMSException {
		//logger.debug("publishing" + msg);
		try{
			if(dest != null){
				producer.send(dest, msg);
			}
		}
		catch(InvalidDestinationException ide){
			logger.debug("For information purposes only");
		}
	}

	/*
	protected Message createTextMessage(JAXBElement<Object> object  ) throws Exception{
		StringWriter xmlDocument = new StringWriter();
		Marshaller marshaller = ScctUtil.createMarshaller();
		marshaller.marshal(object, xmlDocument);
		Message responseMessage = session.createTextMessage(xmlDocument.toString());
		responseMessage.setStringProperty("region", region);
		responseMessage.setStringProperty("messagetype", object.getValue().getClass().getSimpleName());
		return responseMessage;
		
		
	}
	*/

	protected Message createTextMessage(JAXBElement object) throws Exception {
			StringWriter xmlDocument = new StringWriter();
			Marshaller marshaller = ScctUtil.createMarshaller();
			long x = System.currentTimeMillis();
			marshaller.marshal(object, xmlDocument);
			long y = System.currentTimeMillis();
			logger.debug("Marshalling response : " + object.getValue() + " time = " + (double)((y-x)/1000.0));

			y = System.currentTimeMillis();
			Message responseMessage = session.createTextMessage(xmlDocument.toString());
			responseMessage.setStringProperty("region", region);
			responseMessage.setStringProperty("messagetype", object.getValue().getClass().getSimpleName());
			long z = System.currentTimeMillis();
			logger.debug("Building JMS Message : " + object.getValue() + " time = " + (double)((z-y)/1000.0));
			logger.debug(object.getClass().getAnnotations().getClass().getName());
			logger.debug(object.getClass().getCanonicalName());
			return responseMessage;
	}
/*

	protected Message createTextMessage(Object object) throws Exception {
		StringWriter xmlDocument = new StringWriter();
		Marshaller marshaller = ScctUtil.createMarshaller();
		marshaller.marshal(object, xmlDocument);
		Message responseMessage = session.createTextMessage(xmlDocument.toString());
		responseMessage.setStringProperty("region", region);
		responseMessage.setStringProperty("messagetype", object.getClass().getSimpleName());
		logger.debug(object.getClass().getAnnotations().getClass().getName());
		logger.debug(object.getClass().getCanonicalName());
		return responseMessage;
	}
*/
}

