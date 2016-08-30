package com.citigroup.scct.calypsoadapter;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.xml.bind.JAXBElement;
import org.apache.log4j.Logger;

import calypsox.tk.product.CitiCreditSyntheticCDO;
import calypsox.tk.product.CitiPortfolioCDO;
import calypsox.tk.product.CitiPortfolioCDO2;
import calypsox.tk.product.CitiPortfolioIndex;
import calypsox.tk.refdata.DomainValuesKey;
import calypsox.tk.service.CitiPortfolioServer;
import calypsox.tk.service.RemoteCitiPortfolio;
import calypsox.tk.event.PSEventCitiPortfolio;

import com.calypso.tk.product.CDSABSIndex;
import com.calypso.tk.product.CreditDefaultSwap;
import com.calypso.tk.product.CreditDefaultSwapABS;
import com.calypso.tk.util.Timer;
import com.calypso.tk.util.TimerRunnable;
import com.calypso.tk.bo.workflow.TradeWorkflow;
import com.calypso.tk.core.Book;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Product;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.event.ESStarter;
import com.calypso.tk.event.PSConnection;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.event.PSSubscriber;
import com.calypso.tk.service.ConnectionListener;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.service.LocalCache;
import com.citigroup.scct.calypsoadapter.converter.ConverterFactory;
import com.citigroup.scct.calypsoadapter.monitor.TradeMonitorMBean;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.ScctTradeEventType;
import com.citigroup.scct.cgen.ScctTradeType;
import com.citigroup.scct.cgen.ScctPortfolioType;
import com.citigroup.scct.util.ScctUtil;
import com.citigroup.scct.util.StringUtilities;

import calypsox.apps.trading.DomainValuesUtil;
import calypsox.apps.trading.TradeKeywordKey;

/**
 * @author kt60981
 *
 */
/*
 * Change History (most recent first):
        12/14/2010  ss78999     :   Added for trades not to send to downstream system based on the book, startegy and product.
 		08/11/2008	kt60981		: 	Update retry logic.
		07/21/2008	kt60981		:	Update event handling for reconciler thread
		07/1/2008	kt60981		:	Refactor Event Handling for B2B Mirror, Sales, Allocated Trade
		05/13/2008	kt60981		:	Added Exception Handler for ABS/ABX configuration
*/

public class TradeEventHandler implements PSSubscriber , ConnectionListener {

	private PSConnection ps;
	private DSConnection ds;
	private ObjectFactory factory;
	private com.calypso.tk.util.Timer psTimer;
	
	private  final Logger logger = Logger.getLogger(getClass().getName());
	private TradeEventReconciler reconciler;
	private TradeHandler cdsHandler;
	private TradeHandler cdoHandler;
	private TradeHandler absHandler;
	private TradeHandler abxHandler;
	private boolean reconcile;
	private TradeMonitorMBean tradeMBean;
	private Properties props;
	private Map supportedProducts;
	
	private final String CDS = "Product.CreditDefaultSwap";
	private final String CitiCDO = "Product.CitiCreditSyntheticCDO";
	private final String CDSABS= "Product.CreditDefaultSwapABS";
	private final String ABX= "Product.CDSABSIndex";
	
	private boolean disconnected = false;
	private int retry = 1;
	private int MAX_RETRY = 10;
	private int RECONNECT_TIMEOUT = 10;
	//START-ss78999
	private static final String IGNORE_BOOKS_LIST_FOR_DSP_SWITCH = "IgnoreBooksListForDSPSwitch";  
	private static final String IGNORE_BOOK_LIST_FOR_DSP = "IgnoreBooksListForDSP";
	private static final String IGNORE_PRODUCT_LIST_FOR_DSP = "IgnoreProductsListForDSP";
	private static final String IGNORE_STRATEGY_LIST_FOR_DSP = "IgnoreStrategiesListForDSP";
	private static final String CITI_STRATEGY = "CITI_Strategy";
	//End
	
	public TradeEventHandler(DSConnection ds_, boolean reconcile, TradeMonitorMBean tradeMBean, Properties properties) throws Exception {
		ds = ds_;
		factory = new ObjectFactory();
		cdsHandler = new CDSTradeEventHandler(ds);
		cdoHandler = new CDOTradeEventHandler(ds);
		absHandler = new CDSABSTradeEventHandler(ds);
		abxHandler = new ABXTradeEventHandler(ds);
		this.reconcile = reconcile;
		this.tradeMBean = tradeMBean;
		props = properties;
		if (this.reconcile) {
		  // Enable validation based upon props
		  if (validate(CalypsoAdapterConstants.VALIDATE_EVENT)) {
		    reconciler = new TradeEventReconciler(ds, true);
		  } else {
			reconciler = new TradeEventReconciler(ds, false);
		  }
		}		
		supportedProducts = new HashMap();
		initSupportedProduct();
		initReconnectProperty();
	}
	
	private void initSupportedProduct() throws Exception {
		try {
			if (props !=null) {
				isSupported(CalypsoAdapter.getAdapter().getInstanceId(), CDS);
				isSupported(CalypsoAdapter.getAdapter().getInstanceId(), CitiCDO);
				isSupported(CalypsoAdapter.getAdapter().getInstanceId(), CDSABS);
				isSupported(CalypsoAdapter.getAdapter().getInstanceId(), ABX);
			}
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e.getMessage(), e);
		}
	}
	
	private void isSupported(String instanceId, String product) {
		StringBuffer buf = new StringBuffer(instanceId);
		String key = buf.append(".").append(product).toString();
		String val = (String) props.getProperty(key);
		if (!Util.isEmptyString(val)) {
			if ("true".equalsIgnoreCase(val)) {
				supportedProducts.put(product, "TRUE");
			} else {
				supportedProducts.put(product, "FALSE");
			}
		} else {
			buf = new StringBuffer(CalypsoAdapterConstants.GENERIC_INSTANCE).append(".");
			buf.append(product);
			key = buf.toString();
			val = (String) props.getProperty(key);
			if (!Util.isEmptyString(val) && "true".equalsIgnoreCase(val)) {
				supportedProducts.put(product, "TRUE");
			} else {
				supportedProducts.put(product, "FALSE");
			}
		}
	}
	
	private void initReconnectProperty() throws Exception {
		String tmp = props.getProperty(CalypsoAdapterConstants.MAX_RETRY);
		if (!Util.isEmptyString(tmp)) {
			try {
				MAX_RETRY = Integer.parseInt(tmp);
			} catch (RuntimeException e) {
				logger.warn("Unable to parse '"
						+ CalypsoAdapterConstants.MAX_RETRY + "' " + tmp);
			}
		}
		tmp = props.getProperty(CalypsoAdapterConstants.RECONNECT_TIMEOUT);
		if (!Util.isEmptyString(tmp)) {
			try {
				RECONNECT_TIMEOUT = Integer.parseInt(tmp);
			} catch (RuntimeException e) {
				logger.warn("Unable to parse '"
						+ CalypsoAdapterConstants.RECONNECT_TIMEOUT + "' "
						+ tmp);
			}
		}
	}

	public void start() throws Exception{
		connectToPSServer(false);
		startRecon();
	}
	
	
	private void startRecon() throws Exception {
		if (reconcile) {
			Thread t = new Thread(reconciler);
			t.start();
	      	t.join();
		}
		logger.debug("Reconciler thread completed : ");
	}
	
	boolean connectToPSServer(boolean wait) {
		try {
			ps = ESStarter.startConnection(ds, this);
			if (ps == null)
				return false;
			ps.start(wait);
			subscribe();
			return true;
		} catch (Exception e) {
			logger.error("Caught Exception connecting to Event Server", e);
			return false;
		}
	}
	
	public void subscribe() throws Exception{
		Vector events = new Vector();
	    events.addElement(PSEventTrade.class.getName());
	    events.addElement(PSEventCitiPortfolio.class.getName());
	    ps.subscribe(events);	
	}

	/* (non-Javadoc)
	 * @see com.calypso.tk.event.PSSubscriber#newEvent(com.calypso.tk.event.PSEvent)
	 */
	public void newEvent(PSEvent event) {
		try {
			if (event instanceof PSEventTrade) {
				PSEventTrade tradeEvent = (PSEventTrade) event;
				Trade trade = tradeEvent.getTrade();
                 if (!accept(trade)) {
					return;
				}
				
				Product product = trade.getProduct();
				
				// By default, publish all trades, however in the case where these properties are enabled
				// do not publish trades with these criteria
				// [ sales | B2B mirror | allocated trade ]
				
				if (validate(CalypsoAdapterConstants.VALIDATE_EVENT)) {
					if (AdapterUserProfile.isSales(trade.getEnteredUser())) {
						logger.debug("Ignoring trade event on sales trade "
								+ trade.getId() + " entered by "
								+ trade.getEnteredUser());
						return;
					}
					if (ScctUtil.isBTBMirror(trade)) {
					  logger.debug("Ignoring non-main back2back [" + trade.getId() + "]");
					  return;
					}
					if (!ScctUtil.isPrimaryAllocatedTrade(trade)) {
						logger.debug("Ignoring allocated child trade [" + trade.getId() + "]");
						return;
					}
				}
				
				if (product instanceof CitiCreditSyntheticCDO) {
					cdoHandler.handle(trade);
				} else if (product instanceof CreditDefaultSwap) {
					cdsHandler.handle(trade);
				} else if (product instanceof CreditDefaultSwapABS) {
					absHandler.handle(trade);
				} else if (product instanceof CDSABSIndex) {
					abxHandler.handle(trade);
				} 
			} else if (event instanceof PSEventCitiPortfolio) {
				handlePortfolioEvent((PSEventCitiPortfolio) event);
			} else { // unknown event
				// this shouldn't happen since we are only listenening for trade
				// and portfolio events
				logger.debug("event ignored: " + event.toString());
				return;
			}

		} catch (Exception ex) {
			logger.error("Exception " + ex);
			ex.printStackTrace();
		}
	}
	
	public void handleBulkTradeEvents(String productType, Vector<Trade> trades) {
		try {
			if (Product.CREDITDEFAULTSWAP.equals(productType)) {
				cdsHandler.handleBulkTradeEvents(trades);
			} else if (CitiCreditSyntheticCDO.PRODUCT_TYPE.equals(productType)) {
				cdoHandler.handleBulkTradeEvents(trades);
			} else {
				logger.debug("Unsupported product type : " + productType);
			}
		} catch (Exception e) {
			logger.warn("Caught Exception processing bulk trade events : product " + productType);
		}
	}

	private boolean accept(Trade trade) {
		boolean result = false; 
		boolean ignoreBooksListForDSPSwitch = DomainValuesUtil.checkDomainValue(IGNORE_BOOKS_LIST_FOR_DSP_SWITCH,"true", true);//Added-ss78999
		
		if (trade !=null) {
			Product product = trade.getProduct();
			if (product instanceof CreditDefaultSwap && "TRUE".equals(supportedProducts.get(CDS))) {
				result = true;
			} else if (product instanceof CitiCreditSyntheticCDO && "TRUE".equals(supportedProducts.get(CitiCDO))) {
				result = true;
			} else if (product instanceof CreditDefaultSwapABS && "TRUE".equals(supportedProducts.get(CDSABS))) {
				result = true;
			} else if (product instanceof CDSABSIndex && "TRUE".equals(supportedProducts.get(ABX))) {
				result = true;
			} else {
				logger.debug("Non Supported Product ' " + (product !=null ? product.getType() : null)  + "' ignored " + trade.getId());
			}
			//Added -ss78999
			if(ignoreBooksListForDSPSwitch)
			{
				Book book = trade.getBook();
				String bookName = book.getName();
				String strategy = book.getAttribute(CITI_STRATEGY);
				Vector vBookList = LocalCache.getDomainValues(DSConnection.getDefault(), IGNORE_BOOK_LIST_FOR_DSP);
				Vector vStrategiesList = LocalCache.getDomainValues(DSConnection.getDefault(), IGNORE_STRATEGY_LIST_FOR_DSP);
				Vector vProductList = LocalCache.getDomainValues(DSConnection.getDefault(), IGNORE_PRODUCT_LIST_FOR_DSP);
				if(!Util.isEmptyVector(vBookList) && vBookList.contains(bookName))
				{
				  logger.debug("Trade " + trade.getId() + " with book " + bookName + " is not allowed to send the trade details to down Stream, not sending it to DSP");
				  result = false;
				}
				if(!Util.isEmptyVector(vStrategiesList) && vStrategiesList.contains(strategy))
				{
					logger.debug("Trade " + trade.getId() + " with book Strategy " + strategy + " is not allowed to send the trade details to down Stream, not sending it to DSP");
					result = false;
				}
				if(!Util.isEmptyVector(vProductList) && vProductList.contains(trade.getProductType()))
				{
					logger.debug("Trade " + trade.getId() + " with Product " + trade.getProductType() + " is not allowed to send the trade details to down Stream, not sending it to DSP");
					result = false;
				}
			}
			//End
		}
		return result;
	}
	
	private boolean validate(String criteria) {
		boolean result = false;
		try {
			result = Boolean.valueOf(props.getProperty(criteria));
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public void handlePortfolioEvent(PSEventCitiPortfolio portfolioEvent)
			throws Exception {
		logger
				.debug("Portfolio Event generated:  "
						+ portfolioEvent.toString());
		if (PSEventCitiPortfolio.SAVE_ACTION.equals(portfolioEvent.getAction())) {
			RemoteCitiPortfolio rmCitiPortfolio = (RemoteCitiPortfolio) ds
					.getRMIService(CitiPortfolioServer.NAME);

			String type = portfolioEvent.getPortfolioType();
			long id = portfolioEvent.getPortfolioId();
			ScctPortfolioType portfolio = null;
			if (CitiPortfolioCDO.TYPE.equals(type)) {
				CitiPortfolioCDO cdo = rmCitiPortfolio.getCdoPortfolio(id);
				portfolio = ConverterFactory.getInstance()
						.getPortfolioConverter().convertFromCalypso(cdo);

			} else if (CitiPortfolioCDO2.TYPE.equals(type)) {
				CitiPortfolioCDO2 cdo2 = rmCitiPortfolio.getCdo2Portfolio(id);
				portfolio = ConverterFactory.getInstance()
						.getPortfolioConverter().convertFromCalypso(cdo2);
			} else if (CitiPortfolioIndex.TYPE.equals(type)) {
				CitiPortfolioIndex index = rmCitiPortfolio
						.getIndexPortfolio(id);
				portfolio = ConverterFactory.getInstance()
						.getPortfolioConverter().convertFromCalypso(index);
			} else
				logger.error("Unknown portfolio type on portfolio event: "
						+ type);

			CalypsoAdapter.getAdapter().handlePortfolioEvent(portfolio);
		}
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.calypso.tk.event.PSSubscriber#onDisconnect()
	 */
	public void onDisconnect() {
		// TODO Auto-generated method stub
		logger.debug("Disconnected from Event Server");
		internalPSStop();
		//startPSTimer();
		// update retry logic
		reconnectTimer();
	}

	 protected void stopPSTimer() {
		if (psTimer == null)
			return;
		logger.debug("Stopping Timer to reconnect to EventServer");
		com.calypso.tk.util.Timer timer = psTimer;
		psTimer = null;
		timer.stop();
	}
	
	 protected int getTimeoutReconnect() {
		return ds.getTimeoutReconnect();
	}
	  
	 protected void startPSTimer() {
		if (psTimer != null)
			return;
		logger.debug("Starting Reconnect Timer");
		TimerRunnable r = new TimerRunnable() {
			public void timerRun() {
				if (ds == null || ds.isClosed()) {
					logger.debug("PSTimer Not connected to DS skip try");
					return;
				}
				logger.debug( "PSTimer trying reconnect to ES");
				internalPSStop();
				boolean v = connectToPSServer(false);
				if (v) {
					logger.debug( "PSTimer Reconnected to ES");
					stopPSTimer();
				}
			}
		};
		psTimer = new com.calypso.tk.util.Timer(r, getTimeoutReconnect());
		psTimer.start();
	}
	 
    protected void startPSTimer2() {
	  if (psTimer != null) {
		return;
	  }
	  
	  logger.debug("Starting Reconnect Timer");

		// If disconnected from ES, attempt to retry MAX_RETRY if DSconn is
		// available, else shutdown
		// If DS is not available, shutdown as well.
	  TimerRunnable r = new TimerRunnable() {
		  public void timerRun() {
			if (ds == null || ds.isClosed()) {
			  logger.error("PSTimer Not connected to DS skip try");
			  return;
			}
			
			logger.debug("PSTimer trying reconnect to ES");
			internalPSStop();
			if (retry <= MAX_RETRY) {
			  logger.debug("PSTimer retrying for " + retry + " shot.");
			  boolean v = connectToPSServer(false);
			  if (v) {
				retry = 1;
				disconnected = false;
				logger.debug("PSTimer Reconnected to ES");
				stopPSTimer();
				try {
					startRecon();
				} catch (Exception e) {
					logger.warn("Reconciliation Exception : '" + e.getMessage() + "'", e);
				}
			  } else {
				retry++;
			  }
			} else {
				psTimer.stop();
				logger.warn("Retry Limit Exceeded [" + MAX_RETRY + "].....Shutting down Adapter....");
				System.exit(-1);
			}
		  }
		};
		psTimer = new com.calypso.tk.util.Timer(r, getTimeoutReconnect());
		psTimer.start();
	}
     
	 protected void internalPSStop() {
		if (ps != null){
			try {
				logger.warn("Attempting to stop psConn");
				disconnected = true;
				ps.stop();
				logger.warn("STOPPED psConn");
			} catch (Exception e) {
				logger.warn("Disconnect client from Event Server", e);
			}
		}
	}

	protected boolean isReconRunning() {
		return (reconciler !=null ? reconciler.isRunning() : false);
	}

	private void reconnectTimer() {
		logger.debug( "Manual Timer trying reconnect to ES");
		boolean done = false;
		int attempt = 1;
		
		while (attempt <= MAX_RETRY && !done) {
			logger.debug( "Manual Timer retrying for " + attempt + " shot.");
			if (ds == null || ds.isClosed()) {
				logger.error("Manual Timer Not connected to DS skip try");
				logger.fatal("Disconnected from DataServer....Shutting down Adapter....");
				System.exit(-1);
			}
			boolean v = connectToPSServer(false);
			if (v) {
				logger.debug("Manual Timer Reconnected to ES");
				disconnected = false;
				try {
					startRecon();
				} catch (Exception e) {
					logger.warn("Reconciliation Exception : '" + e.getMessage() + "'", e);
				} finally {
					done = true;
				}
			} else {
			    attempt++;
			    try {
					Thread.currentThread().sleep(RECONNECT_TIMEOUT*1000);
				} catch (InterruptedException e) {
					logger.warn("Reconnect thread interrupted : '" + e.getMessage() + "'", e);
				}
			}
		}
		
		if (attempt > MAX_RETRY) {
			logger.fatal("Retry Limit Exceeded [" + MAX_RETRY + "].....Shutting down Adapter....");
			System.exit(-1);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.calypso.tk.service.ConnectionListener#connectionClosed(com.calypso.tk.service.DSConnection)
	 */
	public void connectionClosed(DSConnection arg0) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#reconnected(com.calypso.tk.service.DSConnection)
	 */
	public void reconnected(DSConnection arg0) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#tryingBackup(com.calypso.tk.service.DSConnection)
	 */
	public void tryingBackup(DSConnection arg0) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#usingBackup(com.calypso.tk.service.DSConnection)
	 */
	public void usingBackup(DSConnection arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public boolean isESDisconnected() {
		return disconnected;
	}
}
