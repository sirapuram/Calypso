package calypsox.tk.event;

import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;

import calypsox.apps.trading.DomainValuesUtil;
import calypsox.apps.trading.TradeKeywordKey;
import calypsox.tk.bo.workflow.rule.BackToBackTradeRule;
import calypsox.tk.core.CalypsoLogUtil;
import calypsox.tk.core.CitiCustomTradeData;
import calypsox.tk.core.DealCoverCustomData;
import calypsox.tk.product.GlobalMirrorHandler;
import calypsox.tk.refdata.DomainValuesKey;

import com.calypso.tk.core.Defaults;
import com.calypso.tk.core.LegalEntity;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.core.sql.TradeSQL;
import com.calypso.tk.core.sql.ioSQL;
import com.calypso.tk.event.EventFilter;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.TradeArray;

/**
 * DPSEventFilter : Event Filter for DpsTradeEngine.
*
*/

public class DefaultDPSEventFilter implements EventFilter {
	
	private static String CLASSNAME="DefaultDPSEventFilter";
	private final String strNYBookLocation = "New York";
	private final String strLNBookLocation = "London";
	// Added by pp47347 - To avoid the trades to flow to DPS based on Books	
	boolean ignoreBooksListForDPSSwitch = 
	    DomainValuesUtil.checkDomainValue(DomainValuesKey.IGNORE_BOOKS_LIST_FOR_DPS_SWITCH, "true", true);
	// End pp47374 Changes
	static
	{
		CalypsoLogUtil.adjustLogCategory(CLASSNAME);
	}
	
	public DefaultDPSEventFilter()
	{	
		Log.debug(CLASSNAME,"Initialized DefaultDPSEventFilter");
	}
	
    public boolean accept(PSEvent event) {

    	if(event instanceof PSEventTrade) {
    		
        	PSEventTrade et = (PSEventTrade)event;
			Trade trade = et.getTrade();
			String zsBookLocation=trade.getBook().getAttribute("CITI_Location");
			Vector bookLocation1 = null;
			Vector bookLocation2 = null;
			
			try {
				bookLocation1 = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.NCD_DPS_EVENT_FILTER_BOOK);
				bookLocation2 = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.EMEA_DPS_EVENT_FILTER_BOOK);
				if((Util.isEmptyVector(bookLocation1)) || (Util.isEmptyVector(bookLocation2))){
					Log.warn(CLASSNAME,"Domain Values are empty.. Using the default values...");
					bookLocation1 = getDefaultNYBookLocation();
					bookLocation2 = getDefaultEMEABookLocation();
				}
			}
			catch (Exception e) { 
				Log.error(CLASSNAME,e.getMessage());
			}

			if (!validate(et))
			{
				return false;
			}				

			if (bookLocation1.contains(zsBookLocation) || bookLocation2.contains(zsBookLocation)) {
				Log.debug(CLASSNAME,"Trade " + trade.getId() + " is not being sent by this engine. Engine book is "+ bookLocation1 +"; Trade Book is "+zsBookLocation);
				return false;
			}	
			Log.debug(CLASSNAME, " Trade " + trade.getId() +" with Book "+ zsBookLocation+" is being processed by DefaultDPSEventFilter ");

        	return true;
    	}
    	else return true;
    }
    
    public boolean validate (PSEventTrade et)
    {
      		boolean status=true;

  			Vector includeStatus = null;
  			Vector ignoreProducts = null;
  			Vector ownerSystem = null;  			
  			Trade trade = et.getTrade();
  			//Added by pp47347 - To avoid the trades to flow to DPS based on Books
  			Vector ignoreBooks = null;
  			try {
  				includeStatus = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.INCLUDE_TRADE_STATUS);
  				ignoreProducts = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_PRODUCTS_FROM_BOFEED);
  				ownerSystem = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_EXTERNAL_SYSTEM);
  				
  				// Added by pp47347 - To avoid the trades to flow to DPS based on Books
  				ignoreBooks = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_BOOKS_LIST_FOR_DPS);
  			        Log.info(CLASSNAME, "pp47347 :  ignoreBooks : "+ignoreBooks);
  			        //End pp47347 Changes
  			}
  			catch (Exception e) { 
  				Log.error(CLASSNAME,e.getMessage());
  			}
  			
  			if (includeStatus != null && !includeStatus.contains(et.getStatus().getStatus())) {

  				Log.debug(CLASSNAME,"Trade " + trade.getId() + " status is " + et.getStatus().toString() + ", not sending it to DPS");
  				return false;
  			}
  			
  			String tradeOwner = trade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
  			if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {

  				Log.debug(CLASSNAME,"Trade " + trade.getId() + " Owner is " + tradeOwner + ", not sending it to DPS");
  				return false;
  			}

  			if (ignoreProducts != null && ignoreProducts.contains(trade.getProductType())) {
  				Log.debug(CLASSNAME,"Trade " + trade.getId() + " product is " + trade.getProductType() + ", not sending it to DPS");
  				return false;
  			}
  			
  			if (isB2bOrMirrorEvent(et)) {
  				Log.debug(CLASSNAME, "Skipping: " + et.getId() + " trade id: " + trade.getId());
  				return false;
  			}
  			
  			// Added by pp47347 - To avoid the trades to flow to DPS based on Books  			
  			if(ignoreBooksListForDPSSwitch) {
  			    if(ignoreBooks !=null && ignoreBooks.size() > 0) {
  				String bookFromTrade = trade.getBook().getName();
  				Log.info(CLASSNAME, "pp47347 : bookFromTrade : "+bookFromTrade);
  				if(ignoreBooks.contains(bookFromTrade)) {
  				    Log.info(CLASSNAME,"Trade " + trade.getId() + " with book " + bookFromTrade + " is not allowed to send the trade details to down Stream, not sending it to DPS");
  				    return false;
  				}
  			    }
  			}
  			// End pp47347 changes
   		
      	return status;
    }
    
    public boolean isB2bOrMirrorEvent(PSEventTrade et) {
    	boolean status = false;
		Trade trade = et.getTrade();
		DealCoverCustomData dcCustomData = null;
		Connection conn = null; 
		boolean newConnection = false;
		
		if((CitiCustomTradeData)trade.getCustomData() != null) {
			dcCustomData = ((CitiCustomTradeData)trade.getCustomData()).
                                            getDealCoverCustomData();
		}
		
		if(dcCustomData != null) {

			Log.info(CLASSNAME, " Oasys DealId: " + dcCustomData.getDealID() + " and tradeId: " + trade.getId() + " bunlde info "+trade.getBundle() + "event id "+et.getId());
			
			if(trade.getBundle() != null && trade.getBundle().getType().equals(BackToBackTradeRule.BACK_TO_BACK) &&
				!trade.getRole().equals(LegalEntity.COUNTERPARTY)) {

				Log.warn(CLASSNAME, "BTB firm trade, skip PSEventTrade: " +
					    dcCustomData.getDealID() + "." +
					    dcCustomData.getTransactionID());

				status = true;
			} else if (trade.getBundle() != null &&	trade.getBundle().getType().equals(GlobalMirrorHandler.BUNDLE_NAME)) {
				try {
					TradeArray array = new TradeArray();
					boolean flag = false;
					try {
						if (!connectIoSql()) {
							throw new RemoteException("Unable to connect to database server");
						}
						else {
							if (conn == null) {
								conn = ioSQL._dbServer.getClientThreadConnection(Thread.currentThread());
								if (conn == null) {
									conn = ioSQL.newConnection();
									newConnection = true;
								}
								flag = conn.isReadOnly();
								ioSQL.setReadOnly(conn, true);
							}
							TradeSQL.getBundleTrades(array, trade.getBundle().getId(), conn);
						}
					} catch (Exception e) {
						Log.error(CLASSNAME, "Exception in isB2BorMirrorEvent..... " + e.getMessage());
						throw e;
					} finally {
						if (newConnection && conn != null) {
							ioSQL.setReadOnly(conn, flag);
							ioSQL.releaseConnection(conn);
						}
					}

					Trade mainTrade = (Trade) array.get(0);
					Trade mirrorTrade = (Trade) array.get(1);

					Log.info(CLASSNAME, " DealID: " + dcCustomData.getDealID()
							 + ", tradeId: " + trade.getId()
							 + " are part of a Bundle."
							 + " Main tradeId = " + mainTrade.getId()
							  + ", Mirror tradeId = " + mirrorTrade.getId()); 
					
					if(((CitiCustomTradeData)mainTrade.getCustomData() != null) &&
						((CitiCustomTradeData)mirrorTrade.getCustomData() != null)) {

						DealCoverCustomData mainDCCustomData =
							((CitiCustomTradeData)mainTrade.getCustomData()).getDealCoverCustomData();
						DealCoverCustomData mirrorDCCustomData =
							((CitiCustomTradeData)mirrorTrade.getCustomData()).getDealCoverCustomData();

						Log.info(CLASSNAME, "DealID: " + dcCustomData.getDealID()
								 + " - Checking we're not sending the event for mirror trade");
						
						if(mainDCCustomData != null && mirrorDCCustomData != null) {
							int mainTradeTxnID = mainDCCustomData.getTransactionID();
							int mirrorTradeTxnID = mirrorDCCustomData.getTransactionID();
							int maxTransactionID = 0;

							Log.info(CLASSNAME, " DealID: " + dcCustomData.getDealID()
									 + " - Main trade (" + mainTrade.getId() + ") transactionId is: " + mainTradeTxnID
									 +  ", Mirror trade (" + mirrorTrade.getId() + ") transactionId is: " + mirrorTradeTxnID); 	
							
							if(mainTradeTxnID < mirrorTradeTxnID) {
								maxTransactionID = mirrorTradeTxnID;
							}
							else {
								maxTransactionID = mainTradeTxnID;
							}

							if(dcCustomData.getTransactionID() == maxTransactionID) {
								Log.info(CLASSNAME, "Do not send this trade event to DPS: " + trade.getId() 
										+ " Deal: " + dcCustomData.getDealID()
										+ ":" + dcCustomData.getTransactionID());
								status = true;
							}
						}
					}
				}catch (Exception e) {
					Log.error(CLASSNAME, " For trade: " + trade.getId() 
							+ ", could not get trades by bundle id " + trade.getBundle().getId(), e);
					status = false;
				}
			} else if((trade.getBundle()!=null && trade.getBundle().getId()==0)||trade.getBundle()==null ) {
				Log.debug(CLASSNAME, "Event id: " + et.getId() + " Trade id: " + trade.getId() + " isB2BMirror : " + trade.getKeywordValue(TradeKeywordKey.ISBTBMIRRIOR));
				if(trade.getKeywordValue(TradeKeywordKey.ISBTBMIRRIOR)!=null)
				{
					status = true;
				}else{
					status = false;
				}
			}
		} else {
			status = true;
		}
    	return status;
    }
    
    public static boolean connectIoSql() {
        String driver = Defaults.getProperty(Defaults.DRIVER);
        String url = Defaults.getProperty(Defaults.DBURL);
        String dbuser = Defaults.getProperty(Defaults.DBUSER);
        String dbpasswd = Defaults.getProperty(Defaults.DBPASSWORD);
        boolean status = true;
        try {
            if (ioSQL._dbServer == null) {
                ioSQL.connect(driver, url, dbuser, dbpasswd, null);
            }
            else {
                System.out.println("Connection already made.");
            }
        }
        catch (SQLException sqle) {
            System.out.println("SQLException " + sqle.getMessage());
            sqle.printStackTrace();
            status = false;
        }
        catch (Exception e) {
            System.out.println("Exception " + e.getMessage());
            e.printStackTrace();
            status = false;
        }

        return status;
    }
    
   	public Vector getDefaultNYBookLocation(){
   		Vector vec = new Vector(1);
   		vec.add(strNYBookLocation);
   		return vec;
   	}

   	public Vector getDefaultEMEABookLocation(){
   		Vector vec = new Vector(1);
   		vec.add(strLNBookLocation);
   		return vec;
   	}
   	
}
