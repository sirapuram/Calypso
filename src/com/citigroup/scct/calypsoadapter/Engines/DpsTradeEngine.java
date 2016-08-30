/*
 *
 * Author: Pranay Shah
 *
 */
 

package calypsox.engine.dps;

import calypsox.tk.core.CalypsoLogUtil;

import com.calypso.engine.Engine;
import com.calypso.tk.core.Defaults;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Trade;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.marketdata.PricingEnv;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.ConnectException;
import com.calypso.tk.util.ConnectionUtil;
import com.citigroup.project.dps.DPSAdapter;
import com.citigroup.project.dps.exception.DPSSenderException;

/**
 * DPS Engine that subscribes to PSEventTrade events. 
 * The subscription is established in the database. 
 * See the script called /sql/sampleEngine.sql
 * to see how this is done.
 */

public class DpsTradeEngine extends  Engine {

    final static protected String ENGINE_NAME="DpsTradeEngine";
	protected PricingEnv _pricingEnv;
	protected String _pricingEnvName;
	
	static
	{
		CalypsoLogUtil.adjustLogCategory(ENGINE_NAME);
	}
	
	public PricingEnv getPricingEnv() { return _pricingEnv;}
	public void setPricingEnv(PricingEnv env) { _pricingEnv = env;}

    public DpsTradeEngine(DSConnection ds,String host, int port) {
		super(ds,host,port);
    }

    public String getEngineName() { return ENGINE_NAME;}

	public boolean start(boolean batch)  throws ConnectException {
	   beforeStart();
	   boolean started = super.start(batch);
	   return started;
	}
	
	private void beforeStart() 
	   throws ConnectException { 	
	
		/*
	   _pricingEnvName = getPricingEnvName();
		if( _pricingEnvName==null)
			throw new ConnectException("Can not Get PricingEnv For " + getEngineName());
		_pricingEnv =   loadPricingEnv(_pricingEnvName);
		if( _pricingEnv==null)
			throw new ConnectException("Can not Get PricingEnv " + _pricingEnvName);
			
		System.out.println("DpsTradeEngine PricingEnvName:" + _pricingEnvName);
		*/
	}
	
    /**
     * This method is called automatically every time this 
     * Engine receives an event. This method does whatever work 
     * you wish to to do in response to the new event. If your 
     * Engine processes more than one event type, you will add 
     * handling for all processed type inside this method. Every 
     * Engine must implement this method. 
     * 
     * @param event the just-received PSEvent
     */
    public boolean process(PSEvent event)    {
		boolean result = true;
		if(event instanceof PSEventTrade) {
	    	handleEvent((PSEventTrade) event);
	    	try {

				// Here, we call eventProcessed to acknowledge 
				// we have successfully consumed the event.

				getDS().getRemoteTrade().eventProcessed(event.getId(),
						    ENGINE_NAME);
	    	}
	    	catch (Exception e) {
				Log.error(Log.CALYPSOX, e);
				result = false;
	    	}
		}
		return result;
    }

    /**
     * A simple method that handles processing for trade events.
     */
    public void handleEvent(PSEventTrade event) {
		System.out.println("Received Trade " +
			   	event.toString());
		Trade trade = null;
		try {
	    	trade = event.getTrade();
			boolean tradeSaved = DPSAdapter.processData(trade);
			if (tradeSaved)
			{
			   Log.info(ENGINE_NAME, "Successfully sent Trade: " + trade.getId() + " to DPS.");
			}
		
		}
		catch (DPSSenderException ex)
		{
			Log.error(ENGINE_NAME, "Trade Not Sent To DPS, TradeId: " + trade.getId() 
					   + ". Possible cause: " +  ex.getMessage() , ex);
		}
		
		catch (Exception e) { 
			Log.error(ENGINE_NAME, "Trade Not Sent To DPS, TradeId: " + trade.getId() 
					   + ". Possible cause: " +  e.getMessage() , e);
		}
    }

    static public void main(String args[]) {

		DSConnection ds=null;

		try {
	    	ds= ConnectionUtil.connect(args,"DpsTradeEngine");
		}
		catch (Exception e) {
	    	System.out.println("Usage -env <envName> -user <UserName> " +
			       "-password <password> -o[optional]");
	    
	    	Log.error(Log.CALYPSOX, e);
	    	return ;
		}
		
		if(ds == null) {
			System.out.println("DSConnection is NULL");
			return;
		}
			
	 	String host=Defaults.getESHost();
	 	int port = Defaults.getESPort();

	 	DpsTradeEngine engine = new DpsTradeEngine(ds,host,port);
	 	try {
			engine.start(true);
			//DPSAdapter.initArgs(args, ds, engine.getPricingEnv());
			DPSAdapter.initArgs(args, ds, null);
			System.out.println("DpsTradeEngine started....");
		}
		catch(ConnectException ce) {
			System.out.println("DpsTradeEngine: Cannot get PricingEnv, exiting...");
			Log.error(Log.CALYPSOX, ce);
			System.exit(-1);
		}
	 	catch(Exception ee) {
			Log.error(Log.CALYPSOX, ee);
	     	System.exit(-1);
	 	}
    } // main ends
}
