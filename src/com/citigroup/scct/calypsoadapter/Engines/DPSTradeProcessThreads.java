/*
 *
 * Author: Ilona Kardon
 *
 */
 

package calypsox.engine.dps;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.Trade;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.service.DSConnection;
import com.citigroup.project.dps.DPSAdapter;
import com.citigroup.project.dps.exception.DPSSenderException;
import com.calypso.engine.Engine;
/**
 * Threads to process Trades from DPS Engine 
 * 
 */

	  
public class DPSTradeProcessThreads implements Runnable {
		
		  public final String tName;
		  PSEvent event=null;
		  DSConnection ds=null;
		  Engine engine = null;
		  String engineName = null;
		  Thread dpsTradeProcessThread;
		  PSEventTrade trade;
		  EventDataHolder data = null;
		  int tradeId;
		  private static final String CLASSNAME = "DPSTradeProcessThreads";
	
		  public DPSTradeProcessThreads(String threadName, EventDataHolder data, MultiThreadDpsTradeEngine engine) {
			     tName = threadName;
			     dpsTradeProcessThread = new Thread (this);
			     this.engine = engine;
			     this.data=data;
			     this.engineName=engine.getEngineName();
			 	 dpsTradeProcessThread.start();
			  }
		  
		public void run() {
			while(true)
			{
				event = data.get();
				if (event!=null)
				{
			        try {
				 		if(event instanceof PSEventTrade) {
				 			trade=(PSEventTrade)event;
							tradeId = trade.getTrade().getId();
							Log.debug(CLASSNAME,"Thread "+tName+" Processing trade "+tradeId+". With Event ID: " + event.getId());
							//System.out.println("Thread "+tName+" Processing trade "+tradeId+". With Event ID: " + event.getId());
					    	handleEvent((PSEventTrade) event);	
							//Removing trade ids from processing trade id vector
					    	try{
					    		 int eventId = event.getId();
					    		 Log.info(CLASSNAME,"About to set event Id "+eventId+" status to PROCESSED...");
					    		 //System.out.println("About to set event Id "+eventId+" status to PROCESSED...");
					    		 engine.getDS().getRemoteTrade().eventProcessed(eventId, engineName);
								 boolean status = data.monitorRemove(tradeId);
								 if(status){
									 Log.info(CLASSNAME,"Removed Trade "+tradeId+" from processing queue successfully..");
									 //System.out.println("Removed Trade "+tradeId+" from processing queue successfully..");
								 }else
								 {
									 Log.error(CLASSNAME,"Removing Trade "+tradeId+" from processing queue Failed.. | <------");
									 //System.out.println("Removing Trade "+tradeId+" from processing queue Failed.. | <------");
								 }
					    		 Log.debug(CLASSNAME,"Finished processing Event ID" + event.getId()+" Thread Name: "+tName);
					    		 //System.out.println("Finished processing Event ID" + event.getId()+" Thread Name: "+tName);
					    	}catch (Exception e) {
								Log.error(CLASSNAME,"Exception in "+tName+" while setting event to PROCESSED status.."+e);
					    	}
						}
				     }catch (DPSSenderException dpsEx)
				     {
				    	 if (DPSAdapter.DPSErrorFlag) {
				    		 Log.error(CLASSNAME,"Error sending deals down to DPS/OASYS.");
				    	 	Log.error(CLASSNAME," ******** ABORTING ENGINE ********* ");
				    	 	System.exit(1);
				    	 }
				     }
			        catch (Exception e ) {
				    	 Log.error(CLASSNAME,"Exception in "+tName+" while processing the event.."+e);
				     }
				}else{
					Log.info(CLASSNAME,tName+" Going to sleep...");
					//System.out.println(tName+" Going to sleep...");
				}
			}
		}// run
		  
	   public void handleEvent(PSEventTrade event) {
		   			//Log.info(CLASSNAME,"Received Trade " +event.toString());
					Trade trade = null;
					try {
				    	trade = event.getTrade();		
						boolean tradeSaved = DPSAdapter.processData(trade);
						if (tradeSaved)
						{
							Log.info(CLASSNAME,"Successfully sent Trade: " + trade.getId() + " to DPS.");
						}
					}catch (DPSSenderException ex)
					{
						Log.error(CLASSNAME,"Trade Not Sent To DPS, TradeId: " + trade.getId() 
								   + ". Possible cause: " +  ex.getMessage());
						throw ex;
					}
					catch (Exception e) { 
						Log.error(CLASSNAME,"Trade Not Sent To DPS, TradeId: " + trade.getId() 
								   + ". Possible cause: " +  e.getMessage());
					}
	    }//handleEvent
   
  }//Class defn
	



