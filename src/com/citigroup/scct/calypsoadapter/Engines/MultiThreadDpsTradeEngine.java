/*
 *
 * Author: Ilona Kardon
 *
 */
 
package calypsox.engine.dps;

import calypsox.tk.core.CalypsoLogUtil;

import com.calypso.engine.Engine;
import com.calypso.tk.core.Defaults;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Util;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.marketdata.PricingEnv;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.ConnectException;
import com.calypso.tk.util.ConnectionUtil;
import com.citigroup.project.dps.DPSAdapter;
import com.citigroup.project.util.oasys.OasysUtil;
import java.lang.reflect.Constructor;

/**
 * DPS Engine that subscribes to PSEventTrade events. 
 * The subscription is established in the database. 
 * See the script called /sql/sampleEngine.sql
 * to see how this is done.
 */

public abstract class MultiThreadDpsTradeEngine extends  Engine {
	  
	protected String ENGINE_NAME=null;
	protected PricingEnv _pricingEnv;
	protected String _pricingEnvName;
	DSConnection ds=null;
	static MultiThreadDpsTradeEngine engine = null;
	private EventDataHolder data = null;
	public static final String THREADS_NUMBER = "-threads";
	public static final String CLASSNAME = "MultiThreadDpsTradeEngine";
	

	static
	{
		CalypsoLogUtil.adjustLogCategory(CLASSNAME);
	}
	
	public PricingEnv getPricingEnv() { return _pricingEnv;}
	
	public void setPricingEnv(PricingEnv env) { _pricingEnv = env;}
	
	public static MultiThreadDpsTradeEngine getEngineInstance()
	{
		return engine;	
	}
	

    public MultiThreadDpsTradeEngine(DSConnection ds,String host, int port) {
		super(ds,host,port);
		ENGINE_NAME = CLASSNAME;
    }

    public String getEngineName() { return ENGINE_NAME;}
      
	public boolean start(boolean batch)  throws ConnectException {
	   beforeStart();
	   boolean started = super.start(batch);
	   return started;
	}
	
	private void beforeStart() 
	   throws ConnectException { 	
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
    public boolean process (PSEvent event)    
    {
		boolean result = true;
		if(event instanceof PSEventTrade)
		{
			if(data != null){
				this.data.add(event);
			}else{
				Log.error(CLASSNAME,"Event Data Holder is NULL");
				//System.out.println("Event Data Holder is NULL");
			}
			//data.add(event);
  		}
		return result;
    }
    
    void setEventDataHolder(EventDataHolder eh){
		this.data = eh;
	}
    
    static public void main(String args[]) {
    	
			DSConnection ds=null;
			String className=null;
			try {
				className = OasysUtil.getOption(args, "-class");
				if(Util.isEmptyString(className)){
					throw new Exception("-class option not found..");
				}
		    	ds = ConnectionUtil.connect(args,"MultiThreadDpsTradeEngine");
			}
			catch (Exception e) {
		    	System.out.println("Usage -class <EngineClassName> -env <envName> -user <UserName> " +
				       "-password <password> -o[optional] -threads <ThreadsNumber>");
		    	Log.error(CLASSNAME, e);
		    	return ;
			}
			
			if(ds == null) {
				System.out.println("DSConnection is NULL");
				return;
			}
			
		 	String host=Defaults.getESHost();
		 	int port = Defaults.getESPort();
		 	
		 	try{
		 		Class subclass = Class.forName(className);
		 		Object[] arg = new Object[] { ds,host,new Integer(port) };
		 		Class constructorParemterTypes[] = new Class[3];
		 		constructorParemterTypes[0] = ds.getClass();
		 		constructorParemterTypes[1] = host.getClass();
		 		constructorParemterTypes[2] = Integer.TYPE;
		 		try{
			 		Constructor subclassConstructor = subclass.getConstructor(constructorParemterTypes);
			 		try{
				 		Object obj = subclassConstructor.newInstance(arg);
				 		if(obj instanceof MultiThreadDpsTradeEngine){
				 			engine = (MultiThreadDpsTradeEngine)obj;
				 		}else{
				 			Log.error(CLASSNAME,className+" not of type - MultiThreadDpsTradeEngine.. Unable to start Engine - Exiting.");
				 			System.exit(-1);
				 		}
			 		}catch(InstantiationException instatiationException){
			 			Log.error(CLASSNAME,"Constructor not found.. Exception in method main "+instatiationException.getMessage());
			 		}
		 		}catch(NoSuchMethodException methodException){
		 			Log.error(CLASSNAME,"Instantiation Exception.. Exception in method main "+methodException.getMessage());
		 		}
		 	}catch(ClassNotFoundException ce)
		 	{
		 		Log.error(CLASSNAME,"Class not found Exception in method main "+ce.getMessage());
		 	}
		 	catch(Exception e)
		 	{
		 		Log.error(CLASSNAME,"Exception in method main "+e.getMessage());
		 	}
		 		
		 	try {
				engine.start(true);
				EventDataHolder data =new EventDataHolder();
				engine.setEventDataHolder(data);
				DPSAdapter.initArgs(args, ds, null);
				
				String thread_num = OasysUtil.getOption(args, THREADS_NUMBER);
				
				if (!Util.isEmptyString(thread_num))
				{
					Log.info(CLASSNAME,engine.getEngineName()+" started....");
					//System.out.println("MultiThreadDpsTradeEngine started....");
					int iNumber = Integer.parseInt(thread_num);
					while(iNumber>0)
					{
						String sName = engine.getEngineName()+":Thread_"+iNumber;
						new DPSTradeProcessThreads(sName,data,engine);
						System.out.println(sName+" thread started...");
						iNumber--;
					}
				}
				else
				{
					new DPSTradeProcessThreads("engine.ENGINE_NAME:Thread_1",data,engine);
				}
			}catch(ConnectException ce)
			{
				System.out.println(engine.getEngineName()+": Cannot get PricingEnv, exiting...");
				Log.error(Log.CALYPSOX, ce);
				System.exit(-1);
			}
		 	catch(Exception ee) {
				Log.error(Log.CALYPSOX, ee);
		     	System.exit(-1);
		 	}
		 
	    } // main ends
}
