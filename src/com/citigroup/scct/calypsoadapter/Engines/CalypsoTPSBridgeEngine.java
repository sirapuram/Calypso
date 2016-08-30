/*
 *
 * Author: Guru Alampalli
 *
 */

package calypsox.engine.tps;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import calypsox.tk.core.CalypsoLogUtil;

import com.calypso.engine.Engine;
import com.calypso.tk.core.Defaults;
import com.calypso.tk.core.Log;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventDomainChange;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.ConnectException;
import com.calypso.tk.util.ConnectionUtil;
import com.citigroup.project.util.calypso.ConnUtil;

/**
 * CalypsoTPSBridgeEngine Engine that subscribes to PSEventTrade and PSEventDomainChange events. The subscription is
 * established in the database.
 */

public class CalypsoTPSBridgeEngine extends Engine
{

	final static protected String	ENGINE_NAME	= "CalypsoTPSBridgeEngine";
	public static final String CLASSNAME = "CalypsoTPSBridgeEngine";
	CalypsoTPSPublisher publisher = new CalypsoTPSPublisher();
	static
	{
		CalypsoLogUtil.adjustLogCategory(ENGINE_NAME);
		 
	}

	

	public CalypsoTPSBridgeEngine(DSConnection ds, String host, int port)
	{
		super(ds, host, port);
	}

	public String getEngineName()
	{
		return ENGINE_NAME;
	}

	public boolean start(boolean batch) throws ConnectException
	{
		beforeStart();
		boolean started = super.start(batch);
		return started;
	}

	private void beforeStart() throws ConnectException
	{

	}

	/**
	 * This method is called automatically every time this Engine receives an
	 * event. This method does whatever work you wish to to do in response to
	 * the new event. If your Engine processes more than one event type, you
	 * will add handling for all processed type inside this method. Every Engine
	 * must implement this method.
	 * 
	 * @param event
	 *            the just-received PSEvent
	 */
	public boolean process(PSEvent event)
	{
		boolean result = true;
		if (event instanceof PSEventTrade)
		{
			
			try
			{
				handleEvent((PSEventTrade) event);
				// Here, we call eventProcessed to acknowledge
				// we have successfully consumed the event.

				getDS().getRemoteTrade().eventProcessed(event.getId(),
						ENGINE_NAME);
			} catch (Exception e)
			{
				Log.error(Log.CALYPSOX, e);
				result = false;
			}
		}
		else if (event instanceof PSEventDomainChange)
		{
			
			try
			{
				handleEvent((PSEventDomainChange) event);
				// Here, we call eventProcessed to acknowledge
				// we have successfully consumed the event.

				getDS().getRemoteTrade().eventProcessed(event.getId(),
						ENGINE_NAME);
			} catch (Exception e)
			{
				Log.error(Log.CALYPSOX, e);
				result = false;
			}
		}
		
		return result;
	}

	/**
	 * A simple method that handles processing for trade events.
	 */
	public void handleEvent(PSEventDomainChange event) throws Exception
	{
		System.out.println("Received Domain Change ");
		System.out.println("Received Trade " + event.toString());
		 PSEventDomainChange evdem = (PSEventDomainChange) event;
		 
		 ObjectMessage message;
		try
		{
			message = publisher.getSession().createObjectMessage(evdem);
			publisher.publishMessage(message);
		} catch (JMSException e)
		{
			Log.error(CLASSNAME,"handleEvent PSEventDomainChange Error  message " + e.getMessage());
			e.printStackTrace();
		}
	      
	
	}
	
	/**
	 * A simple method that handles processing for trade events.
	 */
	public void handleEvent(PSEventTrade event) throws Exception
	{
		Log.info(CLASSNAME,"Received Trade Event Change ");
		Log.info(CLASSNAME,"Received Trade " + event.toString());
		PSEventTrade trade=(PSEventTrade)event;
		 
		 ObjectMessage message;
		try
		{
			message = publisher.getSession().createObjectMessage(trade);
			publisher.publishMessage(message);
		} catch (JMSException e)
		{
			Log.error(CLASSNAME,"handleEvent PSEventTrade Error  message " + e.getMessage());
			e.printStackTrace();
		}
	}
	  public void onDisconnect() 
	  {
          super.onDisconnect();
          ConnUtil.disconnect(_ds);
      }
	static public void main(String args[])
	{

		DSConnection ds = null;

		try
		{
			ds = ConnectionUtil.connect(args, "CalypsoTPSBridgeEngine");
		} catch (Exception e)
		{
			System.out.println("Usage -env <envName> -user <UserName> "
					+ "-password <password> -o[optional]");

			Log.error(Log.CALYPSOX, e);
			return;
		}

		if (ds == null)
		{
			System.out.println("DSConnection is NULL");
			return;
		}

		String host = Defaults.getESHost();
		int port = Defaults.getESPort();

		CalypsoTPSBridgeEngine engine = new CalypsoTPSBridgeEngine(ds, host,
				port);
		try
		{
			engine.start(true);

			System.out.println("CalypsoTPSBridgeEngine started....");
		} catch (ConnectException ce)
		{
			System.out
					.println("DpsTradeEngine: Cannot get PricingEnv, exiting...");
			Log.error(Log.CALYPSOX, ce);
			System.exit(-1);
		} catch (Exception ee)
		{
			Log.error(Log.CALYPSOX, ee);
			System.exit(-1);
		}
	} // main ends
}
