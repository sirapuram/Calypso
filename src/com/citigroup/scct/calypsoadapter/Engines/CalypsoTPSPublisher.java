/**
 * 
 */
package calypsox.engine.tps;

import java.util.Hashtable;
import java.util.Vector;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.Logger;

import calypsox.tk.refdata.DomainValuesKey;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.Util;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.service.LocalCache;

/**
 * @author ga18627
 * 
 */
public class CalypsoTPSPublisher
{
	//public String			CALYPS_TPS_EMS_URL		= "tcp://nellnx1d.nam.nsroot.net:7222";
	public static final String CLASSNAME = "CalypsoTPSPublisher";
	public String			CALYPS_TPS_EMS_URL		= null;
	protected static String	CALYPSO_TPS_QUEUE_NAME		= "CALYPSO_PS_EVENT_QUEUE";
	protected Logger		logger;
	QueueConnection			queueConnection	= null;
	Queue					tpsQueue		= null;
	QueueSession			queueSession	= null;
	QueueSender				sender			= null;
	
	private void initClient()
	{
		
		try
		{
		
		Vector emsURLVector = LocalCache.getDomainValues(DSConnection
				.getDefault(), DomainValuesKey.CALAYPSO_TPS_EVENTS_EMS_URL);
		
		if (!Util.isEmptyVector(emsURLVector))
		{
			CALYPS_TPS_EMS_URL = (String) emsURLVector.get(0);
			//System.out.print("CALYPS_TPS_EMS_URL : " + CALYPS_TPS_EMS_URL);
		}
		
		Vector tpsQueueVector = LocalCache.getDomainValues(DSConnection
				.getDefault(), DomainValuesKey.CALAYPSO_TPS_EVENTS_EMS_QUEUE_NAME);

		if (!Util.isEmptyVector(tpsQueueVector))
		{
			CALYPSO_TPS_QUEUE_NAME = (String) tpsQueueVector.get(0);
			//System.out.print("\n CALYPSO_TPS_QUEUE_NAME : " + CALYPSO_TPS_QUEUE_NAME);
		}
		
		} catch (Exception e)
		{
			Log.error(CLASSNAME,"Unable get Domain Values   Exception" + e.getMessage());
			e.printStackTrace();
			
		}
	}
	public QueueSession getSession() throws JMSException
	{
		createSessions();
		return queueSession;
	}

	protected void createSessions() throws JMSException
	{

		try
		{
			initClient();
			// Loading initial Context
			Hashtable env = new Hashtable();
			Context ctx = new InitialContext();

			env.put(Context.INITIAL_CONTEXT_FACTORY,
					"com.tibco.tibjms.naming.TibjmsInitialContextFactory");
			env.put(Context.PROVIDER_URL, CALYPS_TPS_EMS_URL);
			ctx = new InitialContext(env);

			QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory) ctx
					.lookup("QueueConnectionFactory");

			queueConnection = queueConnectionFactory.createQueueConnection();
			tpsQueue = (Queue) ctx.lookup(CALYPSO_TPS_QUEUE_NAME);
			queueSession = queueConnection.createQueueSession(false,
					Session.AUTO_ACKNOWLEDGE);
			sender = queueSession.createSender(tpsQueue);
			

		} catch (NamingException e)
		{
			Log.error(CLASSNAME,"Naming  Exception" + e.getMessage());
		
			e.printStackTrace();
			//System.exit(1);
		} catch (JMSException e)
		{
			Log.error(CLASSNAME,"JMS Exception" + e.getMessage());
			e.printStackTrace();
			//System.exit(1);
		}

	}

	protected void publishMessage(ObjectMessage msg) throws JMSException
	{
		
		sender.send(msg);
		
		if (queueConnection != null)
		{
			try
			{
				queueConnection.close();
				
			} 
			catch (JMSException e)
			{
				Log.error(CLASSNAME,"JMS Exception" + e.getMessage());
			}
		}

	}

	
}
