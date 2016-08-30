/**
 * 
 */
package com.citigroup.scct.server;

import javax.jms.*;

/**
 * @author rs62899
 *
 */
public class SynchronousRequest implements MessageListener{
	
	private Session session = null;
	private Destination destValue = null;
	private Message message = null;
	private Object lock = null;
	
	public SynchronousRequest(Session theSession, Destination value)
	{
		session = theSession;
		destValue = value;
		lock = new Object();
	}
	
	public Message process(Message message)
	{
		try
		{
			MessageProducer producer = session.createProducer(destValue);

			MessageConsumer consumer = session.createConsumer(session.createTemporaryTopic());
			consumer.setMessageListener(this);

			producer.send(message);
			try 
			{
				lock.wait();
			}
			catch (InterruptedException ex) {}
		}
		catch (JMSException ex) {}
		
		return message;
	}
	
	public void onMessage(Message m)
	{
		message = m;
		lock.notify();
	}
	
}
	
