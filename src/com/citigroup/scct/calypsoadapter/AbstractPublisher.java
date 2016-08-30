package com.citigroup.scct.calypsoadapter.publisher;

import java.io.*;
import java.util.*;
import javax.jms.*;
import javax.xml.bind.*;
import org.apache.log4j.*;
import com.citigroup.scct.calypsoadapter.*;
import com.citigroup.scct.cgen.*;
import com.citigroup.scct.util.*;

public abstract class AbstractPublisher implements PublisherInterface {

	int batchSize = Integer.parseInt(System.getProperty(CalypsoAdapterConstants.BATCH_STREAM_SIZE,"500"));
	int totMesgCnt = 0;
	int currMesgCnt = 0;
	int totBatchCnt = 0;
	int currBatchCnt = 0;

	private final static Logger logger = Logger.getLogger("com.citigroup.scct.calypsoadapter.publisher.AbstractPublisher");
	
	public abstract void publish(JAXBElement response, MessageProducer producer, Session session, Destination dest, String region) throws Exception;

	public abstract void publishEmptyMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception;

	public abstract void publishErrorMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception;

	protected void logMessage(){
		logger.debug("This message is for information purposes only");
	}
	
	protected Message createTextMessage(JAXBElement object, Session session, String region, String totMesgCnt, String currMesgCnt, String totBatchCnt, String currBatchCnt) throws Exception {
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
		responseMessage.setStringProperty("totalMessageCnt", totMesgCnt);
		responseMessage.setStringProperty("currentMessageCnt", currMesgCnt);
		responseMessage.setStringProperty("totalBatchCnt", totBatchCnt);
		responseMessage.setStringProperty("currentBatchCnt", currBatchCnt);
		
		long z = System.currentTimeMillis();
		logger.debug("Building JMS Message : " + object.getValue() + " time = " + (double)((z-y)/1000.0));
//		logger.debug(object.getClass().getAnnotations().getClass().getName());
//		logger.debug(object.getClass().getCanonicalName());
		return responseMessage;
	}

	
	protected void send(MessageProducer producer, Destination dest, Message message ) throws Exception {
		long x = System.currentTimeMillis();
		if(dest != null){
			producer.send(dest, message);
		}
		long y = System.currentTimeMillis();
		logger.debug("Publishing JMS Message : time = " + (double)((y-x)/1000.0));
		
	}
}
