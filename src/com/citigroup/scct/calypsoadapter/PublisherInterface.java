package com.citigroup.scct.calypsoadapter.publisher;

import javax.jms.*;
import javax.xml.bind.*;

public interface PublisherInterface {

	public void publish(JAXBElement response, MessageProducer producer, Session session, Destination dest, String region) throws Exception;

	public void publishEmptyMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception;

	public void publishErrorMessage(MessageProducer producer, Session session, Destination dest, String region, String mesg) throws Exception;

}
