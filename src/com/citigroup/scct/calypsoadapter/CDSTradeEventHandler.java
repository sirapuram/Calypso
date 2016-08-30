package com.citigroup.scct.calypsoadapter;

import java.util.Vector;

import org.apache.log4j.Logger;

import calypsox.apps.trading.TradeKeywordKey;

import com.calypso.tk.bo.workflow.TradeWorkflow;
import com.calypso.tk.core.Trade;
import com.calypso.tk.service.DSConnection;
import com.citigroup.scct.calypsoadapter.converter.ConverterFactory;
import com.citigroup.scct.cgen.ObjectFactory;
import com.citigroup.scct.cgen.ScctTradeEventType;
import com.citigroup.scct.cgen.ScctTradeType;
import com.citigroup.scct.util.ScctUtil;

/**
 * Trade Event Handler for CreditDefaultSwap. This handler will convert CreditDefaultSwap trade object
 * into a ScctTradeEvent which will be published by the adapter on the EMS Bus.
 * @author kt60981
 *
 */
public class CDSTradeEventHandler extends TradeHandlerDefault implements TradeHandler {
	
	private static final Logger logger = Logger.getLogger(CDSTradeEventHandler.class.getName());
	private DSConnection ds;
	private static ObjectFactory factory = new ObjectFactory();

	public CDSTradeEventHandler(DSConnection dsConn) {
		this.ds = dsConn;
	}
	
	public void handle(Trade trade) throws Exception {
		logger.debug("CDS Product Trade Event : tradeId = " + trade.getId() + " version = " + trade.getVersion());
		logger.debug(trade.toString());

		logger.debug("loading trade actions");
		Vector actions = TradeWorkflow.getTradeActions(trade, ds);
		logger.debug("Available Actions are " + actions);

		ScctTradeType strade = (ScctTradeType) ConverterFactory.getInstance()
				.getTradeConverter(trade).convertFromCalypso(trade);

		if (actions != null) {
			strade.getAvailableActions().addAll(actions);
		}

		ScctTradeEventType tradeEventType = factory.createScctTradeEventType();
		tradeEventType.setScctTrade(strade);
		CalypsoAdapter.getAdapter().handleTradeEvent(tradeEventType);
		//CalypsoAdapter.getAdapter().getTradeMbean().incCdsEventOut();
	}
}
