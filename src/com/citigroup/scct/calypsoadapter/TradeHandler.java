package com.citigroup.scct.calypsoadapter;

import java.util.Vector;

import com.calypso.tk.core.Trade;

/**
 * Trade Event Handler which convert a trade object into the respective ScctTradeEvent.
 * The event will be published by the adapter on the EMS Bus.
 * @author kt60981
 *
 */
public interface TradeHandler {

	abstract public void handle(Trade trade) throws Exception;

	abstract public void handleBulkTradeEvents(Vector<Trade> trades) throws Exception;

}
