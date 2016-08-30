package com.citigroup.scct.calypsoadapter;

import java.util.Vector;

import org.apache.log4j.Logger;

import com.calypso.tk.core.Trade;
import com.citigroup.scct.cgen.ScctTradeEventType;

public abstract class TradeHandlerDefault implements TradeHandler {
	
	private static final Logger logger = Logger.getLogger(TradeHandlerDefault.class.getName());
	
	abstract public void handle(Trade trade) throws Exception;

	public void handleBulkTradeEvents(Vector<Trade> trades) throws Exception {
		if (trades != null && trades.size() > 0) {
			for (Trade trade : trades) {
				try {
					handle(trade);
				} catch (Exception e) {
					logger.warn("Caught Exception processing trade event : "
							+ trade.getId());
				}
			}
		}
		logger.debug("Handled bulk trades cnt : " + trades != null ? trades.size() : 0);
	}
}
