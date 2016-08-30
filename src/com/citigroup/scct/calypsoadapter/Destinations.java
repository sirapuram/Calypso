/*
 * Created on Oct 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.citigroup.scct.calypsoadapter;
/**
 * @author mi54678
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public interface Destinations {
	
	public static final String LOGIN_KEY="Topics.loginTopic";
	public static final String TRADE_QUERY_KEY="Topics.tradeQueryTopic";
	public static final String TRADE_UPDATE_KEY="Topics.TradeUpdateTopic";
	public static final String TRADE_EVENT_KEY="Topics.TradeEventTopic";	
	
	public String getLogin();	
	public String getTradeQuery();
	public String getTradeUpdate();
	public String getTradeEvent();
}
