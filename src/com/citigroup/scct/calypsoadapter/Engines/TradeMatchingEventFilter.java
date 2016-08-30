
package calypsox.tk.event;

import java.util.Vector;

import calypsox.apps.trading.TradeKeywordKey;
import calypsox.apps.trading.salesallocation.util.AllocationDelegate;
import calypsox.tk.core.TradeStatus;
import calypsox.tk.refdata.DomainValuesKey;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.Status;
import com.calypso.tk.core.Trade;
import com.calypso.tk.event.EventFilter;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.PSEventTrade;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.service.LocalCache;
/**
 * DPSEventFilter : Event Filter for DpsTradeEngine.
*
*/
public class TradeMatchingEventFilter implements EventFilter {
	
	private String CLASSNAME = "TradeMatchingEventFilter";
    /**
     * Returns true if the Status of the Trade is anything 
     * UNMATCH_T or UNMATCH_S else returns false.
     */

	

    public boolean accept(PSEvent event) {

    	if(event instanceof PSEventTrade) {
        	PSEventTrade et = (PSEventTrade)event;

			Trade trade = et.getTrade();
			Log.debug(CLASSNAME,"Trade has status " + trade.getStatus());
			return isTradePartOfFilter(trade);
    	}
    	else return false;
    }
    
    /**
     * This method will check if the trade is fulfilling the
     * conditions for being part of the filter. This method is static
     * so that it can be reused in other classes. 
     */
    public static boolean isTradePartOfFilter(Trade trade)
    {
		AllocationDelegate allocationDelegate = new AllocationDelegate(DSConnection.getDefault());
		boolean retVal = false;
		Vector postProcessingList = LocalCache.getDomainValues(DSConnection.getDefault(), DomainValuesKey.STM_POST_PROCESSING_EVENTS);
		if(postProcessingList!=null && postProcessingList.contains(trade.getStatus().toString()) )
		{
			if(trade.getKeywordValue(TradeKeywordKey.TRADE_SUBTYPE) != null)
			{
				retVal= true;
			}
		}
		else if(allocationDelegate.isPartOfAllocations(trade))
		{
			retVal = true;
		}
		return retVal;
    	
    }
}
