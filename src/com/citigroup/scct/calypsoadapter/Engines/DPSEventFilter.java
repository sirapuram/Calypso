
package calypsox.tk.event;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.Trade;
import com.calypso.tk.event.*;
import com.calypso.tk.service.DSConnection;
import java.util.Vector;

import calypsox.apps.trading.TradeKeywordKey;
import calypsox.tk.refdata.DomainValuesKey;
/**
 * DPSEventFilter : Event Filter for DpsTradeEngine.
*
*/
public class DPSEventFilter implements EventFilter {
	private String CLASSNAME="DPSEventFilter";
    public boolean accept(PSEvent event) {

    	if(event instanceof PSEventTrade) {
        	PSEventTrade et = (PSEventTrade)event;

			Trade trade = et.getTrade();
			//Vector invalidStatus = null;
			Vector includeStatus = null;
			Vector ignoreProducts = null;
			Vector ownerSystem = null;
			
			try {
				includeStatus = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.INCLUDE_TRADE_STATUS);
				ignoreProducts = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_PRODUCTS_FROM_BOFEED);
				ownerSystem = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_EXTERNAL_SYSTEM);
			}
			catch (Exception e) { 
				Log.error(CLASSNAME,e.getMessage());
			}
			
			if (includeStatus != null && !includeStatus.contains(et.getStatus().getStatus())) {

				Log.debug(CLASSNAME,"Trade " + trade.getId() + " status is " + et.getStatus().toString() + ", not sending it to DPS");
				return false;
			}
			
			String tradeOwner = trade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
			if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {

				Log.debug(CLASSNAME,"Trade " + trade.getId() + " Owner is " + tradeOwner + ", not sending it to DPS");
				return false;
			}

			if (ignoreProducts != null && ignoreProducts.contains(trade.getProductType())) {
				Log.debug(CLASSNAME,"Trade " + trade.getId() + " product is " + 
					trade.getProductType() + ", not sending it to DPS");
				return false;
			}

        	return true;
    	}
    	else return true;
    }
    
}
