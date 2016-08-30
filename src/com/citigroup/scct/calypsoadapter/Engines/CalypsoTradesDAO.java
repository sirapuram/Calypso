package com.citi.credit.gateway.recon;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.xml.bind.Marshaller;

import org.apache.log4j.Logger;

import com.citi.credit.gateway.core.CGSchemaObjects;
import com.citi.credit.gateway.data.CGException;
import com.citi.credit.gateway.data.CGReconTrade;
import com.citi.credit.gateway.data.CGResult;
import com.citi.credit.gateway.service.CreditGatewayInterface;
import com.citigroup.scct.cgen.QueryScctReconType;
import com.citigroup.scct.cgen.ScctCalypsoConfigType;
import com.citigroup.scct.cgen.ScctTradeType;

/*
 * History
 * 10/14/2010   pk00001		Gemfire Fee Recon + Performace Improvement
 * 09/21/2010   pk00001		Gemfire Recon reorg + adding new fields in recon report 
 * 10/21/08		kt60981		Initial check-in
 */

public class CalypsoTradesDAO {

	private final static Logger logger = Logger.getLogger("com.citi.credit.gateway.recon.CalypsoTradesDAO");
	
	public Hashtable getTrades(CreditGatewayInterface gateway, String query) throws CGException {

		Hashtable reconTrades = new Hashtable();
		CGResult results = gateway.queryReconTrades(query, false, true);
		int cnt = 0;
		while (results.hasNext()) {
			CGReconTrade trade = (CGReconTrade) results.next();
			reconTrades.put(trade.getTradeId(), trade);
		}
		return reconTrades;
	}
	
	public Hashtable loadTrades(CreditGatewayInterface gateway, String query) throws CGException {

		Hashtable trades = new Hashtable();
		CGResult results = gateway.queryTrades(query, false, false);
		int cnt = 0;
		Integer bad = new Integer(-999999);
		while (results.hasNext()) {
			ScctTradeType trade = null;
			try {
				trade = (ScctTradeType) results.next();
				// store trade id for now to converse memory
				if (trade!=null && trade.getId()!=null) {
					trades.put(trade.getId(), trade.getId());
				} else {
					// some result contains error info only, insert some dummy int
					trades.put(bad, bad);
				}
			} catch (RuntimeException e) {
				logger.warn("Caught Exception bad trade '" + (trade!=null ? trade.getId() : null));
				e.printStackTrace();
			}
		}
		return trades;
	}
	
	public static void main(String[] args) {
		CreditGatewayInterface gateway = null;
		try {
			
//			gateway = new CreditGatewayImpl("CreditGatewayServer1", "calypso_ny", "calypso");
			HashMap props = ReconUtil.loadReconConfig("./config/gemfireReconConfig.xml");
			ScctCalypsoConfigType config = ReconUtil.getCalypsoConfig("test", props);
			Marshaller marshaller = CGSchemaObjects.getMarshaller();
			QueryScctReconType query = config.getQuery().get(0);
			StringWriter w = new StringWriter();
			marshaller.marshal(query, w);
			CalypsoTradesDAO dao = new CalypsoTradesDAO();
			Hashtable trades = dao.getTrades(gateway, w.toString());
			logger.debug("Count : " + trades.size());
		} catch (CGException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (gateway!=null) {
				try {
					gateway.closeGateway();
				} catch (CGException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public Map<Integer, TradeReconObject> getTrades(QueryScctReconType queryScct) throws Exception {
		String product = (queryScct!=null ? queryScct.getProduct() : null);
		if (!isEmptyString(product)) {
			CalypsoLocalDAO dao =new  CalypsoLocalDAO();
			return dao.getTrades(queryScct.getFrom(), queryScct.getWhere(), product);
		}
		return null;
	}
	
	public Map<Integer, List<FeeReconObject>> getTradeFees(QueryScctReconType queryScct) throws Exception {
		String product = (queryScct!=null ? queryScct.getProduct() : null);
		if(!isEmptyString(product)) {
			CalypsoLocalDAO dao =new  CalypsoLocalDAO();
			return dao.getTradeFees(queryScct.getFrom(), queryScct.getWhere(), product);
		}
		return null;
	}
	
	private static boolean isEmptyString(String s) {
        return s == null || s.trim().length() == 0;
    }
}
