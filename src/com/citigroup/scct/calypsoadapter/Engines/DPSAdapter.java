/*
 * DPSAdapter to send trades to OASYS
 * Author: Pranay Shah
 *
 */

package com.citigroup.project.dps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.TimeZone;
import java.util.Vector;

import calypsox.apps.trading.TradeKeywordKey;
import calypsox.tk.bo.workflow.rule.BackToBackTradeRule;
import calypsox.tk.core.CalypsoLogUtil;
import calypsox.tk.core.CitiCustomTradeData;
import calypsox.tk.core.DealCoverCustomData;
import calypsox.tk.pricer.PricerMeasureDpi;
import calypsox.tk.product.GlobalMirrorHandler;
import calypsox.tk.refdata.DomainValuesKey;
import calypsox.tk.util.DealCoverUtil;
import calypsox.tk.util.OasysTradeValidator;

import com.calypso.tk.bo.Fee;
import com.calypso.tk.core.Defaults;
import com.calypso.tk.core.JDate;
import com.calypso.tk.core.JDatetime;
import com.calypso.tk.core.LegalEntity;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.PricerMeasure;
import com.calypso.tk.core.Product;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.marketdata.PricingEnv;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.ConnectException;
import com.calypso.tk.util.ConnectionUtil;
import com.calypso.tk.util.PricerMeasureUtility;
import com.calypso.tk.util.TradeArray;
import com.citigroup.project.dps.dpsjni.DPSTradeSave;
import com.citigroup.project.dps.exception.DPSSenderException;
import com.citigroup.project.util.DateUtil;
import com.citigroup.project.util.oasys.OasysUtil;

/* 
 * 06/02/2009 		ds53697		Changes done to handle exceptions
 * 								thrown from native code (JIRA:35544-4717)
 */

public class DPSAdapter {

	private static final String CLASSNAME = "DPSAdapter";
	
	public static String outputDir = null;
	public static final String OUTPUT_DIR = "-output";
	public static final String NO_SAVE_TO_DPS = "-nodpssave";
	public static final String INIT_PRICINGENV = "-pricingenv";
	public static final String INIT_VALDATE = "-valdate";
	public static final String DEAL_ID_FILE = "-filename";
	public static final String NO_DPI_PRICER = "-nodpipricer";
	public static final String OUTPUT_NAME_SYSTEM = "-outputnamesystem";
	/*static var used by OUTPUT_NAME_SYSTEM option*/
	public static final String OUTPUT_NAME_OPSFILE = "opsfile";
	public static final String OUTPUT_NAME_OASYS = "oasys";
	public static final String OUTPUT_NAME_CALYPSO = "calypso";
	public static String outputNameSystem = OUTPUT_NAME_OASYS; //store output system
	public static boolean CONTRACT_REVISION = false;
	public static boolean isFilePrint;
	public static boolean noDpiPricer = false;
	public static final String CITICREDITSYNTHETICCDO_TYPE = "CitiCreditSyntheticCDO";

/* START-CHANGE || 18-04-2007 || O. Jayachandra Gupta || Constants to check TYPE, STATUS */
	public static final String TYPE="TYPE";
	public static final String STATUS="STATUS";
	public static final String DELIMITER=":";
	public static final String TRADE_ID="TRADEID";
	public static final String DEAL_ID="DEALID";
	public static final String OPS_FILE="OPSFILE";
/* END-CHANGE || 18-04-2007 */
	
	protected static PricingEnv _pricingEnv;
	protected static String _pricingEnvName;
	protected static PricerMeasure[] _pricerMeasures;
	protected static JDatetime _valuationDateTime = null;
	public static TimeZone _valTZ = null;
	
	public static boolean DPSErrorFlag = false; 

	static
	{
		CalypsoLogUtil.adjustLogCategory(CLASSNAME);
	}
	
	public static PricingEnv getPricingEnv() { return _pricingEnv;}
	public static void setPricingEnv(PricingEnv env) { _pricingEnv = env;}

	public static void setValTimeZone(TimeZone tz) { _valTZ = tz;}
	public static TimeZone getValTimeZone() { return _valTZ; }

	public static JDatetime getValuationDateTime() { return _valuationDateTime; }
	public static void setValuationDateTime(JDatetime valuationDateTime ) {
			_valuationDateTime = valuationDateTime;
	}

	public static PricerMeasure[] getPricingMeasures() { return _pricerMeasures;}
	public static void setPricingEnv(PricerMeasure[] pricerMeasures) {
		_pricerMeasures = pricerMeasures;
	}

	/**
	* initArgs function
	* @param args    The <code>args</code> passed thru DpsTradeEngine of command line
	*/
	public static void initArgs(String args[], DSConnection ds, PricingEnv pricingEnv) {

		outputDir = OasysUtil.getOption(args, OUTPUT_DIR);
		if(outputDir == null) {
            outputDir = "./";
        }
		isFilePrint = OasysUtil.isOption(args, NO_SAVE_TO_DPS);
		noDpiPricer = OasysUtil.isOption(args, NO_DPI_PRICER);

		initPricerParams(ds, OasysUtil.getOption(args, INIT_VALDATE));
		Log.info(CLASSNAME, 
				"About to download Pricing Environment ");
		initPricingEnv(ds,pricingEnv,OasysUtil.getOption(args, INIT_PRICINGENV));
		Log.info(CLASSNAME, 
				"Finished downloading Pricing Environment " );
		outputNameSystem = OasysUtil.getOption(args, OUTPUT_NAME_SYSTEM);//kl 2-21-06 determine how output xml file is saved
		
	}

	/**
	* initPricingEnv function
	* @param DSConnection    The <code>DSConnection</code>
	*/
	public static void initPricingEnv(DSConnection ds, PricingEnv pricingEnv, String pricingEnvName) {

		if(pricingEnv == null) {
			try {
				_pricingEnvName = pricingEnvName;

				if(_pricingEnvName == null)
					_pricingEnvName = "default";

				_pricingEnv = ds.getRemoteMarketData().getPricingEnv(_pricingEnvName);
				_pricingEnv.getPricerConfig().refresh(_pricingEnv,getValuationDateTime());

				if(_pricingEnv==null)
					throw new ConnectException("Can not Get PricingEnv: " + _pricingEnvName);
				}
				catch (Exception e) {
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
		}
		else {
			_pricingEnv = pricingEnv;
		}

		if (Log.isDebug())
		{
		
		   Log.info(CLASSNAME, "PricingEnv:        " + getPricingEnv().getName()
			   	   + "PricerConfigName:  " + getPricingEnv().getPricerConfigName()
				   + "PricerQuoteSet:    " + getPricingEnv().getQuoteSetName()
				   + "ValuationDateTime: " + getValuationDateTime()
				   + "DPIPricerMeasures: " + getPricingMeasures().toString()
				   + "ValTimeZone:       " + getValTimeZone().getID()
				   + "NODPIPricer:         " + noDpiPricer
				   + "FilePrint:         " + isFilePrint + "\n");
		}
	}

	/**
	* initPricerParams function
	* @param DSConnection    The <code>DSConnection</code>
	*/
	public static void initPricerParams(DSConnection ds, String valDate) {
		Vector measuresV = new Vector();

		measuresV.add((String)PricerMeasureDpi.S_CASHFLOWS);
		_pricerMeasures = PricerMeasureUtility.makeMeasures(ds, measuresV, null);

		if(ds.getUserDefaults() != null && ds.getUserDefaults().getTimeZone() != null)
			setValTimeZone(ds.getUserDefaults().getTimeZone());
		else
			setValTimeZone(Util.getReferenceTimeZone());

		if(valDate != null) {
			_valuationDateTime = new JDatetime(DateUtil.getStringToJDate(valDate),23,59,59,999,getValTimeZone());
		}
		else {
			_valuationDateTime = new JDatetime(JDate.getNow(),23,59,59,999,getValTimeZone());
		}
	}


	/**
	* processData function to process PSEventTrade thru DpsTradeEngine
	* @param trade    The <code>Trade</code> object associated with PSEventTrade
	*/
	public static boolean processData(Trade trade) {

		String xmlString = null;
		PrintWriter pw = null;
		boolean tradeSaved = false;
		int dealID = 0;

		DealCoverCustomData dcCustomData = null;
		
/* START-CHANGE || 23-04-2007 || O. Jayachandra Gupta || Is valid Fee, Book, Counterparty, Earlyterminationdate */  
		tradeSaved=isValidFBC(trade,DSConnection.getDefault());
		if(!tradeSaved) {
			Log.error(CLASSNAME, "Severe Problem Exiting... Trade Validations Failed!!! TRADE_ID:"+trade.getId());
			return tradeSaved;
		}
/* END-CHANGE || 23-04-2007 */ 

		Log.info(CLASSNAME,
				 "Entered processData() for trade: " + trade.getId());
		
		
		if(!(	trade.getProductType().equals(Product.CREDITDEFAULTSWAP) || 
				trade.getProductType().equals(CITICREDITSYNTHETICCDO_TYPE))) {

			Log.error(CLASSNAME, "Not supported product for Phase I " + trade.getProductType() +
											" trade id: " + trade.getId());
		}

		if((CitiCustomTradeData)trade.getCustomData() != null) {
			dcCustomData = ((CitiCustomTradeData)trade.getCustomData()).
                                            getDealCoverCustomData();
		}

		if(dcCustomData != null) {

			Log.info(CLASSNAME, 
					"Processing Trade with Oasys DealId: " + dcCustomData.getDealID()
					+ " and tradeId: " + trade.getId());
			
			if(dcCustomData.getDealID() <= 0 ||
				dcCustomData.getTransactionID() <= 0) {

				throw new DPSSenderException("Can't send trades to BackOffice since DealID or " +
				"TransactionID is not set in trade CustomData" + trade.getId());
			}

			if(trade.getBundle() != null && trade.getBundle().getType().equals(
				BackToBackTradeRule.BACK_TO_BACK) &&
				!trade.getRole().equals(LegalEntity.COUNTERPARTY)) {

				Log.warn(CLASSNAME, "BTB firm trade, skip PSEventTrade: " +
					    dcCustomData.getDealID() + "." +
					    dcCustomData.getTransactionID());

				return tradeSaved;
			}

			// For mirror trades we don't want to send it twice, so we will skip PSEventTrade for second trade.
			if(trade.getBundle() != null &&
				trade.getBundle().getType().equals(GlobalMirrorHandler.BUNDLE_NAME)) {

				try {
				TradeArray array =  DSConnection.getDefault().
								getRemoteTrade().getBundleTrades(trade.getBundle().getId());

				Trade mainTrade = (Trade) array.get(0);
				Trade mirrorTrade = (Trade) array.get(1);

				Log.info(CLASSNAME,
						 "DealID: " + dcCustomData.getDealID()
						 + ", tradeId: " + trade.getId()
						 + " are part of a Bundle."
						 + " Main tradeId = " + mainTrade.getId()
						  + ", Mirror tradeId = " + mirrorTrade.getId()); 
				
				if(((CitiCustomTradeData)mainTrade.getCustomData() != null) &&
					((CitiCustomTradeData)mirrorTrade.getCustomData() != null)) {

					DealCoverCustomData mainDCCustomData =
						((CitiCustomTradeData)mainTrade.getCustomData()).getDealCoverCustomData();
					DealCoverCustomData mirrorDCCustomData =
						((CitiCustomTradeData)mirrorTrade.getCustomData()).getDealCoverCustomData();

					Log.info(CLASSNAME,
							 "DealID: " + dcCustomData.getDealID()
							 + " - Checking we're not sending Mirror Trade twice");
					
					if(mainDCCustomData != null && mirrorDCCustomData != null) {
						int mainTradeTxnID = mainDCCustomData.getTransactionID();
						int mirrorTradeTxnID = mirrorDCCustomData.getTransactionID();
						int maxTransactionID = 0;

						Log.info(CLASSNAME,
								 "DealID: " + dcCustomData.getDealID()
								 + " - Main trade (" + mainTrade.getId() + ") transactionId is: " + mainTradeTxnID
								 +  ", Mirror trade (" + mirrorTrade.getId() + ") transactionId is: " + mirrorTradeTxnID); 	
						
						if(mainTradeTxnID < mirrorTradeTxnID) {
							maxTransactionID = mirrorTradeTxnID;
						}
						else {
							maxTransactionID = mainTradeTxnID;
						}

						if(dcCustomData.getTransactionID() == maxTransactionID) {
							Log.info(CLASSNAME, 
									"Skip Mirror trade event: " + trade.getId() 
									+ " Deal: " + dcCustomData.getDealID()
									+ ":" + dcCustomData.getTransactionID());
							return tradeSaved;
						}
						else {
							Log.info(CLASSNAME, "Process Main Mirror trade event: " + trade.getId() + " Deal: " + dcCustomData.getDealID() + ":" + dcCustomData.getTransactionID());
						}
					}
				}
				}
				catch (Exception e) {
					   throw new DPSSenderException("For trade: " + trade.getId()
					   		                        + ", could not get trades by bundle id " + trade.getBundle().getId(), e);
				}
			}

		}
		else {
			throw new DPSSenderException("Can't send trade: " + trade.getId()
					                     + " to DPS, since DealCoverCustomData is null.");
			
		}

		xmlString = new String();

		try{

			if(isFilePrint) {
       			try {
       				pw  = new PrintWriter(
					new FileWriter(outputDir +
							String.valueOf(trade.getId()) + ".xml"));
       			}
       			catch (IOException e) {
           			Log.error(CLASSNAME, "Could not open file to write " +
                                   String.valueOf(trade.getId()) + ".xml", e);
       			}
			}

			IDPSDataObject calypsoDeal = new Deal();
			Log.info(CLASSNAME,
					 "TradeId: " + trade.getId()
					 + ", DealID: " + dcCustomData.getDealID()
		 			 + " - About to set Deal data");
			calypsoDeal.set(trade, null);
			Log.info(CLASSNAME,
					 "TradeId: " + trade.getId()
					 + ", DealID: " + dcCustomData.getDealID()
					 + " - Successfully set Deal data.");
			
			xmlString = calypsoDeal.generateXML();
  		    
			Log.info(CLASSNAME, 
					 "TradeId: " + trade.getId()
					 + ", DealID: " + dcCustomData.getDealID()
					 + " - Successfully generated XML.");
				   	   
			

			
			if(isFilePrint) {
       			pw.write(xmlString);
       			pw.close();
			}
			else {
				try {
					Log.info(CLASSNAME, 
							 "TradeId: " + trade.getId()
							 + ", DealID: " + dcCustomData.getDealID()
							+ " - About to save to DPS.");
        	   		DPSTradeSave.saveTrade(xmlString);
        	   		Log.info(CLASSNAME,
        	   				 "TradeId: " + trade.getId()
							 + ", DealID: " + dcCustomData.getDealID()
							 + " - Successfully saved Deal.");
        	   		
        	   		tradeSaved = true;
        		}
        		catch (DPSSenderException dEx) {
            		Log.error(CLASSNAME, "Couldn't save the trade with dealID: " 
            				+ dcCustomData.getDealID(), dEx);
            		DPSErrorFlag = true;
            		throw dEx;
        		}	
        		catch (Exception e) {
            		Log.error(CLASSNAME, "Couldn't save the trade with dealID: " 
            				+ dcCustomData.getDealID(), e);
        		}	
            	
			}
		}
		catch (DPSSenderException dpsEx)
		{
			throw dpsEx;
		}		
		catch (Exception e) 
		{   
			throw new DPSSenderException(e);
		}
		
		return tradeSaved;
	}

/* START-CHANGE || 25-04-2007 || O.Jayachandra Gupta || To check valid fee, counterparty, book, settlementdate*/
	/**
	* @param trade object on which the validations are checked
	* @param ds object to pass on to other methods 
	*/
	public static boolean isValidFBC(Trade trade, DSConnection ds) throws DPSSenderException {

		boolean isValidTradeFee=true;
		boolean isValidTradeCounterParty=true;
		boolean isValidTradeBook=true;
		boolean isValidSettlementDate=true;
		boolean isValidEarlyTerminationDate=true;
		boolean isTradeNotNull = true;
		boolean tradeSaved=false;

		OasysTradeValidator oasysTrade=new OasysTradeValidator();
		
		try {
			
			if(trade==null) {
				Log.error(CLASSNAME, "Trade is Null");
				isTradeNotNull = false;
				throw new DPSSenderException("Trade Not Sent To DPS : Trade is NULL");
				//return tradeSaved;
			}
			Vector feeVector=trade.getFees();
			if(feeVector!=null) {
				for(int i=0;i<feeVector.size();i++) {
					Fee fee=(Fee)feeVector.get(i);
					if(fee!=null) {
						if(oasysTrade.isValidFee(fee,null,ds, trade,null)==false) {
							Log.error(CLASSNAME, "Invalid Fee LegalEntity!!! TRADE_ID:"+trade.getId());
							isValidTradeFee=false;
							throw new DPSSenderException("Trade Not Sent To DPS : Trade "+ trade.getId() +" Invalid Fee LegalEntity!!");							
						}
					}
				}
			}
			isValidTradeCounterParty=oasysTrade.isValidCounterParty(trade,null,ds);
			if(!isValidTradeCounterParty){
				Log.error(CLASSNAME, "Invalid Counter Party/Account Number attribute required!!! TRADE_ID:"+trade.getId());
				throw new DPSSenderException("Trade Not Sent To DPS : Trade "+ trade.getId() +" Invalid Counter Party/Account Number attribute required!!!");
			}
			isValidTradeBook=oasysTrade.isValidBook(trade);
			if(!isValidTradeBook){
				Log.error(CLASSNAME, "Invalid Book/Account Number attribute required!!! TRADE_ID:"+trade.getId());
				throw new DPSSenderException("Trade Not Sent To DPS : Trade "+ trade.getId() +" Invalid Book/Account Number attribute required!!!");
			}
			isValidSettlementDate=oasysTrade.isValidSettlementDate(trade);
			if(!isValidSettlementDate){
				Log.error(CLASSNAME, "Invalid Settlement Date [SettlementDate should not be less than TradeDate]!!! TRADE_ID:"+trade.getId());
				throw new DPSSenderException("Trade Not Sent To DPS : Trade "+ trade.getId() +" Invalid Settlement Date [SettlementDate should not be less than TradeDate]!!!");
			}
			isValidEarlyTerminationDate=oasysTrade.isValidEarlyTerminationDate(trade);
			if(!isValidEarlyTerminationDate){
				Log.error(CLASSNAME, "Invalid Early Termination Date!!! TRADE_ID:"+trade.getId());
				throw new DPSSenderException("Trade Not Sent To DPS : Trade "+ trade.getId() +" Invalid Early Termination Date!!! ");
			} 
		} catch (DPSSenderException dpsEx) {
			throw dpsEx;
		}
		tradeSaved=(isTradeNotNull & isValidTradeFee & isValidTradeCounterParty & isValidTradeBook & isValidSettlementDate & isValidEarlyTerminationDate);
		Log.debug(CLASSNAME, "Result  == "+tradeSaved);
		return tradeSaved;
	}
/* END-CHANGE || 23-04-2007 */
	
	/**
	* processTradesFromList - To send list of Trades to BackOffice
	* @param ArrayList  The <code>DealID</code> list to send to BackOffice
	* @param DSConnection <code>DSConnection</code> to fetch Trade object
	*/

	public static void processTradesFromList(ArrayList dealIDs,
												DSConnection ds) {

		String xmlString = null;
		PrintWriter pw = null;
		String dealStr = null;
		int dealID = 0;
		IDPSDataObject calypsoDeal = null;
		Trade trade = null;
		Vector ownerSystem = null;
		String tradeOwner = null;

/* START-CHANGE || 18-04-2007 || O. Jayachandra Gupta || to check for TYPE, STATUS */
		String typeValue=null;
		String statusValue=null;
		
		if(dealIDs!=null && dealIDs.size()>2) {
			String typeString=(String)dealIDs.get(0);
			if(typeString.startsWith(TYPE) && typeString.indexOf(DELIMITER)!=-1){
				typeValue=typeString.substring(typeString.indexOf(DELIMITER)+1);
			} else {
				Log.error(CLASSNAME,"Invalid File data first two lines[1. TYPE:TRADEID/DEALID/OPSFILE, 2. STATUS:Verified/..]");
				return;
			}
			String statusString=(String)dealIDs.get(1);
			if(statusString.startsWith(STATUS) && statusString.indexOf(DELIMITER)!=-1) {
				statusValue=statusString.substring(statusString.indexOf(DELIMITER)+1);
			} else {
				Log.error(CLASSNAME,"Invalid File data first two lines[1. TYPE:TRADEID/DEALID/OPSFILE, 2. STATUS:Verified/..]");
				return;
			}
			//to remove first two lines (TYPE,STATUS) from array list
			//dealIDs.remove(0);
			//dealIDs.remove(1);
			Log.debug(CLASSNAME,"JC DealIDs Size :"+dealIDs.size());
		} else {
			Log.error(CLASSNAME,"Invalid File data first two lines[1. TYPE:TRADEID/DEALID/OPSFILE, 2. STATUS:Verified/..]");
			return;
		}
/* END-CHANGE || 18-04-2007 */
		
		try {
			ownerSystem = DSConnection.getDefault().getRemoteReferenceData().getDomainValues(DomainValuesKey.IGNORE_EXTERNAL_SYSTEM);
		}
		catch (Exception e) {
			Log.error(CLASSNAME, e.getMessage());
		}

		for(int i = 2; i < dealIDs.size(); i++) {

			try{
/* START-CHANGE || 18-04-2007 || O. Jayachandra Gupta || to identify TYPE:TRADEID */
				dealStr = (String)dealIDs.get(i);
//				dealID = Integer.parseInt(dealStr);
//				trade = ds.getRemoteTrade().getTrade(dealID);
				CitiCustomTradeData ct = null; //used later for opsfile CAL3771
/*
				if(trade != null) {
					tradeOwner = trade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
					if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {
						Log.warn(CLASSNAME, "Not sending trade: " + trade.getId() + " OWNER OASYS");
						continue;
					}
					ct =  (CitiCustomTradeData) trade.getCustomData();
					calypsoDeal = new Deal();
					calypsoDeal.set(trade, null);
				}
				else {
					if(dealID > 0) {
						TradeArray array = DealCoverUtil.getTradesByDealCover(dealID);


						if(array == null || array.size() == 0) {
							continue;
						}

						Trade calTrade =  array.get(0);
						tradeOwner = calTrade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
				if(trade != null) {
					tradeOwner = trade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
					if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {
						Log.warn(CLASSNAME, "Not sending trade: " + trade.getId() + " OWNER OASYS");
						continue;
					}
					ct =  (CitiCustomTradeData) trade.getCustomData();
					calypsoDeal = new Deal();
					calypsoDeal.set(trade, null);
				}
				else {
					if(dealID > 0) {
						TradeArray array = DealCoverUtil.getTradesByDealCover(dealID);


						if(array == null || array.size() == 0) {
							continue;
						}

						Trade calTrade =  array.get(0);
						tradeOwner = calTrade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);

						if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {
							Log.warn(CLASSNAME, "Not sending trade: " + calTrade.getId() + " OWNER OASYS");
							continue;
						}
						ct =  (CitiCustomTradeData) calTrade.getCustomData();	
						calypsoDeal = new Deal();
						calypsoDeal.set(null, array);
					}
					else {
						continue;
					}
				}
*/
				if(typeValue.equalsIgnoreCase(TRADE_ID)) {
					/* START-CHANGE || 02-05-2007 || Venkatesan.k || to identify TYPE:TRADEID */
					try{
						dealID = Integer.parseInt(dealStr);
					}catch(Exception e){
						Log.error(CLASSNAME,"Invalid TradeID:"+dealStr + "Exception :"+e.getMessage());
					}
					/* END-CHANGE || 02-05-2007 */
					if(dealID<=0) {
						continue;
					}
					trade = ds.getRemoteTrade().getTrade(dealID);
					if(trade==null) {
						continue;
					}
					if(!isValidFBC(trade,ds)) {
						Log.error(CLASSNAME, "Severe Problem... Exiting Trade Validation Failed!!! TRADE_ID:"+trade.getId());
						continue;
					} 
/* END-CHANGE || 18-04-2007 */
					tradeOwner = trade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);
					if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {
						Log.warn(CLASSNAME, "Not sending trade: " + trade.getId() + " OWNER OASYS");
						continue;
					}
					ct =  (CitiCustomTradeData) trade.getCustomData();
					calypsoDeal = new Deal();
					calypsoDeal.set(trade, null);
				}
/* START-CHANGE || 18-04-2007 || O. Jayachandra Gupta || to identify TYPE:DEALID/OPSFILE */
				else if(typeValue.equalsIgnoreCase(DEAL_ID)||typeValue.equalsIgnoreCase(OPS_FILE)) {
					TradeArray array=null;
					if(typeValue.equalsIgnoreCase(DEAL_ID)) {
						/* START-CHANGE || 02-05-2007 || Venkatesan.k || to identify TYPE:DEALID */
						try{
							dealID = Integer.parseInt(dealStr);
						}catch(Exception e){
							Log.error(CLASSNAME,"Invalid DealID:"+dealStr + "Exception :"+e.getMessage());	
						}
						/* END-CHANGE || 02-05-2007 */
						if(dealID<=0) {
							continue;
						}
						 array= DealCoverUtil.getTradesByDealCover(dealID);
					} else {
						if(dealStr==null || "".equals(dealStr.trim())) {
							continue;
						}
						array=DealCoverUtil.getTradesByOpsFile(dealStr);
					}
					
					if(array == null || array.size() == 0) {
						continue;
					}
					Trade calTrade =  array.get(0);
					if(calTrade!=null) {
						if(!isValidFBC(calTrade,ds)) {
							Log.error(CLASSNAME, "Severe Problem... Exiting Trade Validation Failed!!! DEAL_ID/OPS_FILE :"+dealStr+" TRADE_ID :"+calTrade.getId());
							continue;
						} 
					}
/* END-CHANGE || 18-04-2007 */
					
					tradeOwner = calTrade.getKeywordValue(TradeKeywordKey.OWNERSYSTEM);

					if (tradeOwner != null && ownerSystem != null && ownerSystem.contains(tradeOwner)) {
						Log.warn(CLASSNAME, "Not sending trade: " + calTrade.getId() + " OWNER OASYS");
						continue;
					}
					ct =  (CitiCustomTradeData) calTrade.getCustomData();	
					calypsoDeal = new Deal();
					calypsoDeal.set(null, array);
				} else {
					continue;
				}

				if(isFilePrint) {
					try {
						if(outputNameSystem.equalsIgnoreCase(OUTPUT_NAME_OPSFILE)){
							//save xml file using opsfile as name							
							String opsFile = ct.getDealCoverCustomData().getOperationsFile();
							pw  = new PrintWriter(new FileWriter(outputDir +
									opsFile + ".xml"));
						}else{
							pw  = new PrintWriter(new FileWriter(outputDir +
									String.valueOf(dealID) + ".xml"));
						}

					}//try
					catch (IOException e) {
						Log.error(CLASSNAME, "Could not open file to write " +
									String.valueOf(dealID) + ".xml", e);
					}
				}

				xmlString = new String();
				xmlString = calypsoDeal.generateXML();

				if(isFilePrint) {
       				pw.write(xmlString);
       				pw.close();
       				Log.info(CLASSNAME, 
       						"Successfully generated XML for DealID: " + dealID);
       						
				}
				else {
					try {
						Log.info(CLASSNAME, "About to send dealID '"
								+ dealID
								+ "' to DPS.");
            			DPSTradeSave.saveTrade(xmlString);
            			Log.info(CLASSNAME, "Successfully sent dealID '"
								+ dealID
								+ "' to DPS.");
        			}
        			catch (DPSSenderException e1) {
            			throw e1;
        			}

        			catch (Exception e) {
            			Log.error(CLASSNAME, "Trade Not Sent To DPS with dealID: " 
            					+ dealID + " to DPS", e);
        			}
				}
			}
			catch (RemoteException re) {
				Log.error(CLASSNAME, "Cannot generate XML for dealID : " + dealID
						   + ". Possible cause: " +  re.getMessage() , re);
				
			}
			catch (DPSSenderException ex) {
				throw ex;
			}
		    catch (Exception e) 
			{ 
		    	Log.error(CLASSNAME, "Cannot generate XML for dealID : " + dealID
						   + ". Possible cause: " +  e.getMessage() , e);
			}
		}
	}

	/**
	* DPSAdpater Driver
	* @param args    The <code>argument</code> list for Backoffice processing
	*/
	public static void main(String[] args) {
		
		if ((args != null) && (args.length >= 2)) {
			String env = Defaults.getOption(args, "-env");
        	String user = Defaults.getOption(args, "-user");
        	String pswd = Defaults.getOption(args, "-password");
        	String contractRev = Defaults.getOption(args, "-cr");
        	if(contractRev != null && contractRev.equalsIgnoreCase("true")) {
        		CONTRACT_REVISION = true;
        	}
			DSConnection ds = null;

			if(OasysUtil.isOption(args, DEAL_ID_FILE)) {

				try {
					ds = ConnectionUtil.connect(user, pswd, "DPSAdapter", env);
				}
				catch (ConnectException conExec) {
					Log.error(CLASSNAME, "Cannot connect to the dataserver");
					conExec.printStackTrace();
				}

				DPSAdapter.initArgs(args,ds,null);

				ArrayList dealIDList = new ArrayList();
				String dealIDFile = Defaults.getOption(args, DEAL_ID_FILE);
				String nextLine = "";
				try {
					String filePath = Util.findFileInClassPath(dealIDFile);
					if(filePath != null) {
						BufferedReader br = new BufferedReader(
											new FileReader(filePath));
                		while ((nextLine = br.readLine()) != null) {
							dealIDList.add(nextLine);
						}
                		Log.info(CLASSNAME,
                				 "From dealIDFile: '" + dealIDFile
                				 + "', processing: '" + dealIDList.size()
                				 + "' deal(s).");
						DPSAdapter.processTradesFromList(dealIDList,ds);
					}

				}
				catch (IOException ie) {
					ds.disconnect();
            		ie.printStackTrace();
        		}
				catch (Exception e) {
					ds.disconnect();
					Log.error(CLASSNAME, "Unable to send trade with dealID " + nextLine + " to DPS, reason."
							+ e.getMessage());
            		e.printStackTrace();
				}
				ds.disconnect();
			}
		}
		System.out.println("DPSAdapter has completed....");
	}
}
