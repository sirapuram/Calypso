/*
* Transaction object
* Author: ps64022
*
*/

package com.citigroup.project.dps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Vector;

import calypsox.apps.trading.DomainValuesUtil;
import calypsox.apps.trading.TradeKeywordKey;
import calypsox.apps.trading.salesallocation.util.AllocationDelegate;
import calypsox.tk.bo.FeeType;
import calypsox.tk.core.CalypsoLogUtil;
import calypsox.tk.core.CitiCustomTradeData;
import calypsox.tk.core.DealCoverCustomData;
import calypsox.tk.product.CitiCreditSyntheticCDO;
import calypsox.tk.refdata.DomainValuesKey;
import calypsox.tk.refdata.DynamicPanelConstants;

import com.calypso.tk.bo.BOCache;
import com.calypso.tk.bo.Fee;
import com.calypso.tk.core.JDate;
import com.calypso.tk.core.JDatetime;
import com.calypso.tk.core.LegalEntity;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Product;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.mo.TradeFilter;
import com.calypso.tk.product.AssetSwap;
import com.calypso.tk.product.Bond;
import com.calypso.tk.product.CreditDefaultSwap;
import com.calypso.tk.product.Swap;
import com.calypso.tk.product.SwapLeg;
import com.calypso.tk.product.TerminationUtil;
import com.calypso.tk.product.TotalReturnLeg;
import com.calypso.tk.product.TotalReturnSwap;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.service.LocalCache;
import com.citigroup.apps.trading.tradeamendment.AmendReason;
import com.citigroup.project.dps.exception.DPSTransactionException;
import com.citigroup.project.migration.firmaccounts.IMigrateFirmAccount;
import com.citigroup.project.migration.service.CalypsoServices;
import com.citigroup.project.util.DateUtil;
import com.citigroup.project.util.StrUtil;
import com.citigroup.project.util.TribecaProperties;
import com.citigroup.project.util.XMLPrintUtil;
import com.citigroup.project.util.oasys.OasysUtil;
import calypsox.tk.core.sql.TPSEventHandlerSQL;

/**
*	Transaction object for OASYS processing <br>
* CAL-8988: Changing the way of reading the sales person name from short name to full name.
*/

/*
 * Change History (most recent first):
 * 		07/28/2009  bs83325		:   Added TPS logic to handle the events.
 * *    10/02/2009  ss78999     :   Added for oasys calypso map(AmendReason and AmendAction)
 *  	02/19/2009	ga18627		:	Added SNAC TRADE TransactionClassificationList in XML.(ISDA_MATRIX_TYPE)
 * * 	01/07/2009	ik76729		:	Added domain value keys for adding 3 new fields to Transaction tag. (AmendmentReason, AmendmentAction, AmendmentComment)
 * 		10/30/2008	hn74214		:	TCC Changes
 * 		08/12/2008	hn74214		:	DTCC Date Changes
		02/07/2008	bs80435		:	Added Entries required for Partial Assignment Project; init(), generateXML() is changed. 
*/

public class Transaction implements IDPSDataObject {

	protected int		Deal;
	protected int		TransactionID;
	protected String	ProductClass;
	protected int		MirroredTransaction;
	protected int		OriginationLink;
	protected int		TerminationLink;
	protected JDate		EntryDate;
	protected JDate		TradeDate;
	protected String 	ExecutionTime;//Added by Manjay for MiFID
	protected String    timeZone;     //Added by Manjay for MiFID
	protected long      executionTimeInMilli;//Added by Manjay for MiFID
	
	protected JDate		SettlementDate;
	protected JDate		ModificationEffectiveDate; // For Contract Revision
	protected JDate		EffectiveDate; // For Contract Revision
	protected String	TradeStatus;
	protected String	TradeTerminationReason;
	protected JDate		EarlyTerminationTradeDate;
	protected JDate		EarlyTerminationDate;
	protected JDate		EarlyTerminationSettlementDt;
	protected JDate		ActualTerminationDate;
	protected String	PartyRelationship;
	protected String	PartyMnemonic;
	protected String	CounterPartyMnemonic;
	protected String	PartyBranch;
	protected String	CounterPartyBranch;
	protected String	Trader;
	protected String	CreditOfficer;
	protected String	OperationsFile;
	protected String	FeeCalendars;
	protected String	FeeRollConvention;
	protected String	Location;
	protected String	AgencyDesk;
	protected double	BuyBackAmount;
	protected double	BuyBackPercentage;
	protected String	BuyBackCurrency;
	protected JDate		TerminationDate;
	protected JDatetime	LastEconomicModification;
	protected String 	LastEconomicModificationTZ;
	protected String	FirmRole;
	protected String	SalesPerson;
	protected String 	CalculationAgent;
	protected String	PrimarySystem;

	protected JDatetime	ActualTerminationTime;
	protected String  primaryTransactionID = null;
	protected ArrayList _productCorpuses = new ArrayList();
	protected ArrayList _fees = null;
	protected ArrayList _eValues = null;
	protected ArrayList _transactionNotes = null;
	protected CreditDefault _creditDefault = null;
	protected CreditSupport _creditSupport = null;
	protected RightToTerminate _rightToTerminate = null;
	protected ExternalTransactionCorrelation _externalTransactionCorr = null;
	protected MLEvent _mlEvent = null;
	protected TPSTradeEvent tpsTradeEvent = null;
	protected boolean isZTU_SUTF = false;
	protected ElectronicProcessing ep = null;
	protected String TradeAmendReason = null;
	protected boolean isPAEnabled = true;
	
	protected String TradeAmendAction = null;
	protected String TradeAmendComment = null;
			
	protected TransactionClassificationList theTransactionClassificationList = null;
	
	// Added by DS53697 to include TradeMedium, 
	// AggressorFlag and Trader Spread for Broker Fee Project
	protected String  TradeMedium;
	protected String  AggressorFlag;
	
	/*
	 * START-CHANGE || 17-01-2007 || O. Jayachandra Gupta || used to maintain
	 * datapoints and values
	 */

	//tag name to be added to XML rather than domain value of trade type
	private final String TRADE_TYPE_TAG = "CDXTradeType";
	
	//domain value loancds to be replaced with tag value
	private final String LOAN_CDS_DOMAIN_VALUE = "LoanCDS";
	
	//domain value loancds to be replaced with tag value
	private final String MUNI_CDS_DOMAIN_VALUE = "MuniCDS";
	
	//tag value to identify and change it to YYYYMMDD format
	private final String DATE_OF_ORIGINAL_CREDIT_AGREEMENT = "OrigCreditAgreementDate";

	//tag value to be added to tradetype tag rather than domain value
	private final String LOAN_CDS_TAG_VALUE ="LCDS";
	
	//tag value to be added to tradetype tag rather than domain value
	private final String MUNI_CDS_VALUE="CDS on muni";
	
	// tradeTypeTags arraylist contains all the tagnames that will be added to
	// XML
	private Vector tradeTypeTags = null;

	// tradeTypeValues arraylist contains all the tag values
	private Vector tradeTypeValues = null;

	// LEVEL string is used whether to add tag at this transaction level or not
	private String LEVEL = "T";

	/* END-CHANGE || 17-01-2007 */

	private static final String CLASSNAME = "Transaction";
	
	/* START-CHANGE || 02-05-2007 || Venkatesan.k || global variable declared to hold the keyword */
	protected String blockTradetoOasys=null;
	/* END-CHANGE || 02-05-2007 */

	
	protected boolean TCCEligibleSwitch = DomainValuesUtil.checkDomainValue(DomainValuesKey.TCC_ELIGIBLE_SWITCH, "true", true); // TCC Changes - hn74214
	protected boolean AmendmentDetails = DomainValuesUtil.checkDomainValue(DomainValuesKey.AMENDMENT_DETAILS_SWITCH_OFF, "true", true); // Added AmendmentReason, AmendmentAction, AmedmentComment - ik76729
	
	protected boolean snacTradeSwitch = DomainValuesUtil.checkDomainValue(DomainValuesKey.SNAC_TRADE_SWITCH, "true", true); // SNAC Trade Changes - ga18627
	
	static
	{
		CalypsoLogUtil.adjustLogCategory(CLASSNAME);
	}
	/**
	* Init function for Transaction object
	* @param args    The <code>args</code> currently NULL
	*/
	public void init(List args) {
		
		if ( args != null && ! args.isEmpty() )
		{
				//
				// retrieve whether PA is enabled or not 
				//
			Boolean b = (Boolean) args.get(0);
			isPAEnabled = b.booleanValue();
		}
	}

	/**
	* Set function for Transaction object
	* @param trade    The <code>Trade</code> to set transction data
	* @param Object   The <code>Object</code> currently NULL
	*/
	public void set(Trade trade, Object o) {

		DealCoverCustomData dcCustomData =
		((CitiCustomTradeData)trade.getCustomData()).getDealCoverCustomData();

  		/*
		 * START-CHANGE || 17-01-2007 || O. Jayachandra Gupta || to get values

		 * from trade keyword
		 */
		setTradeTypeValues(trade);
		/* END-CHANGE || 17-01-2007 */
	
		/* START-CHANGE || 02-05-2007 || Venkatesan.k || to check for the trade keywordkey : BLOCK_TRADE_TO_OASYS */
		//blockTradetoOasys=trade.getKeywordValue(TradeKeywordKey.BLOCK_TRADE_TO_OASYS);
	
		if (trade.getProductType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE)) {
			if (!((CitiCreditSyntheticCDO)trade.getProduct()).isSendToOasys())
				 blockTradetoOasys = "Yes";
		}
			
		/* END-CHANGE || 02-05-2007 */

		Deal = dcCustomData.getDealID();
		TransactionID = dcCustomData.getTransactionID();

		Log.info(CLASSNAME,
				 "TradeId: " + trade.getId()
				 + " - About to process Transaction " + Deal + ":" + TransactionID);
	
		Log.info(CLASSNAME,"TCC Eligible Switch is "+TCCEligibleSwitch);
		
		ProductClass = CalypsoServices.getProductClass(trade.getProductType());
		
		Log.info(CLASSNAME, "ProductClass is: " + ProductClass);
		
		try
		{
		
		   TradeDate = Util.stringToJDate(Util.datetimeToString2(trade.getTradeDate()));
		}
		catch (Exception ex)
		{
			throw new DPSTransactionException("Unable to convert TradeDate: " + TradeDate, ex);
		}
  	    
		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				 + " - About to Set the EntryDate");

		
		try
		{
			
		  if (trade.getEnteredDate().getJDate(Util.getReferenceTimeZone()).before(
			  	trade.getTradeDate().getJDate(Util.getReferenceTimeZone()))) {
			  EntryDate = TradeDate;
			  Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - set EntryDate: '"+trade.getEnteredDate()+ "' to TradeDate: "+TradeDate.getJDatetime());
		  }
		  else
		  {
			EntryDate = Util.stringToJDate(Util.datetimeToString2(trade.getEnteredDate()));
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - set EntryDate: '"+trade.getEnteredDate()+ "' to actual Trade Entered Date of: "
					  + EntryDate);
		
		  }
		}
		catch (Exception ex)
		{
			  throw new DPSTransactionException("Error setting EntryDate from either the TradeDate: " 
			  		                            + TradeDate
												+ " or Trade Entered Date: " + trade.getEnteredDate()
												, ex);
		}              
		
		SettlementDate = trade.getSettleDate();
		TradeStatus = CalypsoServices.getTradeStatus(trade.getStatus().toString());

		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				 + " - About to Set the ActualTerminationDate.");
		
		if(trade.getKeywordValue(TradeKeywordKey.ACTUAL_TERMINATION_DATE) != null) 
		{
			ActualTerminationDate = new JDate(DateUtil.strToDate(
				trade.getKeywordValue(TradeKeywordKey.ACTUAL_TERMINATION_DATE)),TimeZone.getDefault());
			  Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - set the ActualTerminationDate to: " 
					 + trade.getKeywordValue(TradeKeywordKey.ACTUAL_TERMINATION_DATE));
	
		}
		else if(trade.getTerminationDate() != null 
				&& !trade.getStatus().toString().equals(calypsox.tk.core.TradeStatus.EARLYTERMINATED )) 
		{
			ActualTerminationDate = trade.getTerminationDate();
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - set the ActualTerminationDate to the Trade Termination Date: "
					  + trade.getTerminationDate());
				
		}
		else
		{
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					+ " ActualTerminationDate not set");
			
		}

		TerminationDate = trade.getProduct().getMaturityDate();
		FirmRole = MappingConstants.FIRMROLE;

		// Contract Revision event
		String contractRevisionEvent = OasysUtil.getAmendmentEvent(trade);
		if(/*contractRevisionEvent != null && */DPSAdapter.CONTRACT_REVISION == false) {
			_mlEvent = new MLEvent();
			
			/**
			 * Handling EVENT Started
			 */
			
			if(TPSEventHandlerSQL.isTPSTrade(trade))
			{
				System.out.println("TPS: TPS Trade");
				boolean isVoided = TPSEventHandlerSQL.isVoided(trade);
				System.out.println("TPS: TPS Trade isVoided"+isVoided);
				String eventID = TPSEventHandlerSQL.getEventID(trade);
				String subType = TPSEventHandlerSQL.getEventSubType(trade);
				String calypsoSubType = TPSEventHandlerSQL.getCalypsoEventType(subType);

				if(!TPSEventHandlerSQL.isTradeExist(trade.getId()))
				{
					Log.info(CLASSNAME,"TPS: TPS Trade exist before false");
					TPSEventHandlerSQL.handleTradeEventInfo(trade.getId(), eventID);
					 
					 /*
					  * If its Partial/Full buy back then set the event
					  */
					 if(TPSEventHandlerSQL.isSettingEventRequired(subType))
					 {
						 if(!isVoided)
							 _mlEvent.set(trade,calypsoSubType);
					 }
					 
				 }else{
					 Log.info(CLASSNAME,"TPS: TPS Trade exist before true");
					 String eventIDFromSideTbl = TPSEventHandlerSQL.getEventId(trade.getId());
					 if(!eventIDFromSideTbl.equalsIgnoreCase(eventID))
					 {
						 Log.info(CLASSNAME,"TPS: TPS Trade exist Event ID different");	 
						 if(!isVoided)
							 _mlEvent.set(trade,calypsoSubType);
						 TPSEventHandlerSQL.handleTradeEventInfo(trade.getId(), eventID);
					 }
				}
				/*
				 * If trade is from TPS and its voided then remove the record from the side table.
				 */
				if(isVoided)
				{
					TPSEventHandlerSQL.removeTradeEventInfo(trade.getId());
				}
				
				ModificationEffectiveDate =
					Util.istringToJDate(trade.getKeywordValue(TradeKeywordKey.TPS_AMENDMENT_EFF_DATE));//DTCC Date changes
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the ModificationEffectiveDate to the AmendmentDate : " 
						 + trade.getKeywordValue(TradeKeywordKey.TPS_AMENDMENT_EFF_DATE));

				EffectiveDate = Util.istringToJDate(trade.getKeywordValue(TradeKeywordKey.TPS_AMENDMENT_TRADE_DATE));
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the ModificationEffectiveDate to the AmendmentDate : " 
						 + trade.getKeywordValue(TradeKeywordKey.TPS_AMENDMENT_TRADE_DATE));
			}else{
				
				TPSEventHandlerSQL.removeTradeEventInfo(trade.getId());
				
				_mlEvent.set(trade, (Util.isEmptyString(contractRevisionEvent)) ? trade.getKeywordValue(TradeKeywordKey.AMENDMENT_REASON) : contractRevisionEvent);
			}
			
			/**
			 * Handling EVENT ended
			 */
			_mlEvent.set(trade, (Util.isEmptyString(contractRevisionEvent)) ? trade.getKeywordValue(TradeKeywordKey.AMENDMENT_REASON) : contractRevisionEvent);

			if(trade.getKeywordValue(TradeKeywordKey.AMENDMENT_DATE) != null) {
				ModificationEffectiveDate =
					Util.istringToJDate(trade.getKeywordValue(TradeKeywordKey.AMENDMENT_DATE));//DTCC Date changes
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the ModificationEffectiveDate to the AmendmentDate : " 
						 + trade.getKeywordValue(TradeKeywordKey.AMENDMENT_DATE));
					
			}
			else {
				ModificationEffectiveDate = JDate.getNow();//DTCC Date changes
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the ModificationEffectiveDate to NOW : "
						 + JDate.getNow());
			
			}
			if (trade.getKeywordValue(TradeKeywordKey.AMENDMENT_TRADE_DATE) != null) {
				EffectiveDate = Util.istringToJDate(trade.getKeywordValue(TradeKeywordKey.AMENDMENT_TRADE_DATE));
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the EffectiveDate to the AmendmentTradeDate : " 
						 + trade.getKeywordValue(TradeKeywordKey.AMENDMENT_TRADE_DATE));

			} else {
				EffectiveDate = JDate.getNow();
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - set the EffectiveDate to NOW : "
						 + JDate.getNow());
			}

			// DPS sets SettlementDate for CR, so at the moment no need to set here.
			//if(contractRevisionEvent.equals(MappingConstants.DPS_ASSIGNMENT)) {
				//SettlementDate = EffectiveDate;
			//}
		}

		if(trade.getMirrorBook() != null)
			PartyRelationship = MappingConstants.PARTY_RELATIONSHIP_FIRM;
		else
			PartyRelationship = MappingConstants.PARTY_RELATIONSHIP_CUSTOMER;

		PartyMnemonic = trade.getBook().getName();
		/******* Original code used for stripping _BO begins **********/

		/* if(trade.getCounterParty().hasRole(LegalEntity.COUNTERPARTY)) {

			CounterPartyMnemonic = CalypsoServices.getLegalEntityCodeForExternalSys(
													trade.getCounterParty().getCode());
		}
		else {
			CounterPartyMnemonic = trade.getCounterParty().getCode();
			System.out.println("CounterPartyMnemonic does not have a role ' CounterParty ' in database: "+CounterPartyMnemonic);
		}
		*/
		/******* Original code for stripping _BO ends **********/


		/*** CALYPSO INC CODE FOR STRIPING _BO ADDED ON JAN 10, 2006 BEGINS *********/
		/** AUTHOR: ANKIT SHARMA, AVNEESH DATTA ****/

		/*** CALYPSO READONLY CODE STARTS - FOR DEBUGGING PURPOSES *********/

		if(trade.getCounterParty().getRoleList().size() == 0) {
			System.out.println("Missing role list. Investigating....");
			LegalEntity tradeLE = trade.getCounterParty();
			LegalEntity cacheByIdLE = BOCache.getLegalEntity(DSConnection.getDefault(), tradeLE.getId());
			LegalEntity cacheByNameLE = BOCache.getLegalEntity(DSConnection.getDefault(), tradeLE.getName());
			if(cacheByIdLE != cacheByNameLE) {
				System.out.println("The LE by id and the LE by name in the cache do not return the same instance");
			}
			if(cacheByIdLE != tradeLE) {
				System.out.println("The LE by id from cache and the LE in the trade are not the same instance");
			}
			if(cacheByNameLE != tradeLE) {
				System.out.println("The LE by name from the cache and the LE in the trade are not the same instance");
			}
		}
		/*** CALYPSO READONLY CODE ENDS *********/

		if(trade.getCounterParty().getCode().endsWith("_BO"))
		{
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - About to strip _BO from legal entity: "
					 + trade.getCounterParty().getCode());
			CounterPartyMnemonic = CalypsoServices.getLegalEntityCodeForExternalSys(trade.getCounterParty().getCode());
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + "Stripped _BO. New legal entity is : "+CounterPartyMnemonic);
		}
		else {
			CounterPartyMnemonic = trade.getCounterParty().getCode();
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					+  " Legal Entity do not have _BO to be stripped: "+CounterPartyMnemonic);
		
		}

		/*** CALYPSO INC CODE FOR STRIPING _BO ADDED ON JAN 10, 2006 ENDS *********/

		Trader = trade.getTraderName();
		SalesPerson = trade.getSalesPerson();
		LastEconomicModification = trade.getUpdatedTime();
		LastEconomicModificationTZ = Util.getReferenceTimeZone().getID();

		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				 + " - About to Set the Location");
		
		if(trade.getKeywordValue(TradeKeywordKey.LOCATION) != null) {
			Location = trade.getKeywordValue(TradeKeywordKey.LOCATION);
					
		}
		else if(trade.getBook().getAttribute(IMigrateFirmAccount.CITI_LOCATION) != null) {
			Location = trade.getBook().getAttribute(
									IMigrateFirmAccount.CITI_LOCATION);
		}
		else {
			Location = MappingConstants.NEW_YORK;
		}

		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				+ " - Location has been set to " + Location);
		
		OperationsFile = dcCustomData.getOperationsFile();

		if(trade.getMirrorTradeId() > 0) {

			try {
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						+ " - About to get mirror trade from MirrorTradeId: "
						+ trade.getMirrorTradeId());
				
				Trade mirrorTrade = DSConnection.getDefault().getRemoteTrade().
									getTrade(trade.getMirrorTradeId());

				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						+ " - successfully got a mirror trade with ID: "
						 + mirrorTrade.getId());
			
				
				DealCoverCustomData mdcCustomData =
					((CitiCustomTradeData)mirrorTrade.getCustomData()).
											getDealCoverCustomData();

				MirroredTransaction = mdcCustomData.getTransactionID();
			}
			catch (RemoteException e) {
				throw new DPSTransactionException("Could not get Mirror Trade from a MirrorTradeId of '" 
						                          + trade.getMirrorTradeId() + "'"
												  , e);
            }
		}

		if(	trade.getProductType().equals(Product.CREDITDEFAULTSWAP) || 
			trade.getProductType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE)) {
			int calcAgentId = ((CreditDefaultSwap)trade.getProduct()).getCalcAgentId();

			if(calcAgentId > 0) {

				try {
				CalculationAgent = DSConnection.getDefault().getRemoteReferenceData().
										getLegalEntity(calcAgentId).getCode();
				}
				catch (RemoteException e) {
                	throw new DPSTransactionException("Could not get LegalEntity from calcAgentId of '" + calcAgentId + "'"
                	  		                      , e);
                                   	  		                      
            	}
			}
		}

//		 Set ExternalTransactionCorrelation information
		_externalTransactionCorr = new ExternalTransactionCorrelation();
		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				+ " - About to set ExternalTransactionCorrelation Data.");
	
		// Set Transaction Keywords
		setTransactionKeyWords(trade);

		if(trade.getProductType().equals(Product.CREDITDEFAULTSWAP)) {
            _creditDefault = new CreditDefault();
            Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					+ " - About to set CreditDefault Data.");
		
            _creditDefault.set(trade,o);
            
            Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					+ " - Successfully set CreditDefault Data.");
        } else if(trade.getProductType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE)) {
            _creditDefault = new CitiCDODataObject();
            if(Log.isInfo()) {
                StringBuffer msg = new StringBuffer();
                msg.append("TradeId = ").append(trade.getId());
                msg.append(" - About to set CitiCDODataObject Data.");                
                Log.info(CLASSNAME, msg.toString());
        	}

            _creditDefault.set(trade,o);
            
            if(Log.isInfo()) {
                StringBuffer msg = new StringBuffer();
                msg.append("TradeId = ").append(trade.getId());
                msg.append(" - Successfully set CitiCDODataObject Data.");                
                Log.info(CLASSNAME, msg.toString());
            }
        }

		_externalTransactionCorr.set(trade, o);

		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				+ " - Successfully set ExternalTransactionCorrelation Data.");
		
		// Set CashFlowCorpus(leg) information
		Product p = trade.getProduct();
		ArrayList legsArrayList = null;

		if(p.getType().equals(Product.CREDITDEFAULTSWAP) ||
            p.getType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE) ||                
			p.getType().equals(Product.SWAP) ||
			p.getType().equals(Product.ASSETSWAP) ||
			p.getType().equals(Product.TOTALRETURNSWAP)) {

			legsArrayList = getCashFlowCorpusLegs(p);

			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - About to Set CashFlowCorpus Data on "
					 + legsArrayList.size()
					 + " Leg(s).");
			
			for(int i = 0; i < legsArrayList.size(); i++) {
				Object obj = legsArrayList.get(i);
				CashFlowCorpus cashFlowCorpus = new CashFlowCorpus();
				cashFlowCorpus.set(trade,obj);
				_productCorpuses.add(cashFlowCorpus);
			}
			
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - successfully set CashFlowCorpus Data");
			
		}

		Vector feeVector =  trade.getFees();

		if(feeVector != null) {

			_fees = new ArrayList();
			_eValues = new ArrayList();

			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					+ " - Found Trade Fees, about to set ExpectedValues on "
					+ feeVector.size()
					 + " fees.");
			
			
			for(int i = 0; i < feeVector.size(); i++) {

				Object obj = feeVector.elementAt(i);

				Fee fee = (Fee) obj;

				if(fee.getType().equals(FeeType.EXPECTED_VALUE)) {
					Log.info(CLASSNAME,
							"TradeId = " + trade.getId()
							 + " - About to set ExpectedValue Data on trade");
					
					ExpectedValue ev = new ExpectedValue();
					ev.set(trade,obj);
					
					Log.info(CLASSNAME,
							"TradeId = " + trade.getId()
							 + " - Successfully set ExpectedValue Data.");
					
					_eValues.add(ev);

					//if(SalesPerson == null || SalesPerson.equals(" ")) {
						try {

						LegalEntity le = BOCache.getLegalEntity(DSConnection.getDefault(),
															fee.getLegalEntityId());
							if(le != null){
								//SalesPerson = le.getCode();
								/*
								 *  CAL-8988: Changing the way of reading the sales person name from short name to full name.
								 */
								SalesPerson = le.getName();
							}
								
								Log.info(CLASSNAME,
									"TradeId = " + trade.getId()
									 + " - Set the SalesPerson to: "
									  + SalesPerson);
							
						}
						catch (Exception e) 
						{ 
							throw new DPSTransactionException("Could not get LegalEntity from Id of '" + fee.getLegalEntityId() + "'"
			                	  		                      , e);
			                  
						}
					//}
				}
		        else 
		        {
		        	
		        	// Before adding a Fee, let's check to see if the property, 
		        	// 'DPS.Fee.Restrict.Type' has been assigned a value. If so, we need to 
		        	// check whether the fee type on the trade is not equal to the fee type 
		        	// set on the property. 
		        	// This is initially implemented for Brokerage fees for ECD trades.
		        	 
		        	String restrictedFee = TribecaProperties.DPS_FEE_RESTRICT_TYPE;
		        	
		        	// If the property has not been defined or has no restriction then add the fee
		        	if (restrictedFee == null
		        			|| (restrictedFee != null && restrictedFee.length() ==0))
		        	{
		        		addFee(trade, obj);
		        		continue;
		        	}
		        	 
		        	
		        	TradeFilter restrictedTradeFilter = null;
					String tradeFilterStr = TribecaProperties.DPS_FEE_RESTRICT_TRADEFILTER;

					// A value has been set, first get the trade filter from
					// the property,'DPS.Fee.Restrict.TradeFilter' - this should be the ECD
					// trade filter
					restrictedTradeFilter = BOCache.getTradeFilter(DSConnection.getDefault(), tradeFilterStr);

					if (restrictedTradeFilter == null)
					{
						throw new DPSTransactionException("Unable to obtain tradeFilter: '" + tradeFilterStr
								+ "'.\nCheck tribeca.properties has the correct TradeFilter"
								+ " for property: 'DPS.Fee.Restrict.TradeFilter'");
					}

					Log.info(CLASSNAME, "TradeId: " + trade.getId() + " - Checking trade does not have a fee type of: "
							+ restrictedFee + " and is not part of TradeFilter: " + tradeFilterStr
							+ ". If so, the fee value will be set to zero.");

					// The fee type matches and the trade is part of the trade filter property, 
					// so do not add the fee..
					if (fee.getType().equals(restrictedFee) 
							 && restrictedTradeFilter.accept(trade))
					{
						Log.info(CLASSNAME, "TradeId: " + trade.getId() + " - changing Fee value from : " + fee.getAmount()
								+ " to 0 (zero), as it is a fee type of: " + fee.getType()
								+ " and the trade is part of the restricted TradeFilter: " + tradeFilterStr);

						// pg53529 - changed so Broker fees falling under the filter now not deleted but set to zero...	
						fee.setAmount(0);
										
					}
										 
					addFee(trade, obj);
		        }
					         	
		        
			}
		}

		// Set Single Premium Fee information
		if(	trade.getProductType().equals(Product.CREDITDEFAULTSWAP) ||
			trade.getProductType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE)) {
			CreditDefaultSwap cds = (CreditDefaultSwap) trade.getProduct();
			if(cds.getSinglePremiumPaymentB()) {

				TradeFee tradeFee = new TradeFee();
				tradeFee.setSinglePremiumFee(trade);

				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - Set a Single Premium Fee");
				
				
				if(_fees == null) {
					_fees = new ArrayList();
				}
				_fees.add(tradeFee);
			}
		}

		// Set Right To Terminate info
		if(OasysUtil.isRightToTerminate(trade)) {
			Object obj = null;

			if(legsArrayList != null && legsArrayList.size() > 0) {
				obj = legsArrayList.get(0);
			}
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " About to set RightToTerminate data");
			
			_rightToTerminate = new RightToTerminate();
			_rightToTerminate.set(trade,obj);
			
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - successfully set RightToTerminate data");
		}
		// Set CreditSupport
		if(OasysUtil.isInitialMarginRequired(trade)) {
			_creditSupport = new CreditSupport();
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - About to set IM data on the CreditSupport object");
		
			_creditSupport.set(trade,null);
			
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - successfully set IM data on the CreditSupport object");
		}

		// Set TransactionNote
		if(OasysUtil.isCRNoteRequired(trade) || OasysUtil.isMiscNoteRequired(trade)) {
			_transactionNotes = new ArrayList();

			if(OasysUtil.isCRNoteRequired(trade)) {
				TransactionNote crNote = new TransactionNote();
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - About to set 'CRNote' on TransactionNote data.");
				
				crNote.set(trade,"CRNote");
				_transactionNotes.add(crNote);
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - successfully set 'CRNote' on TransactionNote data.");
				
			}
			if(OasysUtil.isMiscNoteRequired(trade)) {
				TransactionNote miscNote = new TransactionNote();
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - About to set 'MiscNote' on TransactionNote data.");
				
				miscNote.set(trade,"MiscNote");
				_transactionNotes.add(miscNote);
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - successfully set 'MiscNote' on TransactionNote data.");
				
			}
		}
		
		// Start of TCC Changes - hn74214 
		if(TCCEligibleSwitch){
			ep = new ElectronicProcessing();
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - About to set ElectronicProcessing data.");
		
			ep.set(trade,null);
			Log.info(CLASSNAME,
					"TradeId = " + trade.getId()
					 + " - successfully set ElectronicProcessing data.");
			if(AllocationDelegate.isValidForAllocation(trade))
			{
					isZTU_SUTF = true;
			}
		}else{ // Roll back changes 
			if(AllocationDelegate.isValidForAllocation(trade))
			{
					isZTU_SUTF = true;
					ep = new ElectronicProcessing();
					Log.info(CLASSNAME,
							"TradeId = " + trade.getId()
							 + " - About to set ElectronicProcessing data.");
				
					ep.set(trade,null);
					Log.info(CLASSNAME,
							"TradeId = " + trade.getId()
							 + " - successfully set ElectronicProcessing data.");
			}
		}
		// End of TCC Changes - hn74214
		
		
		// Guru changes for SNAC trade Changes 
		if(snacTradeSwitch)
		{
			if (!Util.isEmptyString(trade.getKeywordValue(TradeKeywordKey.ISDA_MATRIX_TYPE)))
			{
				theTransactionClassificationList = new TransactionClassificationList();
				Log.info(CLASSNAME,	"TradeId = " + trade.getId()
						 + " - About to set transactionClassification data.");
			
				theTransactionClassificationList.set(trade,null);
				Log.info(CLASSNAME,
						"TradeId = " + trade.getId()
						 + " - successfully set transactionClassification data.");	
				
				
			}
			
		}
		
		tpsTradeEvent = new TPSTradeEvent();
		tpsTradeEvent.set(trade, "");
		
		
		
		
		
	}


	/**
	* GenerateXML function for Transaction object
	* @return     <code>String</code> containing XML
	*/
	public String generateXML() {

		StringBuffer xmlStringBuf = new StringBuffer(XMLPrintUtil.XML_STRING_SIZE);
		HashMap trnHashMap = new HashMap();

		xmlStringBuf.append(XMLPrintUtil.createXMLTag("Transaction",
										key(),"Attribute"));

		xmlStringBuf.append(XMLPrintUtil.createXMLTag("Deal",
										String.valueOf(Deal),"Key"));

		xmlStringBuf.append(XMLPrintUtil.createXMLTag("TransactionID",
						String.valueOf(TransactionID),"Key"));
	  		/*
		 * START-CHANGE || 17-01-2007 || O. Jayachandra Gupta || to add trade
		 * type values to XML
		 */
		addTradeTypeTagsToXML(trnHashMap);
		/* END-CHANGE || 17-01-2007 */

		trnHashMap.put("ProductClass",ProductClass);

		trnHashMap.put("EntryDate",DateUtil.getDateYYYYMMdd(EntryDate.getDate()));

		trnHashMap.put("TradeDate",DateUtil.getDateYYYYMMdd(TradeDate.getDate()));
		
		//Added by Manjay for MiFID
		trnHashMap.put("TradeTime",new Long(executionTimeInMilli).toString());
		trnHashMap.put("TradeTimeTimeZone",timeZone);
		
		trnHashMap.put("SettlementDate",DateUtil.getDateYYYYMMdd(SettlementDate.getDate()));
		trnHashMap.put("TradeStatus",TradeStatus);
		trnHashMap.put("Location",Location);
		
		/* START-CHANGE || 02-05-2007 || Venkatesan.k || to create a tag based on trade keywordkey */
		if(blockTradetoOasys!=null && !blockTradetoOasys.trim().equals("")) {
			trnHashMap.put(TradeKeywordKey.BLOCK_TRADE_TO_OASYS, "Yes");
		}
		/* END-CHANGE || 02-05-2007 */
		
		// MLEvent set then we must pass EffectiveDate
		if(_mlEvent != null) {
			trnHashMap.put("EffectiveDate",DateUtil.getDateYYYYMMdd(EffectiveDate.getDate()));
			trnHashMap.put("ModificationEffectiveDate",DateUtil.getDateYYYYMMdd(ModificationEffectiveDate.getDate()));// DTCC Date changes
		}

		if(CounterPartyBranch != null && !CounterPartyBranch.equals(" "))
			trnHashMap.put("CounterPartyBranch",CounterPartyBranch);

		if(primaryTransactionID != null ){
			trnHashMap.put("PrimaryTransactionID",primaryTransactionID);
		}
		if(ActualTerminationDate != null) {

			trnHashMap.put("ActualTerminationDate",
					DateUtil.getDateYYYYMMdd(ActualTerminationDate.getDate()));
		}

		if(TerminationDate != null) {
			trnHashMap.put("TerminationDate",
					DateUtil.getDateYYYYMMdd(TerminationDate.getDate()));
		}
		if(EarlyTerminationDate != null) {
			trnHashMap.put("EarlyTerminationDate",
				DateUtil.getDateYYYYMMdd(EarlyTerminationDate.getDate()));
		}
		if(EarlyTerminationTradeDate != null) {
					trnHashMap.put("EarlyTerminationTradeDate",
						DateUtil.getDateYYYYMMdd(EarlyTerminationTradeDate.getDate()));
		}
		/* changed per CAL3911
		 * EarlyTerminationSettlementDt = EarlyTerminationDate = T+1, 
		 * not EarlyTerminationTradeDate which is T
		 * 
		 */
		if(EarlyTerminationSettlementDt != null) {
			trnHashMap.put("EarlyTerminationSettlementDt",
				DateUtil.getDateYYYYMMdd(EarlyTerminationDate.getDate()));
		}

		trnHashMap.put("PartyRelationship",PartyRelationship);
		trnHashMap.put("PartyMnemonic",PartyMnemonic);
		trnHashMap.put("CounterPartyMnemonic",CounterPartyMnemonic);

		if(Trader != null && !Trader.equals(" "))
			trnHashMap.put("Trader",Trader);

		if(SalesPerson != null && !SalesPerson.equals(" "))
			trnHashMap.put("SalesPerson",SalesPerson);

		trnHashMap.put("CreditOfficer",CreditOfficer);
		trnHashMap.put("OperationsFile",OperationsFile);
		trnHashMap.put("CalculationAgent",CalculationAgent);
		trnHashMap.put("TradeTerminationReason",TradeTerminationReason);
		trnHashMap.put("FeeCalendars",FeeCalendars);
		trnHashMap.put("FeeRollConvention",FeeRollConvention);
		trnHashMap.put("AgencyDesk",AgencyDesk);

		if(TerminationLink > 0) {
			trnHashMap.put("TerminationLink",String.valueOf(TerminationLink));
		}
		/*if(OriginationLink > 0) {
			trnHashMap.put("OriginationLink",String.valueOf(OriginationLink));
		}*/
		if(MirroredTransaction > 0) {
			trnHashMap.put("MirroredTransaction",String.valueOf(MirroredTransaction));
		}


		//String strLastEconomicModification = DateUtil.getDateTimeAsStr(LastEconomicModification);

		Log.debug(CLASSNAME,
				"[DateUtil.modifyTimeStamp(DateUtil.getDateTimeAsStr(LastEconomicModification))]" + DateUtil.modifyTimeStamp(DateUtil.getDateTimeAsStr(LastEconomicModification)));
		String strLastEconomicModification = DateUtil.modifyTimeStamp(DateUtil.getDateTimeAsStr(LastEconomicModification));

		Log.debug(CLASSNAME,"[strLastEconomicModification]" + strLastEconomicModification);

		String modifiedDate = new String(strLastEconomicModification);


		Log.debug(CLASSNAME, 
				"LastEconomicModification: " + LastEconomicModification + " Str:" + modifiedDate);
		trnHashMap.put("LastEconomicModification",modifiedDate );
		//trnHashMap.put("LastEconomicModification",DateUtil.getDateTimeAsStr(LastEconomicModification));
		trnHashMap.put("LastEconomicModificationTZ",LastEconomicModificationTZ);

		
		if(AmendmentDetails)
		{
			if ( ! Util.isEmptyString(TradeAmendReason))
			{	
				trnHashMap.put("AmendmentReason", TradeAmendReason);	
			}
			if ( ! Util.isEmptyString(TradeAmendAction))
			{	
				trnHashMap.put("AmendmentAction", TradeAmendAction);	
			}
			if ( ! Util.isEmptyString(TradeAmendComment))
			{	
				trnHashMap.put("AmendmentComment", TradeAmendComment);	
			}		
			
		}
		else
		{
			if ( isPAEnabled )
			{
				if ( ! Util.isEmptyString(TradeAmendReason) 
						&& TradeAmendReason.equals(AmendReason.CANCEL_PARTIAL_ASSIGNMENT) 
					)
				{	
					trnHashMap.put("AmendmentReason", TradeAmendReason);	
				}
			}		
		}
		
		//Broker Fee Project
		trnHashMap.put("BrokerTradeMode", TradeMedium);
		trnHashMap.put("BrokerCalculationMode", AggressorFlag);
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTagsFromMap(trnHashMap));

		if(_creditDefault != null) {
			xmlStringBuf.append(_creditDefault.generateXML());
		}

		xmlStringBuf.append(XMLPrintUtil.createXMLTag(
					"ExternalTransactionCorrelation",key(),"List"));
		xmlStringBuf.append(_externalTransactionCorr.generateXML());
		xmlStringBuf.append(XMLPrintUtil.createClosingTag(
								"ExternalTransactionCorrelationList"));

		if(_fees != null && _fees.size() != 0) {

			xmlStringBuf.append(XMLPrintUtil.createXMLTag("Fee",key(),"List"));

			for (int i = 0; i < _fees.size(); i++) {
				TradeFee tradeFee = (TradeFee) _fees.get(i);
				xmlStringBuf.append(tradeFee.generateXML());
			}
			xmlStringBuf.append(XMLPrintUtil.createClosingTag("FeeList"));
		}

		if(_eValues != null && _eValues.size() != 0) {

			xmlStringBuf.append(XMLPrintUtil.createXMLTag("ExpectedValue",key(),"List"));

			for (int i = 0; i < _eValues.size(); i++) {
				ExpectedValue ev = (ExpectedValue) _eValues.get(i);
				xmlStringBuf.append(ev.generateXML());
			}
			xmlStringBuf.append(XMLPrintUtil.createClosingTag("ExpectedValueList"));
		}

		// Initial Margin
		if(_creditSupport != null) {
			xmlStringBuf.append(_creditSupport.generateXML());
		}

		// Right To Terminate
		if(_rightToTerminate != null) {
			xmlStringBuf.append(_rightToTerminate.generateXML());
		}

		//	ProductCorpus list
		xmlStringBuf.append(XMLPrintUtil.createXMLTag("ProductCorpus", key(),"List"));
		for (int i = 0; i < _productCorpuses.size(); i++) {
			CashFlowCorpus cashFlowCorpus = (CashFlowCorpus) _productCorpuses.get(i);
			xmlStringBuf.append(cashFlowCorpus.generateXML());
		}
		xmlStringBuf.append(XMLPrintUtil.createClosingTag("ProductCorpusList"));

		// TransactionNote list
		if(_transactionNotes != null) {
			xmlStringBuf.append(XMLPrintUtil.createXMLTag("TransactionNote", key(),"List"));
			for (int i = 0; i < _transactionNotes.size(); i++) {
				TransactionNote transactionNote = (TransactionNote) _transactionNotes.get(i);
				xmlStringBuf.append(transactionNote.generateXML());
			}
			xmlStringBuf.append(XMLPrintUtil.createClosingTag("TransactionNoteList"));
		}

		// MLEvent
		if(_mlEvent != null) {
			xmlStringBuf.append(_mlEvent.generateXML());
		}

		// Start of TCC Changes - hn74214
		if(TCCEligibleSwitch){
			if(isZTU_SUTF){
				xmlStringBuf.append(ep.generateXML());
			}else{
				xmlStringBuf.append(ep.generateTccXML());
			}
		}else if (isZTU_SUTF){ // Original Cond.
			xmlStringBuf.append(ep.generateXML());
		}
		// End of TCC Changes - hn74214

		// Guru changes for SNAC trade Changes 
		if(snacTradeSwitch)
		{
			if (theTransactionClassificationList!=null)
			{
				xmlStringBuf.append(theTransactionClassificationList.generateXML());
			}
		}
		
		if(tpsTradeEvent!=null)
		{
			xmlStringBuf.append(tpsTradeEvent.generateXML());
		}
		
		xmlStringBuf.append(XMLPrintUtil.createClosingTag("Transaction"));

		return xmlStringBuf.toString();
	}


	/**
	*  Key for Transaction object
	* @return  String <code>Key</code> for Transaction
	*/
	public String key() {
		return(new String(Deal + XMLPrintUtil.XML_KEY_SEPERATOR + TransactionID));
	}

	/**
	*  Function to get Product legs
	*  @param Product    The <code>Product</code> of a trade
	*  @return ArrayList The <code>ArrayList</code> of given Product legs
	*/
	public static ArrayList getCashFlowCorpusLegs(Product p) {

        ArrayList corpusLegList = new ArrayList();
        SwapLeg corpusLeg1 = null;
        SwapLeg corpusLeg2 = null;
		Bond	assetBond = null;
        TotalReturnLeg trorLeg = null;

       
        if(	p.getType().equals(Product.CREDITDEFAULTSWAP) || 
            p.getType().equals(CitiCreditSyntheticCDO.PRODUCT_TYPE)) {
            CreditDefaultSwap cdSwap = (CreditDefaultSwap) p;
            corpusLeg1 = (SwapLeg) cdSwap.getPremiumPayments();

            corpusLegList.add(corpusLeg1);

        }
        else if(p.getType().equals(Product.SWAP)) {
            Swap irSwap = (Swap) p;
            corpusLeg1 = (SwapLeg) irSwap.getPayLeg();
            corpusLeg2 = (SwapLeg) irSwap.getReceiveLeg();

            corpusLegList.add(corpusLeg1);
            corpusLegList.add(corpusLeg2);
        }
        else if(p.getType().equals(Product.ASSETSWAP)) {
            AssetSwap assetSwap = (AssetSwap) p;
            corpusLeg2 = (SwapLeg) assetSwap.getSwapLeg();
			assetBond = (Bond) assetSwap.getUnderlyingAsset();
			corpusLegList.add(assetBond);
            corpusLegList.add(corpusLeg2);
        }
        else if(p.getType().equals(Product.TOTALRETURNSWAP)) {
            TotalReturnSwap trSwap = (TotalReturnSwap) p;

            trorLeg = (TotalReturnLeg) trSwap.getTotalReturnLeg();
            corpusLeg2 = (SwapLeg) trSwap.getPremiumPayments();

            corpusLegList.add(trorLeg);
            corpusLegList.add(corpusLeg2);
        }

        return corpusLegList;
    }

	/**
	* Set Value for Transaction keyword
	* @param trade    The <code>Trade</code> to set keywords
	*/
	public void setTransactionKeyWords(Trade trade) {

		/*	DF 4/25/2007 - old code	
		 * if(trade.getKeywordValue(TradeKeywordKey.CREDIT_OFFICER) != null) {
			CreditOfficer = trade.getKeywordValue(TradeKeywordKey.CREDIT_OFFICER);
		}
		else {
			CreditOfficer = "Calypso Booking";
		}*/
		// DF 4/25/2007 - new code =>
		
		if (trade.getKeywordValue(TradeKeywordKey.CREDIT_OFFICER) != null) {
			CreditOfficer = trade
					.getKeywordValue(TradeKeywordKey.CREDIT_OFFICER);
		} else {
			// CreditOfficer = "Calypso Booking";
			// DF 4/25/2007 - Added ExternalSystem info to booking.
			if (_externalTransactionCorr.ExternalSystem.trim().equalsIgnoreCase("calypso")
					|| _externalTransactionCorr.ExternalSystem.trim().equalsIgnoreCase("unknown")) {
				CreditOfficer = "Calypso Booking";
			} else {
				CreditOfficer = _externalTransactionCorr.ExternalSystem.trim() + " Booking";
			}
			// DF end.
		}
		
		if(trade.getKeywordValue(TradeKeywordKey.AGENCY_DESK) != null) {
			AgencyDesk = trade.getKeywordValue(TradeKeywordKey.AGENCY_DESK);
		}

		if(trade.getKeywordValue("PrimaryTransactionID") != null) {
			primaryTransactionID = trade.getKeywordValue("PrimaryTransactionID");
		}

		if(trade.getKeywordValue(TradeKeywordKey.COUNTERPARTY_BRANCH) != null) {
			CounterPartyBranch = trade.getKeywordValue(TradeKeywordKey.COUNTERPARTY_BRANCH);
		}
		
		//Added by Manjay for MiFID
		if(trade.getKeywordValue(TradeKeywordKey.EXECUTION_TIME) != null) 
		{
			

			try
	        {
			ExecutionTime = trade.getKeywordValue(TradeKeywordKey.EXECUTION_TIME);
			String tradeTimeTimeZone[]=ExecutionTime.split(" ");
			
			timeZone=tradeTimeTimeZone[3];
			
			String tradeTime[]=tradeTimeTimeZone[1].split(":");


			int hour= Integer.parseInt(tradeTime[0]);
			int min = Integer.parseInt(tradeTime[1]);
			int sec = Integer.parseInt(tradeTime[2]);

			executionTimeInMilli= (hour*3600+min*60+sec)*1000;
	        }
			catch (Exception e)
			{
				e.printStackTrace();
				IllegalArgumentException ex = new IllegalArgumentException("Unable to set execution time.");
				ex.initCause(e);
				throw ex;
			}
		
			
		}
		if(trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_DATE) != null) {
			EarlyTerminationDate = new JDate(DateUtil.strToDate(
				trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_DATE)),TimeZone.getDefault());
		}
		if(trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_TRADEDATE) != null) {
			EarlyTerminationTradeDate = new JDate(DateUtil.strToDate(
				trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_TRADEDATE)),TimeZone.getDefault());
		}
		if(trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_SETTLEMENTDATE) != null) {
			EarlyTerminationSettlementDt = new JDate(DateUtil.strToDate(
				trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_SETTLEMENTDATE)),TimeZone.getDefault());
		}

		if(trade.getKeywordValue(TradeKeywordKey.TERMINATION_REASON) != null &&
			!trade.getKeywordValue(TradeKeywordKey.TERMINATION_REASON).equals(" ")) {

			TradeTerminationReason = OasysUtil.getTerminationReason(trade);
			
			if(TPSEventHandlerSQL.isTPSTrade(trade))
			{
				/**
				 * This is only happens when we do the full term and buy back
				 * once you do the buyback then only this process wants to set up the
				 * Termination reason as buyback/Termination
				 */
				String subType = TPSEventHandlerSQL.getEventSubType(trade);
				
				boolean isEventNeedSetUp = TPSEventHandlerSQL.isSettingEventRequired(subType);
				
				if(isEventNeedSetUp)
					TradeTerminationReason = TPSEventHandlerSQL.getCalypsoEventType(subType);
				
				EarlyTerminationDate = new JDate(DateUtil.strToDate(
						trade.getKeywordValue(TradeKeywordKey.TPS_TERMINATION_EFF_DATE)),TimeZone.getDefault());
				EarlyTerminationTradeDate = new JDate(DateUtil.strToDate(
						trade.getKeywordValue(TradeKeywordKey.TPS_TERMINATION_TRADE_DATE)),TimeZone.getDefault());
				EarlyTerminationSettlementDt = new JDate(DateUtil.strToDate(
						trade.getKeywordValue(TradeKeywordKey.TPS_TERMINATION_EFF_DATE)),TimeZone.getDefault());

			}
			if(TradeTerminationReason != null &&
				!TradeTerminationReason.equals(MappingConstants.TERMINATION_MATURED) &&
				trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_DATE) == null) {

				if(trade.getTerminationTradeDate()!=null){//KL 3/17/06 CAL3911
					EarlyTerminationDate = trade.getTerminationDate();
					EarlyTerminationTradeDate = trade.getTerminationTradeDate().getJDate(Util.getReferenceTimeZone());//need to have user default
					EarlyTerminationSettlementDt = EarlyTerminationDate;
				}//has TerminationTradeDate/TerminationDate on Screen
				else{
				EarlyTerminationDate = trade.getTerminationDate();
				EarlyTerminationTradeDate = EarlyTerminationDate;
				EarlyTerminationSettlementDt = EarlyTerminationDate;
				}//else
			}
		}
		else if(trade.getKeywordValue(TerminationUtil.TERMINATION_TYPE) != null) {
			if(trade.getKeywordValue(TerminationUtil.TERMINATION_TYPE).equals(
				TerminationUtil.FULL_TERMINATION)) {
				TradeTerminationReason = MappingConstants.TERMINATION_MANUAL;
			}
			else if(trade.getKeywordValue(TerminationUtil.TERMINATION_TYPE).equals(MappingConstants.TERMINATION_MATURED)) {
				TradeTerminationReason = MappingConstants.TERMINATION_MATURED;
			}
			else {
				TradeTerminationReason = MappingConstants.TERMINATION_PARTIAL_BUYBACK;
			}

			if(TradeTerminationReason != null &&
				!TradeTerminationReason.equals(MappingConstants.TERMINATION_MATURED) &&
				trade.getKeywordValue(TradeKeywordKey.EARLY_TERMINATION_DATE) == null) {

				if(trade.getTerminationTradeDate()!=null){ //KL 3/17/06 CAL3911
					EarlyTerminationDate = trade.getTerminationDate();
					EarlyTerminationTradeDate = trade.getTerminationTradeDate().getJDate(Util.getReferenceTimeZone());//need to have user default
					EarlyTerminationSettlementDt = EarlyTerminationDate;
				}//has TerminationTradeDate/TerminationDate on Screen
				else{
					EarlyTerminationDate = trade.getTerminationDate();
					EarlyTerminationTradeDate = EarlyTerminationDate;
					EarlyTerminationSettlementDt = EarlyTerminationDate;
				}
			}
		}

		if(AmendmentDetails)
		{
			String amendReason = trade.getKeywordValue( TradeKeywordKey.AMENDMENT_REASON);
			String amendAction = trade.getKeywordValue( TradeKeywordKey.AMENDMENT_ACTION);
			String amendComment = trade.getKeywordValue( TradeKeywordKey.AMENDMENT_COMMENT);
			String oasysAmendReason = getOasysAmendReasonFromMap(amendReason);//Added for oasys calypso map -ss78999
			String oasysAmendAction = getOasysAmendActionFromMap(amendAction);//Added for oasys calypso map -ss78999
			
			if( ! Util.isEmptyString(amendReason))
			{
				if(! Util.isEmptyString(oasysAmendReason))//Added for oasys calypso map -ss78999
					TradeAmendReason = oasysAmendReason; //Added for oasys calypso map -ss78999
				else
				    TradeAmendReason = amendReason;							
			}			
			if( !Util.isEmptyString(amendAction))
			{
				if(! Util.isEmptyString(oasysAmendAction))//Added for oasys calypso map -ss78999
					TradeAmendAction = oasysAmendAction;//Added for oasys calypso map -ss78999
				else
				    TradeAmendAction = amendAction;							
			}		
			if( ! Util.isEmptyString(amendComment))
			{
				TradeAmendComment = amendComment;							
			}		
		}
		else
		{
			if ( isPAEnabled )
			{
				String amendReason = trade.getKeywordValue( TradeKeywordKey.AMENDMENT_REASON );
				String oasysAmendReason = getOasysAmendReasonFromMap(amendReason);//Added for oasys calypso map -ss78999
				if( ! Util.isEmptyString(amendReason) 
					&& AmendReason.CANCEL_PARTIAL_ASSIGNMENT.equals(amendReason) )
					{
					if(! Util.isEmptyString(oasysAmendReason))//Added for oasys calypso map -ss78999
						TradeAmendReason = oasysAmendReason; //Added for oasys calypso map -ss78999
					else
					    TradeAmendReason = amendReason;							
					}			
			}
		}
		
		if(trade.getKeywordValue(TradeKeywordKey.FEE_CALENDARS) != null) {
			FeeCalendars = trade.getKeywordValue(TradeKeywordKey.FEE_CALENDARS);
		}
		if(trade.getKeywordValue(TradeKeywordKey.FEE_ROLL_CONVENTION) != null) {
			FeeRollConvention = trade.getKeywordValue(TradeKeywordKey.FEE_ROLL_CONVENTION);
		}

		try {
			if(trade.getKeywordValue(TradeKeywordKey.TERMINATIONLINK) != null) {
				TerminationLink = Integer.parseInt(
						trade.getKeywordValue(TradeKeywordKey.TERMINATIONLINK));
			}
			if(trade.getKeywordValue(TradeKeywordKey.ORIGINATION_LINK) != null) {
				OriginationLink = Integer.parseInt(
							trade.getKeywordValue(TradeKeywordKey.ORIGINATION_LINK));
			}
			if(trade.getKeywordValue(TradeKeywordKey.BUYBACK_PERCENTAGE) != null) {
				BuyBackPercentage = Double.parseDouble(
						trade.getKeywordValue(TradeKeywordKey.BUYBACK_PERCENTAGE));
			}
			if(trade.getKeywordValue(TradeKeywordKey.BUYBACK_AMOUNT) != null) {
				BuyBackAmount = Double.parseDouble(
							trade.getKeywordValue(TradeKeywordKey.BUYBACK_AMOUNT));
			}
		}
		catch (NumberFormatException numExec) {
               throw 
			     new DPSTransactionException("Invalid conversion attempt one of the following Transaction Keywords:"
			     		+  "TerminationLink: " + trade.getKeywordValue(TradeKeywordKey.TERMINATIONLINK)
						 + "\n OriginationLink: " + trade.getKeywordValue(TradeKeywordKey.ORIGINATION_LINK)
						  + "\n BuyBackPercentage: " + trade.getKeywordValue(TradeKeywordKey.BUYBACK_PERCENTAGE)
						  + "\n BuyBackAmount: " + trade.getKeywordValue(TradeKeywordKey.BUYBACK_AMOUNT)
						  , numExec);
		}

		if(trade.getKeywordValue(TradeKeywordKey.BUYBACK_CURRENCY) != null) {
			BuyBackCurrency = trade.getKeywordValue(TradeKeywordKey.BUYBACK_CURRENCY);
		}
		
		if(trade.getKeywordValue(TradeKeywordKey.TRADE_MEDIUM) != null) {
			TradeMedium = trade.getKeywordValue(TradeKeywordKey.TRADE_MEDIUM);
		}
		
		if(trade.getKeywordValue(TradeKeywordKey.AGGRESSOR_FLAG) != null) {
		   if(trade.getKeywordValue(TradeKeywordKey.AGGRESSOR_FLAG).equalsIgnoreCase("Aggressed")) {
				AggressorFlag = "PASSIVE";
		   }
		   if(trade.getKeywordValue(TradeKeywordKey.AGGRESSOR_FLAG).equalsIgnoreCase("Aggressor")) {
				AggressorFlag = "AGGRESSIVE";
		   }
		}
	}
	
	private void addFee(Trade trade, Object obj)
	{
		TradeFee tradeFee = new TradeFee();
		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				+ " - About to Set TradeFee Data.");
	
		tradeFee.set(trade,obj);
	
		Log.info(CLASSNAME,
				"TradeId = " + trade.getId()
				+ " - Successfully set TradeFee Data.");
	
		_fees.add(tradeFee);
		
	}

	/*
	 * START-CHANGE || 17-01-2007 || O. Jayachandra Gupta || To create XML tags
	 * for trade type at Transaction level
	 */

	/*
	 * @param trade to retrieve trade keywords from trade object and add tag
	 * names and tag values to vectors
	 */
	private void setTradeTypeValues(Trade trade) {
		Vector tagsFromDomainValues = null;
		try {
			tagsFromDomainValues = DSConnection.getDefault()
					.getRemoteReferenceData().getDomainValues(
							DomainValuesKey.TRADE_CLASS_XML);
		} catch (Exception e) {
			Log.error(CLASSNAME,
					"Error Fetching XML tags from domain values ->"
							+ e.getMessage());
		}
		if (tagsFromDomainValues == null) {
			return;
		}
		Vector tagsToConstruct = new Vector();
		// all the trade type values are seperated using a delimiter key
		// specified in DynamicPanelConstants.java and added to a vector
		for (int i = 0; i < tagsFromDomainValues.size(); i++) {
			if (tagsFromDomainValues.get(i).toString().indexOf(
					DynamicPanelConstants.DELIMETER_KEY) == -1) {
				Log.info(CLASSNAME, "Missed the tag :"
						+ tagsFromDomainValues.get(i));
				continue;
			}
			StringTokenizer st = new StringTokenizer(
					(String) tagsFromDomainValues.get(i),
					DynamicPanelConstants.DELIMETER_KEY);
			String tagName = null;
			String level = null;
			if (st.hasMoreTokens()) {
				tagName = st.nextToken();
			}
			if (st.hasMoreTokens()) {
				level = st.nextToken();
			}
			if (level != null && level.equals(this.LEVEL)) {
				tagsToConstruct.add(tagName);
			}
		}
		tradeTypeTags = new Vector();
		tradeTypeValues = new Vector();
		for (int i = 0; i < tagsToConstruct.size(); i++) {
			String value = trade.getKeywordValue((String) tagsToConstruct
					.get(i));
			if (value == null || value.trim().equals("")) {
				continue;
			}
			String str = (String) tagsToConstruct.get(i);
			if (str == null || str.trim().equals("")) {
				continue;
			}
			if (str.indexOf(DynamicPanelConstants.DOMAIN_NAME_SEPARATOR) != -1) {
				str = str
						.substring(str
								.lastIndexOf(DynamicPanelConstants.DOMAIN_NAME_SEPARATOR) + 1);
			}
			tradeTypeTags.add(str);
			tradeTypeValues.add(value);
		}
	}
	
//Added oasys calypso Amend Reason map - ss78999
	
	private String getOasysAmendReasonFromMap(String amendReason)
	{
		String oasysCalypsoAmdReason = null;
		oasysCalypsoAmdReason = LocalCache.getDomainValueComment(DSConnection.getDefault(), DomainValuesKey.OASYS_CALYPSO_AMEND_REASON_MAP, amendReason);
		return oasysCalypsoAmdReason;
	}
//Added oasys calypso Amend Action map - ss78999
	
	private String getOasysAmendActionFromMap(String amendAction)
	{
		String oasysCalypsoAmdAction = null;
		oasysCalypsoAmdAction = LocalCache.getDomainValueComment(DSConnection.getDefault(), DomainValuesKey.OASYS_CALYPSO_AMEND_ACTION_MAP, amendAction);
		return oasysCalypsoAmdAction;
	}

	/*
	 * @param hashMap to add all tradetype tags and values to hashMap
	 */
	private void addTradeTypeTagsToXML(HashMap hashMap) {

		if (tradeTypeTags == null || tradeTypeValues == null) {
			return;
		}
		for (int i = 0; i < tradeTypeTags.size(); i++) {
			if (tradeTypeTags.get(i) == null || tradeTypeValues.get(i) == null) {
				continue;
			}
			// Code changed for downstream to recognize tag and value
			if (tradeTypeTags.get(i).equals(TradeKeywordKey.TRADE_CLASS)) {
				if (tradeTypeValues.get(i).toString().equalsIgnoreCase(
						this.LOAN_CDS_DOMAIN_VALUE)) {
					hashMap.put(this.TRADE_TYPE_TAG, this.LOAN_CDS_TAG_VALUE);
				} else if (tradeTypeValues.get(i).toString().equalsIgnoreCase(
						this.MUNI_CDS_DOMAIN_VALUE)) {
					hashMap.put(this.TRADE_TYPE_TAG,this.MUNI_CDS_VALUE);
				} else {
					hashMap.put(this.TRADE_TYPE_TAG,tradeTypeValues.get(i));
				}
			} else if(tradeTypeTags.get(i).equals(this.DATE_OF_ORIGINAL_CREDIT_AGREEMENT)){
				hashMap.put(tradeTypeTags.get(i),DateUtil.getDateYYYYMMdd(Util.stringToDate(tradeTypeValues.get(i).toString())));
			} else {
				hashMap.put(tradeTypeTags.get(i), tradeTypeValues.get(i));
			}
		}
	}
	/* END-CHANGE || 17-01-2007 */
}
