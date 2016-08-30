/*
* Deal Object for BackOffice feed
* Author: ps64022
*
*/

package com.citigroup.project.dps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import calypsox.apps.citi.partialassignment.PartialAssignmentUtil;
import calypsox.apps.trading.DomainValuesUtil;
import calypsox.apps.trading.STM_Constants;
import calypsox.apps.trading.TradeKeywordKey;
import calypsox.apps.trading.salesallocation.util.AllocationDelegate;
import calypsox.tk.bo.workflow.rule.BackToBackTradeRule;
import calypsox.tk.core.BackToBackCustomData;
import calypsox.tk.core.CalypsoLogUtil;
import calypsox.tk.core.CitiCustomTradeData;
import calypsox.tk.core.DealCoverCustomData;
import calypsox.tk.refdata.DomainValuesKey;
import calypsox.tk.util.DealCoverUtil;

import com.calypso.tk.core.JDatetime;
import com.calypso.tk.core.LegalEntity;
import com.calypso.tk.core.Log;
import com.calypso.tk.core.Status;
import com.calypso.tk.core.Trade;
import com.calypso.tk.core.Util;
import com.calypso.tk.product.TerminationUtil;
import com.calypso.tk.service.DSConnection;
import com.calypso.tk.util.TradeArray;
import com.citigroup.project.abcds.util.CitiCustomFactory;
import com.citigroup.project.dps.exception.DPSDealException;
import com.citigroup.project.migration.service.CalypsoServices;
import com.citigroup.project.util.DateUtil;
import com.citigroup.project.util.XMLPrintUtil;

/**
*  Deal object for BackOffice feed <br>
*
*/

/*
 * Change History (most recent first):
 * 		03/03/2008	bs80435		:	Partial Assignment link is not being populated for the child trade; if the trade is a B2B.
		02/07/2008	bs80435		:	Added Entries required for Partial Assignment Project; generateXML() is changed. 
*/
public class Deal implements IDPSDataObject {

	protected ArrayList _transactions = new ArrayList();
	protected ArrayList _backToBacks = new ArrayList();
	protected TradeArray _partialAssignList;

	protected int		Deal;
	protected int		Transaction;
	protected String	OpsFile;
	protected String	Type;
	protected String	FirmRole;
	protected String	CustomerMnemonic;
	protected String	PAOrigNotional;
	protected JDatetime	LastModified;
	protected String	LastModifiedTimeZone;
	protected boolean isAllocatedParent = false;
	protected TradeArray ta = null;
	protected boolean isPartialAssignChild;
	protected int parentDealId;
	protected int parentTransactionId;
	protected boolean isPAEnabled = true;
	
	private static final String CLASSNAME = DPSConstants.DEAL;
	public static final String DUMMY_PAIR_ID = "1";	// dummy id for oasys since oasys need it to be populated 


	static 
	{
	  CalypsoLogUtil.adjustLogCategory(CLASSNAME);
	}

	/**
	* Init function for Deal object
	* @param args    The <code>args</code> currently NULL
	*/
	public void init(List args) {
	}

	/**
	* Set function for Deal object
	* @param trade    The <code>Trade</code> to set trade data
	* @param Object   The <code>Object</code> TradeArray when the first Arg is NULL
	*/
	public void set(Trade trade, Object obj) {
		int noOfAlloc = 0;
		DealCoverCustomData dcCustomData = null;
		Trade calTrade =  null;
		TradeArray array = null;


		
		if(trade != null) {
			if((CitiCustomTradeData)trade.getCustomData() != null) {
				dcCustomData = ((CitiCustomTradeData)trade.getCustomData()).
													getDealCoverCustomData();
			}
			
			Log.info(CLASSNAME, 
					"TradeId : " + trade.getId()
					+ " - Setting Deal data");

			if(dcCustomData != null) {
				Deal = dcCustomData.getDealID();
				Transaction = dcCustomData.getTransactionID();
				OpsFile = dcCustomData.getOperationsFile();
			}
			else {
				Log.warn(CLASSNAME,
				"Deal.java: DealCoverCustomData is null for trade " + trade.getId());
				return;
			}
			
			try {
				array = DealCoverUtil.getTradesByDealCover(dcCustomData.getDealID());
			}
			catch (RemoteException re) { 
				DPSDealException dpsEx = 
					 new DPSDealException("Could not get trades by dealID: " + dcCustomData.getDealID(), 
					 		re);
				throw dpsEx;
				
			}
	
		}
		else {
			
			if(obj instanceof TradeArray)
				array = (TradeArray) obj;
			else
				return;
			
			if(array == null || array.size() == 0) {
				return;
			}
			
			calTrade =  array.get(0);
			
			if((CitiCustomTradeData)calTrade.getCustomData() != null) {
				dcCustomData = ((CitiCustomTradeData)calTrade.getCustomData()).
													getDealCoverCustomData();
			}

			if(dcCustomData != null) {
				Deal = dcCustomData.getDealID();
				Transaction = dcCustomData.getTransactionID();
				OpsFile = dcCustomData.getOperationsFile();
			}
		}
			
		isPAEnabled = DomainValuesUtil.checkDomainValue(DomainValuesKey.ENABLE_PARTIALASSIGNMENT_XML, STM_Constants.YES, true);
		
		Type = MappingConstants.TRADE_TYPE;
		FirmRole = MappingConstants.FIRMROLE;
		LastModifiedTimeZone = Util.getReferenceTimeZone().getID();
		
		Trade paTrade = null;
		for (int i = 0; i < array.size();i++) {

			calTrade =  array.get(i);
			if(calTrade.getBundle()!=null && calTrade.getBundle().getType().equals(MappingConstants.BUNDLE_BACKTOBACK))
			{
				if( !Util.isEmptyString(calTrade.getKeywordValue(TradeKeywordKey.TRADE_TYPE)) )
				{
					paTrade = calTrade;
				}
			} else paTrade = calTrade;
			
			Log.info(CLASSNAME, 
					"Processing tradeId: "
					+ calTrade.getId());
			
			if ( isPAEnabled )
			{
				if ( calTrade.getCounterParty() != null )
				{
					CustomerMnemonic = CalypsoServices.getLegalEntityCodeForExternalSys(calTrade.getCounterParty().getCode());
				}
				PAOrigNotional = PartialAssignmentUtil.getPAOrigNotional(calTrade);
			}
			
			if(LastModified == null)
				LastModified = calTrade.getUpdatedTime();
			else if(LastModified.before(calTrade.getUpdatedTime()))
				LastModified = calTrade.getUpdatedTime();
			
		    // Since Partially Terminated trade in Calypso has the same dealcover id
			// as the newly created trade, skip the Terminated trade
			if(calTrade.getKeywordValue(TerminationUtil.TERMINATION_TYPE) != null &&
				calTrade.getKeywordValue(TerminationUtil.TERMINATION_TYPE).
									equals(TerminationUtil.PARTIAL_TERMINATION)) {
				continue;
			}
			
			if (calTrade.getBundle() != null && 
				calTrade.getRole().equals(LegalEntity.COUNTERPARTY) &&
				calTrade.getBundle().getType().indexOf(BackToBackTradeRule.BACK_TO_BACK) > -1) {

				BackToBackCustomData btbCustomData = null;
				BackToBackCustomData btbCustomData1 = null;

				btbCustomData = ((CitiCustomTradeData)calTrade.getCustomData()).
												getBackToBackCustomData();
					
				if(btbCustomData != null) {
					BackToBacks btbFirstRow = new BackToBacks();
					Log.info(CLASSNAME, 
						   "Setting backtoback data on trade: "
							+ calTrade.getId());
					btbFirstRow.set(calTrade,btbCustomData);
					_backToBacks.add(btbFirstRow);
				}

				btbCustomData1 = ((CitiCustomTradeData)calTrade.getCustomData()).
												getBackToBackCustomData1();
					
				if(btbCustomData1 != null) {
					BackToBacks btbSecondRow = new BackToBacks();
					Log.info(CLASSNAME, 
							   "Setting 2nd backtoback data on trade: "
								+ calTrade.getId());
					btbSecondRow.set(calTrade,btbCustomData1);
					_backToBacks.add(btbSecondRow);
				}
			}
			try{
			if(calTrade.getKeywordValue(TradeKeywordKey.NO_OF_ALLOCATIONS) != null){
				noOfAlloc = Integer.parseInt(calTrade.getKeywordValue(TradeKeywordKey.NO_OF_ALLOCATIONS));
			}
				if(noOfAlloc > 0){
					AllocationDelegate allocDel = new AllocationDelegate(DSConnection.getDefault());
					ta = allocDel.getAllocations(calTrade.getId());
					if(ta != null && ta.size() > 0){
						isAllocatedParent = true;	
					}
				}
			}
			catch(NumberFormatException ne)
			{
				throw new DPSDealException("Error in Deal for tradeId: " + calTrade.getId()
						                   + ", in parsing '" 
										   + calTrade.getKeywordValue(TradeKeywordKey.NO_OF_ALLOCATIONS) + "' to an Integer", ne);
			}
			catch(Exception ex)
			{
				throw new DPSDealException("Error in Deal - occured when trying to get Allocations for Calypso tradeId: "
						 + calTrade.getId(), ex);
				
			}
		
			//Transaction transaction = new Transaction();
			IDPSDataObject transaction = CitiCustomFactory.getTransaction(calTrade);
			Log.info(CLASSNAME, 
					"TradeId : " + calTrade.getId()
					+ " - About to set Transaction data.");
			transaction.set(calTrade,obj);
			List txnArgList = new ArrayList();
			txnArgList.add(new Boolean(isPAEnabled));
			transaction.init(txnArgList);
			Log.info(CLASSNAME, "TradeId : " + calTrade.getId()+ " - Successfully set Transaction data.");
			_transactions.add(transaction);
			
		}
		
		/*Moved the code outside of the for loop and set the trade object to the customer trade.*/
		// 
		// Partial Assignment Link list
		//
		if ( isPAEnabled )
		{
			try
			{	
				isPartialAssignChild = ( PartialAssignmentUtil.getPartialAssignmentParentId(paTrade) != 0 ) ? true : false;
				if ( isPartialAssignChild )
				{
					try
					{
						Trade parentTrade = PartialAssignmentUtil.getPartialAssignmentParentTrade(paTrade);
						if ( parentTrade != null )
						{	 
							DealCoverCustomData dcParentCustomData =
								((CitiCustomTradeData)parentTrade.getCustomData()).getDealCoverCustomData();
							if ( dcParentCustomData != null )
							{
								parentDealId = dcParentCustomData.getDealID();
								parentTransactionId = dcParentCustomData.getTransactionID();
							}
							
						}
					}
					catch ( Exception e )
					{
						Log.error(CLASSNAME, "Trade " + trade.getId() + " unable to retrieve its partial assignment parent information");
						Log.error(CLASSNAME, e);
					}
				}
				
				if ( PartialAssignmentUtil.isPartialAssignmentTrade(paTrade) )
				{
					_partialAssignList = PartialAssignmentUtil.getChildTrades(paTrade, TradeKeywordKey.PAFROM);
				}
			}
			catch ( Exception ex )
			{
				DPSDealException dpsEx = 
					new DPSDealException("Could not retrieve PA children trades: " + paTrade.getId(), ex);
				throw dpsEx;
				
			}
		}
	}
	
	

	/**
	* GenerateXML function for Deal object
	* @return     <code>String</code> containing XML
	*/
	public String generateXML() {
		String strLastModified = null;
		StringBuffer xmlStringBuf = 
						new StringBuffer(XMLPrintUtil.XML_STRING_SIZE);

		HashMap dealHashMap = new HashMap();
		
		strLastModified = DateUtil.modifyTimeStamp(DateUtil.getDateTimeAsStr(LastModified));
		
		dealHashMap.put("FirmRole",FirmRole);
		dealHashMap.put("LastModified",strLastModified);
		//dealHashMap.put("LastModified",DateUtil.getDateTimeAsStr(LastModified));
		//dealHashMap.put("LastModified",DateUtil.getDateTimeAsStr(LastModified));
		//dealHashMap.put("LastModifiedTimeZone",MappingConstants.DEFAULT_TIMEZONE);
		dealHashMap.put("LastModifiedTimeZone",LastModifiedTimeZone);
		dealHashMap.put("Type",Type);
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTag(DPSConstants.DEAL, key(),DPSConstants.ATTRIBUTE));

		xmlStringBuf.append(XMLPrintUtil.createXMLTag("DealId",
										String.valueOf(Deal),"Key"));

		xmlStringBuf.append(XMLPrintUtil.createXMLTagsFromMap(dealHashMap));

		if(_backToBacks != null && _backToBacks.size() != 0) {

			xmlStringBuf.append(XMLPrintUtil.createXMLTag("BackToBacks",
									String.valueOf(Deal),"List"));

			for (int j = 0; j < _backToBacks.size(); j++) {
				BackToBacks btbTradeInfo = (BackToBacks) _backToBacks.get(j);
				xmlStringBuf.append(btbTradeInfo.generateXML());
			}

			xmlStringBuf.append(XMLPrintUtil.createClosingTag("BackToBacksList"));
		}

		if ( isPAEnabled )
		{
			createPartialAssignXML(xmlStringBuf);
		}
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTag("Transaction",
									String.valueOf(Deal),"List"));

		for (int i = 0; i < _transactions.size(); i++) {
			Transaction transaction = (Transaction) _transactions.get(i);
			xmlStringBuf.append(transaction.generateXML());
		}

		xmlStringBuf.append(XMLPrintUtil.createClosingTag("TransactionList"));

		// set and generate all the child allocations data.
		if(isAllocatedParent){  
			createAllocationXML(xmlStringBuf);
		}
		xmlStringBuf.append(XMLPrintUtil.createClosingTag(DPSConstants.DEAL));

		return xmlStringBuf.toString();
	}
	
	public void createPartialAssignLinkXML(StringBuffer xmlStringBuf
										, int parentDealId
										, int deal
										, int parentTransactionId
										, int transaction
										, String customMnemonic
										, String opsfile
										, String notional)
	{
		String parentKey = 	parentDealId + XMLPrintUtil.XML_KEY_SEPERATOR + deal + XMLPrintUtil.XML_KEY_SEPERATOR + DUMMY_PAIR_ID;
		HashMap trnHashMap = new HashMap();
		
		trnHashMap.put(DPSConstants.DEAL, String.valueOf(parentDealId));
		trnHashMap.put(DPSConstants.ALLOCATION_DEAL, String.valueOf(deal));
		trnHashMap.put(DPSConstants.PAIR_LINK_I_D, DUMMY_PAIR_ID);
		trnHashMap.put(DPSConstants.TRANSACTION_ID, String.valueOf(parentTransactionId));
		trnHashMap.put(DPSConstants.CHILD_TRANSACTION_ID, String.valueOf(transaction));
		trnHashMap.put(DPSConstants.CUSTOMER_MNEMONIC, customMnemonic);
		trnHashMap.put(DPSConstants.ALLOCATION_OPS_FILE, opsfile);
		trnHashMap.put(DPSConstants.NOTIONAL, notional);
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTag(DPSConstants.PART_ASSIGN_LINK,
				parentKey,DPSConstants.ATTRIBUTE));
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTagsFromMap(trnHashMap));
		
		xmlStringBuf.append(XMLPrintUtil.createClosingTag(DPSConstants.PART_ASSIGN_LINK));
	}
	
	/**
	 * 
	 * Purpose: create xml for partial assignment link list 
	 * @param xmlStringBuf
	 */
	public void createPartialAssignXML(StringBuffer xmlStringBuf)
	{
		if ( ( _partialAssignList != null && ! _partialAssignList.isEmpty() )
			|| 
			isPartialAssignChild 
			)
		{
			xmlStringBuf.append(XMLPrintUtil.createXMLTag(DPSConstants.PART_ASSIGN_LINK,
					String.valueOf(Deal),"List"));

			if ( isPartialAssignChild )
			{
				createPartialAssignLinkXML(xmlStringBuf, parentDealId, Deal, parentTransactionId, Transaction, CustomerMnemonic, OpsFile, PAOrigNotional);
			}
			if ( _partialAssignList != null && ! _partialAssignList.isEmpty() )
			{
				for ( int i = 0; i < _partialAssignList.size(); i++ )
				{
					Trade trade = (Trade) _partialAssignList.get(i);
					DealCoverCustomData dcChildCustomData = ((CitiCustomTradeData)trade.getCustomData()).getDealCoverCustomData();
					int childDealId = dcChildCustomData.getDealID();
					int childTransactionId = dcChildCustomData.getTransactionID();
					String childOpsFile = dcChildCustomData.getOperationsFile();
					String childCustomerMnemonic = CalypsoServices.getLegalEntityCodeForExternalSys(trade.getCounterParty().getCode());
					String childPAOrigNotional = PartialAssignmentUtil.getPAOrigNotional(trade);
					
					createPartialAssignLinkXML(xmlStringBuf, Deal, childDealId, Transaction, childTransactionId, childCustomerMnemonic, childOpsFile, childPAOrigNotional);
				}
			}
			
			xmlStringBuf.append(XMLPrintUtil.createClosingTag("PartAssignLinkList"));
		}
	}
	
	/**
	 * Creates & appends the allocationList and allocationLinkList xmls
	 * @param xmlStringBuf
	 */
	private void createAllocationXML(StringBuffer xmlStringBuf){
		AllocationList allocList = null;
		AllocationLink allocLink = null;
		Trade childTrade = null;
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTag("Allocation", allocationKey(),"List"));
		for (int i = 0; i < ta.size(); i++) {
			childTrade = (Trade)ta.get(i);
			if( childTrade.getStatus().getStatus().equals(Status.VERIFIED) || 
					childTrade.getStatus().getStatus().equals(Status.TERMINATED) ){
				allocList = new AllocationList();
				childTrade.addKeyword(TradeKeywordKey.PARENT_DEAL_ID,String.valueOf(Deal));
				allocList.set(childTrade,null);
				xmlStringBuf.append(allocList.generateXML());
			}
		}
		xmlStringBuf.append(XMLPrintUtil.createClosingTag("AllocationList"));
		
		xmlStringBuf.append(XMLPrintUtil.createXMLTag("AllocationLink", allocationKey(),"List"));
		for (int i = 0; i < ta.size(); i++) {
			childTrade = (Trade)ta.get(i);
			if( childTrade.getStatus().getStatus().equals(Status.VERIFIED) || 
					childTrade.getStatus().getStatus().equals(Status.TERMINATED)){
				allocLink = new AllocationLink();
				childTrade.addKeyword("ParentDealId",String.valueOf(Deal));
				allocLink.set(childTrade,null);
				xmlStringBuf.append(allocLink.generateXML());
			}
		}
		xmlStringBuf.append(XMLPrintUtil.createClosingTag("AllocationLinkList"));
	}
	
	public String allocationKey() {
		return String.valueOf(Deal);
	}
	
	/**
	*  Deal object key
    *  @return     <code>Key</code> for Deal object
    */
	public String key() {
        return(String.valueOf(Deal));
    }
}
