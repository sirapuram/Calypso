/*
 *
 * Author: Pranay Shah
 *
 */
 

package com.citigroup.project.util;

import java.util.*;

public class XMLPrintUtil {

	/**
	*	XML Constant Tags
	*/

	public static final String XML_INITIAL_TAG = "<";
	public static final String XML_FINAL_TAG = ">";
	public static final String XML_QUOTE_TAG = "\"";
	public static final String XML_ASSIGN_TAG = "=";
	public static final String XML_ELEMENT_CLOSE = "/";
	public static final String XML_DATA_SEPERATOR = ",";
	public static final String XML_KEY_SEPERATOR = "\t";
	public static final String XML_NOTE_TYPE = "XML_NOTE";

	public static final int XML_STRING_SIZE = 20000;
	public static final int LARGE_XML_STRING_SIZE = 200000;

	
	private static Map entityReferenceEncoding = new HashMap();
	static
	{
		entityReferenceEncoding.put("&", "&amp;");
		entityReferenceEncoding.put("<", "&lt;");
		entityReferenceEncoding.put(">", "&gt;");
		entityReferenceEncoding.put("\"", "&quot;");
	}
	
	/**
	* Creates XML tag for a given Node name & Value. This function
	* only create plain tags, it doesn't create tags with Key, Attribute 
	* or List.
	* Example: createXMLTag("TradeStatus","Active",false,false,false) 
	* Output: <TradeStatus>Active</TradeStatus> 
	*/

	public static String createXMLTag (String name, String value) {

		StringBuffer strXML =  new StringBuffer("");

		if(value != null) {
			value = encodeEntityReferences(value);
			strXML.append(XML_INITIAL_TAG + name + XML_FINAL_TAG + value +
				XML_INITIAL_TAG + XML_ELEMENT_CLOSE + name + XML_FINAL_TAG + "\n");

			// Log.dbg("\nSimpleXMLTag: " + strXML.toString());
		}
		return strXML.toString();
	}

	/**
	* Creates XML tag for just the Node name. 
	* Example: createXMLTag("Edit") 
	* Output: <Edit> 
	*/

	public static String createNameTag(String name) {

		StringBuffer strXML =  new StringBuffer("");
		strXML.append(XML_INITIAL_TAG + name + XML_FINAL_TAG + "\n");
				
		return strXML.toString();
	}

	/**
	* Creates XML tag for a given Node name & Value. This function  
	* creates all possible tag types such as Key, Attribute,List or plain tag.
	*
	* Example: createXMLTag("TradeStatus","Active",false,false,false) 
	* Output: <TradeStatus>Active</TradeStatus> 
	*/

	public static String createXMLTag (String name, String value, 
						String tagType) {

		StringBuffer strXML =  new StringBuffer("");

		if(value != null) {
			value = encodeEntityReferences(value);

			if(tagType.equals("List")) {
				strXML.append(XML_INITIAL_TAG + name + "List id" + XML_ASSIGN_TAG + 
					XML_QUOTE_TAG + value + XML_QUOTE_TAG + XML_FINAL_TAG + "\n");
			}
			else if(tagType.equals("Attribute")) {
				strXML.append(XML_INITIAL_TAG + name + " id" + XML_ASSIGN_TAG + 
					XML_QUOTE_TAG + value + XML_QUOTE_TAG + XML_FINAL_TAG + "\n");
			}
			else if(tagType.equals("Key")) {
				strXML.append(XML_INITIAL_TAG + name + " Key" + XML_ASSIGN_TAG + 
					XML_QUOTE_TAG + "Y" + XML_QUOTE_TAG + XML_FINAL_TAG +
					value + XML_INITIAL_TAG + XML_ELEMENT_CLOSE + name + 
													XML_FINAL_TAG + "\n");
			}
			else {
				strXML.append(XML_INITIAL_TAG + name + XML_FINAL_TAG + value +
					XML_INITIAL_TAG + XML_ELEMENT_CLOSE + name + XML_FINAL_TAG + "\n");
			}
		}

		return strXML.toString();
	}

	/**
	* Creates XML tags for all name, value pairs in a HashMap, this method
	* should be used only to create leaf Nodes, to create nodes with 
	* Attribute, Key, or List, use createXMLTag or createXMLTagsFromList
	*
	* Example: HashMap("TradeStatus","Active") 
	* Output: <TradeStatus>Active</TradeStatus> 
	*/

	public static String createXMLTagsFromMap (HashMap nameValueMap ) {

		StringBuffer strXML =  new StringBuffer("");

		for (Iterator iter = nameValueMap.entrySet().iterator(); iter.hasNext();) {

			Map.Entry entry = (Map.Entry) iter.next();
			String name = (String) entry.getKey();
			String value = (String) entry.getValue();

			if(value != null) {	
				value = encodeEntityReferences(value);
				strXML.append(XML_INITIAL_TAG + name + XML_FINAL_TAG + value +
				XML_INITIAL_TAG + XML_ELEMENT_CLOSE + name + XML_FINAL_TAG + "\n");
			}
		}
		return strXML.toString();
	}

	/**
	* Creates XML tags for all name, value(Value is a ArrayList) pairs in a HashMap.
	* First element in the ArrayList object should be string value such as 
	* Key, Attribute,List or Node. Second element should be the actual value.
	*
	* Example1: HashMap("TradeStatus", ArrayList(Node,Active)) 
	* Output1: <TradeStatus>Active</TradeStatus> 
	*
	* Example2: HashMap("DealId", ArrayList(Key,123456))
	* Output2: <DealId Key="Y">123456</DealId> 
	*
	* Example3: HashMap("Transaction", ArrayList(List,123456))
	* Output3: <TransactionList id="123456">
	*
	* Example4: HashMap("Deal", ArrayList(Attribute,123456))
	* Output4: <Deal id="123456">
	*
	* This is to create different XML tags for different requirements.
	*/

	public static String createXMLTagsFromList (HashMap nameValueMapList ) {

		StringBuffer strXML =  new StringBuffer("");
		String value = null;

		for (Iterator iter = nameValueMapList.entrySet().iterator(); 
													iter.hasNext();) {

			Map.Entry entry = (Map.Entry) iter.next();
			String name = (String) entry.getKey();
			List l = (List) entry.getValue();

			for(Iterator iter1 = l.iterator(); iter1.hasNext();) {
				
				String xmlTagType = (String) iter1.next();
				value = (String) iter1.next();
				if(value != null) {
					value = encodeEntityReferences(value);
					if(xmlTagType.equals("Key")) {
						strXML.append(XML_INITIAL_TAG + name + " Key" + XML_ASSIGN_TAG + 
							XML_QUOTE_TAG + "Y" + XML_QUOTE_TAG + XML_FINAL_TAG +
							value + XML_INITIAL_TAG + XML_ELEMENT_CLOSE 
												+ name + XML_FINAL_TAG + "\n");
					}
					else if(xmlTagType.equals("Attribute")) {
						strXML.append(XML_INITIAL_TAG + name + " id" + XML_ASSIGN_TAG + 
						XML_QUOTE_TAG + value + XML_QUOTE_TAG + XML_FINAL_TAG + "\n");
					}
					else if(xmlTagType.equals("List")) {
						strXML.append(XML_INITIAL_TAG + name + "List id" 
							+ XML_ASSIGN_TAG + XML_QUOTE_TAG
							+ value + XML_QUOTE_TAG + XML_FINAL_TAG + "\n");
					}
					else if(xmlTagType.equals("Node")) {
						strXML.append(XML_INITIAL_TAG + name + XML_FINAL_TAG + value +
						XML_INITIAL_TAG + XML_ELEMENT_CLOSE + name + XML_FINAL_TAG + "\n");
					}
				}
			}
		}
		return strXML.toString();
	}

	/**
	* This function creates closing tag, example </Deal>, </TransactionList>
	*/
	public static String createClosingTag(String name) {

		return(new String(XML_INITIAL_TAG + XML_ELEMENT_CLOSE + 
											name + XML_FINAL_TAG + "\n"));
	}

	/*
	* Sample main function to print XML tags
	*/

	public static void main(String[] args) {
	
		HashMap nameValueHash = new HashMap();
		HashMap nameValueHashList = new HashMap();
		ArrayList list = new ArrayList();
		ArrayList list1 = new ArrayList();

		list.add("Key");
		list.add("200000");
		nameValueHashList.put("Transaction", list);

		list1.add("Node");
		list1.add("Active");
		nameValueHashList.put("TradeStatus", list1);

		nameValueHash.put("FirmRole","PRINCIPAL");
		nameValueHash.put("PartyMnemonic","GNB06");
		nameValueHash.put("CounterPartyMnemonic","STC10681");

		XMLPrintUtil.createXMLTag("PaymentFrequency","QUARTERLY");

		XMLPrintUtil.createXMLTag("Deal","123456","Key");
		XMLPrintUtil.createXMLTag("Deal","123456","Attribute");
		XMLPrintUtil.createXMLTag("Transaction","123456","List");
		XMLPrintUtil.createXMLTag("ProductClass","CreditDefaultSwap","");

		XMLPrintUtil.createXMLTagsFromMap(nameValueHash);

		XMLPrintUtil.createXMLTagsFromList(nameValueHashList);
	}

	private static String encodeEntityReferences(String xmlString)
	{
		StringBuffer sb = new StringBuffer();
		
		StringTokenizer st = new StringTokenizer(xmlString, "&<>\"", true);
		String token;
		while(st.hasMoreTokens())
		{
			token = st.nextToken();
			Object encodedSeparator;
			if((encodedSeparator = entityReferenceEncoding.get(token)) != null)
				sb.append(encodedSeparator);
			else
				sb.append(token);
		}
		
		return sb.toString();
	}
}
