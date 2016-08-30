package com.citi.credit.gateway.recon;

import java.text.SimpleDateFormat;
import java.util.Comparator;

import com.citi.credit.gateway.data.CGReconTrade;
import com.citi.credit.gateway.util.CGUtilities;

public class CGTradeComparator implements Comparator{
	private String DATE_FORMAT = "yyyy-MM-dd k:m:s.S";
	private SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
	private int iMatch = 0;
    
	public int compare(Object o1, Object o2) {
		if (o1!=null && o2!=null) {
			CGReconTrade r1 = (CGReconTrade) o1;
			CGReconTrade r2 = (CGReconTrade) o2;
			// Test id match
			iMatch= matchIntegers(new Integer(r1.getTradeId()), new Integer(r2.getTradeId()));                                             
			if (iMatch != 0){
				return -1;
			}
			// Test Books match
			iMatch= matchStrings(r1.getBook(), r2.getBook());                                             
			if (iMatch < 0){
				return iMatch;
			}
			// Test direction match
			iMatch= matchStrings(r1.getBuySell(), r2.getBuySell());                                             
			if (iMatch < 0){
				return iMatch;
			}			
			// Test ccy match
			iMatch= matchStrings(trimLEBO(r1.getCpty()), trimLEBO(r2.getCpty()));                                             
			if (iMatch < 0){
				return iMatch;
			}			
			// Test status match
			iMatch= matchStrings(r1.getTradeStatus(), r2.getTradeStatus());                                             
			if (iMatch < 0){
				return iMatch;
			}			
	
			// Test notional match 
			double notional1 = 0d;
			double notional2 = 0d;
/*			if (r1.getBuySell().equals("Buy") && r1.getNotional()> 0) {
				notional1 = -1 * r1.getNotional();
			}
			else{
				notional1 = r1.getNotional();
			}
			
			if (r2.getBuySell().equals("Buy") && r2.getNotional()> 0) {
				notional2 = -1 * r2.getNotional();
			}
			else{
				notional2 = r2.getNotional();
			}
*/			iMatch= matchDoubles(notional1, notional2);
			if (iMatch != 0){
				return -1;
			}		
		    // Test version
			iMatch=matchIntegers(new Integer(r1.getVersion()), new Integer(r2.getVersion()));
			if (iMatch != 0){
				return -1;
			}		
			// Test Update date match
			String sCalUpDt = r1.getUpdateTime();
			String sCalUpDtNoSS= sCalUpDt.substring(0, sCalUpDt.lastIndexOf('.'));
			String sGfeUpDt = r2.getUpdateTime();
			String sGfeUpDtNoSS= sGfeUpDt.substring(0, sGfeUpDt.lastIndexOf('.'));			
			iMatch= matchStrings(sCalUpDtNoSS, sGfeUpDtNoSS);                                             
			if (iMatch < 0){
				return iMatch;
			}			
 		}
		
		return iMatch;
	}
	
	private int matchStrings(Object o1, Object o2) {
		if(o1 instanceof String && o2 instanceof String){ 
			String s1 = (String)o1;                                              
			String s2 = (String)o2;
			return s1.compareTo(s2);
		} 
		return -1;
	}
	private int matchIntegers(Object o1, Object o2) {
		if(o1 instanceof Integer && o2 instanceof Integer){ 
			Integer T1 = (Integer)o1;                                              
			Integer T2 = (Integer)o2;
			return T1.compareTo(T2);
		} 
		return -1;
	}	
	
	private int matchDoubles(double o1, double o2) {
			Double D1 = new Double(o1);                                              
			Double D2 = new Double(o2);
			return D1.compareTo(D2);
	}	
	
	private String trimLEBO(String cpty) {
		String val = (!CGUtilities.isStringEmpty(cpty) ? cpty : "");
		int idx = cpty.lastIndexOf("_BO");
		if (idx>0) {
			val = cpty.substring(0, idx);
		}
		return val;
	}
}
