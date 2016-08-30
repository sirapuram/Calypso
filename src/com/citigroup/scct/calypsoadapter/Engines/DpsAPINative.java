/*
 *
 * Author: Pranay Shah
 *
 */
// Native DpsAPI interface, see implementation in file DpsAPINative.c
// Please make sure LD_LIBRARY_PATH set to point to libDpsAPINative.so library.

/*
 * 2008 Feb 12 | hn94560 | SM #CAL-8080 | Added entry for dealKeysPath directory argument for Code CleanUp Project
 */

package com.citigroup.project.dps.dpsjni;

import java.lang.*;
import java.io.*;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.Util;

public class DpsAPINative
{

	private static final String CLASSNAME = "DpsAPINative";
	
    static {
    	Log.info(CLASSNAME,"Loading libDpsAPINative.so library...");
    	System.loadLibrary("DpsAPINative");
    	Log.info(CLASSNAME,"\n\nLoading libDpsAPINative.so done...");
    }

    public native void initialize();
    public native synchronized void save(String xmlString);

    public native void executeDPSQueryCB(String queryName,String[] queryParams,
									String callBackFuncName, String outputDir);

    public native int[] executeQueryDeals(String queryName,String[] queryParams,
									String callBackFuncName);

    public native String executeDPSQuery(String queryName,String[] queryParams, String outputFile);
	public native void executeQueryTrnMap(String queryName, String[] queryParams, String callBackFuncName, String outputDir);
	public native void retrieveDeals(int[] dealKeys, String outputDir);
	public native String retrieveDeal(String dealKey);
	public native void doPaymentFeed(String scheduledPaymentsFile);
	// Modified for Code CleanUp Project by Hari krishnan.N on 2/12/2008
	public native void setDealKeysFilePath(String strDealKeyFilePath);
	
	public static void main(String[] args) {
		DpsAPINative dpsAPINative = new DpsAPINative();

		try {
			dpsAPINative.initialize();
		}
		catch (Exception e) {
			
			Log.fatal(CLASSNAME,"In java Initialize: from C++ code" + e.getMessage());
		}

		Log.debug(CLASSNAME,"In DpsAPINative constructor");
	}
}
