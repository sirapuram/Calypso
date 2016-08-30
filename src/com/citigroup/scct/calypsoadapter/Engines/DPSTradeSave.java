/*
 *
 * Author: Pranay Shah
 *
 */

package com.citigroup.project.dps.dpsjni;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.calypso.tk.core.Util;
import com.citigroup.project.dps.exception.DPSSenderException;

// Please make sure LD_LIBRARY_PATH is in the environment
// to load DPS API library.

/* 
 * 06/02/2009 		ds53697		Changes done to handle exceptions
 * 								thrown from native code (JIRA:35544-4717)
 */

public class DPSTradeSave
{
	private static DpsAPINative dpsAPINative;

	static {
		dpsAPINative = new DpsAPINative();

		try {
        	dpsAPINative.initialize();
		}
		catch (Exception e) {
			System.out.println("In java Initialize: from C++ code");
		}
	}

	public DPSTradeSave() {
	}

	public static synchronized void saveTrade(String xmlString) {
		try {
        	dpsAPINative.save(xmlString);
		}
		catch (Exception e) {
			System.out.println("In java save: from C++ code " + e.getMessage());
			throw new DPSSenderException(e);
		}
	}

	public static void main(String[] args) {

		String xmlFileName = null;
		String xmlString = null;

		if(args.length > 1) {
			System.out.println("Command line argument");
			xmlFileName = args[1];
		}
		else {
			xmlFileName = "/opt/emapps/GCDT/Calypso/workspace/ps64022/inhouse/build/2314.xml";
			System.out.println("Looking for input file 2314.xml in " + xmlFileName);
		}

		try {
			String filePath = Util.findFileInClassPath(xmlFileName);
			String nextLine = "";
			BufferedReader br = new BufferedReader(new FileReader(xmlFileName));
   			StringBuffer sb = new StringBuffer();

   			while ((nextLine = br.readLine()) != null) {
     			sb.append(nextLine);
			}

			xmlString = sb.toString();
		}
		catch (IOException ie) {
			System.out.println("IOException caught");
			ie.printStackTrace();
		}
 
        System.out.println("DpsApiNative Test Class running");

		try {
        	DPSTradeSave.saveTrade(xmlString);
		}
		catch (Exception e) {
			System.out.println("In java main, couldn't save: from C++ code");
		}
    }
}
