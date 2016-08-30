package com.citigroup.project.util;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.*;

import com.calypso.tk.core.sql.ioSQL;
import com.citigroup.project.eod.apps.EodFeed;
import com.citigroup.project.migration.staticdata.Constants;

/**
 * @author rv63599
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
/**
 * Provides simple String Utilities
 *
 * @version $Revision: 1.20 $
 *
 * @author $Author: vz55591 $
 *
 * $Log: StrUtil.java,v $
 * Revision 1.20  2008/06/12 17:05:31  vz55591
 * Merger of PRE_REL_27_01
 *
 * Revision 1.19.16.1  2008/05/15 13:51:45  ri99555
 * Merge of dev branch BR_MD_2007056756 to release branch BR_REL_27_01
 *
 * Revision 1.19  2007/11/22 01:40:30  vz55591
 * Merger of PRE_REL_24_01
 *
 * Revision 1.18.88.1  2007/11/07 16:01:27  jm15575
 * added two blank lines to the end of the files to eliminated Hermes EOF warnings
 *
 * Revision 1.18  2006/05/08 11:16:57  ri99555
 * Risk-based solar changes for LN UAT before risk-based PAA is activated
 *
 * Revision 1.17.40.1  2006/04/28 09:44:41  mn22621
 * Reviewing implementation of reset date for PAA Solar feed.
 *
 * Revision 1.17  2005/08/23 13:30:42  am75275
 * merged from Branch to Trunk REL_141_NACD_BRANCH_600_20_20040722
 *
 * Revision 1.12.2.4  2005/07/29 10:33:15  ok84741
 * CAL2537 Enhancements to BulkTradeLoader
 *
 * Revision 1.16  2005/07/27 14:24:50  ok84741
 * CAL2537 new function
 *
 * Revision 1.15  2005/06/02 16:49:20  ok84741
 * Alter the contract for bulk updates to ensure the user is advised to filter the trades before submitting back to the base class. concrete sub-classes are modified to reflect this advice
 *
 * Revision 1.14  2005/04/07 10:04:26  ok84741
 * add functions to remove occurances of a certain string from within a given string
 *
 * Revision 1.13  2005/03/22 11:39:32  am75275
 * Branch Merger to Trunk REL_141_NACD_BRANCH_700_06_20050315
 *
 * Revision 1.12.2.2  2004/11/17 21:55:17  nk68589
 * SM # 1258  ts3: feeds enhancements
 *
 * Revision 1.12.2.1  2004/10/12 20:35:26  nk68589
 * 1152  Remove Perl5Util references in StrUtil.java
 *
 * Revision 1.12  2004/06/03 15:13:53  rv63599
 * Added java 1.4 regex to search and replace in java Strings
 *
 * Revision 1.11  2004/03/07 00:22:21  rv63599
 * Added new variables
 *
 * Revision 1.10  2003/12/11 16:34:22  nk68589
 * splitAndTrimWithBlank() is added and is used by Workflow utility.
 *
 * Revision 1.9  2003/10/29 00:07:56  nk68589
 * removeSpacesInBetween is modified so as not to reduce the value to 32 char.
 *
 * Revision 1.8  2003/10/23 16:06:26  nk68589
 * Code is removed to replace Long name to Short name.
 *
 * Revision 1.7  2003/08/28 14:55:34  nk68589
 * Log statement is added.
 *
 * Revision 1.6  2003/06/26 20:53:28  nk68589
 * getId method is added.
 *
 * Revision 1.5  2003/06/16 14:31:00  nk68589
 * New methos doesVectorContains(Vector v,
 * String val) is added.
 *
 * Revision 1.4  2003/01/14 22:32:15  nk68589
 * com.citigroup.migration.util.StringUtil code is moved
 * to StrUtil class.
 *
 * Revision 1.3  2002/12/26 18:14:12  rv63599
 * Added routines to fix Strings for SQL
 *
 * Revision 1.2  2002/12/23 21:04:56  rv63599
 * Added splitAndTrim() method
 *
 * Revision 1.1  2002/12/20 20:41:43  rv63599
 * Initial Addition
 *
 *
 */
public class StrUtil {
	
	public static final String QUOTE = "'";

	/**
	 * The default implementation of split. Calls splitAndTrim(Srting, String, false)
	 * @param str
	 * @param delimiter
	 * @return
	 */
	public static List split(String str, String delimiter) {
		return splitAndTrim(str, delimiter, false);
	}

	/**
	 * The regular java string tokenizer that trims each token. Note that 
	 * with java implementation, if string contains two delimited chars together, 
	 * it does not recognise them as individual blank entries 
	 * @param str
	 * @param delimiter
	 * @param trim
	 * @return
	 */
	public static List splitAndTrim(String str, String delimiter, boolean trim) {
		List ret = new ArrayList();
		if ((str != null) && !str.equals("")) {
			String token;
			StringTokenizer st = new StringTokenizer(str, delimiter);
			while(st.hasMoreTokens()) {
				if (((token = (String) st.nextToken()) != null) && trim) {
					token = token.trim();
				}
				ret.add(token);
			}
		}

		return ret;
	}

	/**
	 * To add more fucntionality that javas string tokenizer, it places an empty string
	 * for the index where two delimeters are in sequence
	 * @param str
	 * @param delimiter
	 * @param trim
	 * @return
	 */
	public static List splitAndTrimWithEmpty(String str, String delimiter, boolean trim) {
		List ret = new ArrayList();

		if ((str != null) && !str.equals("")) {
			String token, prevToken;
			StringTokenizer st = new StringTokenizer(str, delimiter, true);
			prevToken = "";
			while(st.hasMoreTokens()) {
				if (((token = (String) st.nextToken()) != null) && 
					!token.equals(delimiter) && trim) {
					token = token.trim();
				}
				if (token.equals(delimiter)) {
					if (token.equals(prevToken)) {
						ret.add("");
					}
					prevToken = token;
					continue;
				} else {
					prevToken = token;
					ret.add(token);
				}
			}
		}

		return ret;
	}

	/**
	 * Fix the passed in String to be used correctly in SQL statements <br>
	 * For eg., "'" (A single quote) should be followed by another single
	 * quote<br> to be correctly interpreted by the SQL interpreter.
	 * 
	 * @param strOrig Original String that was passed in
	 * @return String Fixed String to be used with SQL
	 */
	public static String fixStringForSQL(String strOrig) {
		int index = -1;
		String ret = strOrig;
		
		if (strOrig != null && !strOrig.equals("")) {
			if ((index = strOrig.indexOf(QUOTE)) != -1) {
				ret = ioSQL.string2SQLString(strOrig);
			}
		}
		return ret;
	}
	
	/**
	 * Appends a string to a string which has been fixed for SQL.<br>
	 * Logic: Remove the final Quote and append the String and Quote.<br>
	 * 
	 * @see #fixStringForSQL
	 * @param strSQL String that was fixed for SQL Parser
	 * @param strAppend String to Append
	 * @return String SQL Fixed String with the string correctly appended
	 */
	public static String appendToSQLString(String strSQL, String strAppend) {
		int len;
		String ret = strAppend;
		
		if (strSQL != null && strAppend != null) {
			if (strSQL.endsWith(QUOTE)) {
				StringBuffer sb = new StringBuffer();
				if ((len = strSQL.length()) >= 2) {
					sb.append(strSQL.substring(0, len - 1));
				} 

				sb.append(strAppend);
				sb.append(QUOTE);
				ret = sb.toString();
			} else {
				ret = strSQL + strAppend;
			}
		}
		
		return ret;
	}

	/**
	*Checks if the supplied String have more characters then allowed then it
	*removes the extra characters .
	*@param String actualString
	*@return String modString.
	*/
	public static String correctName(String actualString) {
		if (actualString.length() < 1)
			return actualString;
		String modString = actualString.trim();
		if(modString.length() >= Constants.maxLegalEntityLength)
			modString = modString.substring(0,(Constants.maxLegalEntityLength));
		return modString;
	}

	/**
	*This method removes all the spaces (including space in between the words) from the supplied string.
	*@param String oldString.
	*@return String newString.
	*/
	public static String removeSpaces(String oldString) {
		String newString="";
		for(int count = 0; count <oldString.length(); count++) {
			if(oldString.charAt(count) != ' ')
				newString=newString+oldString.charAt(count);
		}
		return newString;
	}

	/**
	*This method all removes the spaces but leaves one space in between the words.
	*@param String oldString.
	*@return String newString.
	*/
	public static String removeSpacesInBetween(String rawStr) {
		if(rawStr == null)
			return rawStr;
		String ret = rawStr.replaceAll("( )+", " ");
		return ret;
	}

	public static String searchAndReplace(String oldStr, String pattern, String replacement) {
		Pattern p =  Pattern.compile(pattern);
		Matcher m = p.matcher(oldStr);
		String ret = m.replaceAll(replacement);
		if (ret == null) {
			ret = oldStr;
		}
		return ret;
	}
	
	/**
	*This method replaces the long values with their appropriate abbreviations.
	*@param String rawStr.
	*@return String modifiedStr.
	*/
	public static String replaceLongName(String rawStr) {
		String modifiedStr = null;
		rawStr = rawStr.toUpperCase();
		int coId = rawStr.indexOf("COMPANY");
		int coLength = "COMPANY".length();
		int corpId = rawStr.indexOf("CORPORATION");
		int corpLength = "CORPORATION".length();
		int ltdId = rawStr.indexOf("LIMITED");
		int ltdLength = "LIMITED".length();
		int incId = rawStr.indexOf("INCORPORATED");
		int incLength = "INCORPORATED".length();

		if(coId > -1) //Has COMPANY word
			modifiedStr = rawStr.substring(0,coId) + "CO." + rawStr.substring(coId+coLength);
		else if(corpId > -1) //Has CORPORATION word
			modifiedStr = rawStr.substring(0,corpId) + "CORP." + rawStr.substring(corpId+corpLength);
		else if(ltdId > -1) //Has LIMITED word
			modifiedStr = rawStr.substring(0,ltdId) + "LTD." + rawStr.substring(ltdId+ltdLength);
		else if(incId > -1) //Has INCORPORATED word
			modifiedStr = rawStr.substring(0,incId) + "INC." + rawStr.substring(incId+incLength);
		else
			modifiedStr = rawStr;
		return modifiedStr;
	}

	/**
	*This method checks the length of the string and if the length is more then passed length
	*then truncates the extra characters.
	*@param String rawStr.
	*@return String modifiedStr.
	*/
	public static String getProperVal(String longStr, int len) {
		if(longStr == null || longStr.length() < len)
			return longStr;
		String properStr = longStr.substring(0, len);
		return properStr;
	}
	
	public static boolean doesVectorContains(Vector values, String value) {
		boolean exist = false;
		for(int i = 0; i< values.size(); i++){
			String existingVal = (String)values.elementAt(i);		
			if(existingVal.equalsIgnoreCase(value)){
				exist = true;
				break;
			}
		}
		return exist;
	}
	
	/**
	 * @param corpId
	 */
	public static int getCorpusId(String corpId) {
		 int lastIndex = corpId.lastIndexOf(":");
		 String corpusId = corpId.substring((lastIndex+1));
		 int id = 0;
		 try{
			id = Integer.parseInt(corpusId);
			Log.dbg("Leg id is = " + id + " for OASYS ID = " + corpId);
		 }
		 catch(NumberFormatException numExec){
			Log.warn("Corpus id " + corpusId + " is not a valid number");
		 }
		return id;
	}
	public static void main(String[] args) {
	}
	
	/**
	 * To add more fucntionality that javas string tokenizer, it places an null entry
	 * for the index where two delimeters are in sequence
	 * @param str
	 * @param delimiter
	 * @return
	 */
	public static List splitAndTrimWithBlank(String str, String delimiter) {
		List ret = new ArrayList();
		ret = getTokens(str, delimiter);	
		return ret;
	}

	/**
	 * To add more fucntionality that javas string tokenizer, it places an null entry
	 * for the index where two delimeters are in sequence. This uses the call to 
	 * getTokensIncludeLast()
	 * @param str
	 * @param delimiter
	 * @return
	 */
	public static List splitAndTrimWithBlankIncludeLast(String str, String delimiter) {
		List ret = new ArrayList();
		ret = getTokensIncludeLast(str, delimiter);	
		return ret;
	}
	
	// These two functions below differ on so far as getTokens() expects the
	// end of the tokenized string to end with the delimter, effectively discarding what 
	// is after the last delimter.
	// The function getTokensIncludeLast() places the text after the last occurance of the 
	// delimter into the list also. More suitable for reading excel delimeted files
	// //TODO see if these two functions are needed
	
	/**
	 * This call expects the end of the tokenized string to end with the delimter, effectively 
	 * discarding what test is located after the last delimter.
	 * @param str
	 * @param obj
	 * @return
	 */
	private static List getTokens(String str, String delimiter) {
		List tokens = new ArrayList();
		boolean flag = true;
		
		while (flag) {
			int delimitIndex = str.indexOf(delimiter);
			if (delimitIndex < 0){
				break;
			}
			String token = str.substring(0,delimitIndex);
			if (token.length() == 0)
				token = null;
			str = str.substring(delimitIndex+1);
			tokens.add(token);
		}
		return tokens;
	}

	/**
	 * This function places the text located after the last occurance of the delimter into 
	 * the returned list also. More suitable for reading excel delimeted files
	 * @param str
	 * @param delimiter
	 * @return
	 */
	private static List getTokensIncludeLast(String str, String delimiter) {
		List tokens = new ArrayList();
		boolean flag = true;
		
		while (flag) {
			int delimitIndex = str.indexOf(delimiter);
			if (delimitIndex < 0){
				tokens.add(str);
				break;
			}
			String token = str.substring(0,delimitIndex);
			if (token.length() == 0)
				token = null;
			str = str.substring(delimitIndex+1);
			tokens.add(token);
		}
		return tokens;
	}
	
	/**
     * This will return a array of Strings from the given string cols.
     * cols will be separated on the given delimiter string. i.e. ","
     * If the delimiter string is required in a field, then the escape
     * character(EodFeed.ESCAPE_CHARACTER) can be used to prevent the 
     * delimiter character being separated.
     * 
     * i.e.
     * cols="MTM=bob,CR01=fred,THETA=john\\,marry"
     * delimiter=","
     * escape="\\"
     * 
     * would return ["MTM=bob","CR01=fred","THETA=john,marry"]
     * 
     * 
     * @param cols the string to be separated
     * @param delimiter the separating string
     * @return an array of strings.
     */
	public static String[] getStringArray(String cols, String delimiter) {
		return getStringMappings(cols,delimiter,EodFeed.ESCAPE_CHARACTER);
	}
	
    /**
     * This will return a array of Strings from the given string cols.
     * cols will be separated on the given delimiter string. i.e. ","
     * If the delimiter string is required in a field, then the escape
     * character can be used to prevent the delimiter character being 
     * separated.
     * 
     * i.e.
     * cols="MTM=bob,CR01=fred,THETA=john\\,marry"
     * delimiter=","
     * escape="\\"
     * 
     * would return ["MTM=bob","CR01=fred","THETA=john,marry"]
     * 
     * 
     * @param cols the string to be separated
     * @param delimiter the separating string
     * @param escape the string to prevent separation.
     * @return an array of strings.
     */
    public static String[] getStringMappings(String cols, String delimiter, String escape) {
        if (cols == null)
            return null;
        StringTokenizer st = new StringTokenizer(cols, delimiter);
        //int colCount = st.countTokens();
        //String columns[] = new String[colCount];
        ArrayList al = new ArrayList();

        while(st.hasMoreTokens()) {
            String token = (String) st.nextToken();
			if (token != null) {
				token = token.trim();
				
				//need to examine the end of the previuos token,
				//if this contains the escape character we need to
				//add the current token to the previous one including the 
				//delimiter
				if(!al.isEmpty()){
					//Use startPos to ensure we only check the last few characters.
					String previousToken=(String)al.get(al.size()-1);
					int startPos = previousToken.length()-EodFeed.ESCAPE_CHARACTER.length();
					if(startPos<0)
						startPos=0;
					int index =previousToken.indexOf(EodFeed.ESCAPE_CHARACTER,startPos);
					if(index>-1){
						//found escape character need to join current token to previous
						token=previousToken.substring(0,startPos)+delimiter+token;
						al.remove(al.size()-1);
					}
				}
				al.add(token);
			}			
		}
        
        return (String[])al.toArray(new String[al.size()]);
    }	
    public static String[] getStringArray(String cols, String delimiter, String innerDelimiter) {
        if (cols == null)
            return null;
        String [] allColumns = getStringArray(cols, delimiter);
        String [] columns = new String[allColumns.length];
        
        for (int i = 0; i < allColumns.length; i++) {
            String col = allColumns[i];
            StringTokenizer st = new StringTokenizer(col, innerDelimiter);
            
            int j = 0;
            while(st.hasMoreTokens()) {
                String token = (String) st.nextToken();
                if (j ==0) { //First token is the field name and not the column name.
                    j++;
                    continue;
                }
    			if (token != null) {
    				token = token.trim();
    				columns[i] = token;
    				j = 0;
    			}			
    		}
        }
        
        return columns;
    }		
    
    /**
     * Return a String with all occurrences of the "from" String
     * within "original" replaced with the "to" String.
     * If the "original" string contains no occurrences of "from",
     * "original" is itself returned, rather than a copy.
     *
     * @param original the original String
     * @param from the String to replace within "original"
     * @param to the String to replace "from" with
     *
     * @returns a version of "original" with all occurrences of
     * the "from" parameter being replaced with the "to" parameter.
     */
      public static String replace(String original, String from, String to) {
         int from_length = from.length();

         if (from_length != to.length()) {
            if (from_length == 0) {
               if (to.length() != 0) {
                  throw new IllegalArgumentException("Replacing the empty string with something was attempted");
               }
            }

            int start = original.indexOf(from);

            if (start == -1) {
               return original;
            }

            char[] original_chars = original.toCharArray();

            StringBuffer buffer = new StringBuffer(original.length());

            int copy_from = 0;
            while (start != -1) {
               buffer.append(original_chars, copy_from, start - copy_from);
               buffer.append(to);
               copy_from = start + from_length;
               start = original.indexOf(from, copy_from);
            }

            buffer.append(original_chars, copy_from, original_chars.length - copy_from);

            return buffer.toString();
         }
         else
         {
            if (from.equals(to)) {
               return original;
            }

            int start = original.indexOf(from);

            if (start == -1) {
               return original;
            }

            StringBuffer buffer = new StringBuffer(original);

         // Use of the following Java 2 code is desirable on performance grounds...


         /*
         // Start of Java >= 1.2 code...
            while (start != -1) {
               buffer.replace(start, start + from_length, to);
               start = original.indexOf(from, start + from_length);
            }
         // End of Java >= 1.2 code...
         */

         // The *ALTERNATIVE* code that follows is included for backwards compatibility with Java 1.0.2...


         // Start of Java 1.0.2-compatible code...

            char[] to_chars = to.toCharArray();
            while (start != -1) {
               for (int i = 0; i < from_length; i++) {
                  buffer.setCharAt(start + i, to_chars[i]);
               }

               start = original.indexOf(from, start + from_length);
            }
         	// End of Java 1.0.2-compatible code...


            return buffer.toString();
         }
      }


    /**
     * Return a String with all occurrences of the "search" String
     * within "original" removed.
     * If the "original" string contains no occurrences of "search",
     * "original" is itself returned, rather than a copy.
     *
     * @param original the original String
     * @param search the String to be removed
     *
     * @returns a version of "original" with all occurrences of
     * the "from" parameter removed.
     */
      public static String remove(String original, String search) {
         return replace(original, search, "");
      }
      
      /**
       * Method to test if a string is a valid int ot float.
       * @param in
       * @return
       */
      public static boolean isNumber(String in){
          try{
              Double.parseDouble(in);
              return true;
          }catch(NumberFormatException e){
              return false;
          }
      }

      
}
 

