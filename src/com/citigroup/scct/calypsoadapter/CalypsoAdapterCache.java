/*
 * Created on Oct 27, 2006
 *
 * Nov 18 2008		kt60981			Cache ABX products
 * 
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.citigroup.scct.calypsoadapter;

import javax.naming.*;
import javax.jms.*;
import java.util.*;
import java.util.regex.Pattern;

import calypsox.apps.product.CDSTerminationWindow;
import calypsox.tk.product.CitiPortfolioCDO;
import calypsox.tk.service.CitiPortfolioServer;
import calypsox.tk.service.RemoteCitiPortfolio;
import calypsox.tk.service.RemoteCustomBondData;
import calypsox.tk.service.RemoteSTM;
import calypsox.tk.product.CitiCreditSyntheticCDO;
import calypsox.tk.product.CitiPortfolioCDO2;
import calypsox.tk.product.CitiPortfolioIndex;
import calypsox.tk.event.PSEventCitiPortfolio;
import calypsox.tk.refdata.*;
import calypsox.tk.service.RemoteIssuerData;

import com.citigroup.scct.cgen.*;
import com.citigroup.scct.server.*;
import com.citigroup.scct.util.*;
import com.citigroup.scct.util.DateUtil;
import com.citigroup.scct.valueobject.IssuerAttribute;

import org.apache.log4j.Logger;
import com.calypso.tk.core.*;
import com.calypso.tk.service.*;
import com.calypso.tk.event.*;
import com.calypso.tk.mo.TradeFilter;
import com.calypso.tk.refdata.FeeGrid;
import com.calypso.tk.refdata.Country;
import com.calypso.tk.product.Bond;
import com.calypso.tk.product.*;
import com.calypso.tk.product.Ticker;
import com.calypso.tk.core.DateRoll;
import com.calypso.tk.util.TimerRunnable;
import com.calypso.tk.bo.FeeDefinition;
import com.citigroup.scct.calypsoadapter.converter.*;
import com.calypso.tk.bo.BOCache;
import com.calypso.tk.service.LocalCache;
import com.calypso.tk.refdata.*;
import com.citigroup.product.bond.elements.BondContainer;
import com.citigroup.product.bond.elements.BondResult;
import com.citigroup.product.bond.search.SearchParams;
import com.citigroup.project.cef.data.entity.CEFLEEntity;
import com.citigroup.project.sql.IndexTradeCaptureData;
import com.calypso.tk.refdata.LegalEntityAttribute;
import com.calypso.tk.marketdata.CreditRating;
import java.rmi.RemoteException;

/**
 * @author mi54678
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 * 
 * 7/21/08		kt60981		Skip private template 
 * 
 */
public class CalypsoAdapterCache implements PSSubscriber , ConnectionListener  {
	/**
	 * Logger for this class
	 */
	private final Logger logger = Logger.getLogger(getClass().getName());
	
	private static final int LE_LIST_SIZE = 500;
	private static final String PC_TICKER_TYPE = "-pc-";
	protected static final String DOMAIN_TRADE_GROUPS = "citi_eblotter.trade_groups";
	protected static final String DOMAIN_PROT_LEG_TYPE = "citi_eblotter.prot_leg_type";
	protected static final String DOMAIN_PREM_LEG_TYPE = "citi_eblotter.prem_leg_type";
	protected static final String DOMAIN_STRUCTURE_DESK = "citi_eblotter.structure_desks";
	protected static final String DOMAIN_STRUCTURE_TYPE = "citi_eblotter.structure_types";
	protected static final String DOMAIN_SKEW_TYPE = "citi_eblotter.skew_type";
	protected static final String DOMAIN_DRAFT_TYPE = "citi_eblotter.draft_type";	
	protected static final String DOMAIN_TERMINATION_TYPE = "citi_eblotter.termination_type";
	protected static final String DOMAIN_PREM_LEG_FREQ = "citi_eblotter.prem_leg_frequency";
	protected static final String DOMAIN_INSTRUMENT_CLASS = "citi_eblotter.instrument_class";
	protected static final String DOMAIN_FORWARD_TYPE = "citi_eblotter.forward_type";
	protected static final String DOMAIN_TERMINATION_REASON = "terminationReason";
	protected static final String RATING_AGENCY = "ratingAgency";
	
	protected static final String FEE_DEFINITION_ASSIGNMENT = "ASSIGNMENT";	
	protected static final String FEE_DEFINITION_BRK = "BRK";
	protected static final String FEE_DEFINITION_COUPON = "COUPON";
	protected static final String FEE_DEFINITION_EXPECTED_VALUE = "EXPECTED_VALUE";
	protected static final String FEE_DEFINITION_UPFRONT = "UPFRONT";	
	protected static final String FEE_DEFINITION_TERMINATION = "TERMINATION";
	protected static final String FEE_DEFINITION_OFF_MARKET = "OFF_MARKET";	

	protected CalypsoDSConnection dsConnection;
	protected PSConnection ps;
	
	protected Hashtable bookIds;
	protected Hashtable bookNames;
	protected Hashtable legalEntityIds;
	protected Hashtable legalEntityNames;
	protected Hashtable tradeFilters;
	protected Hashtable tickerIds;
	protected Hashtable tickerNames;	
	//protected Hashtable _countries;
	protected Hashtable portfolios;
    protected HashMap domainValues;
	protected List rollConventions;
	protected List dayCounts;
	protected HashMap tradeGroups;
	//protected List currencies;
	protected HashMap<String, ScctHolidayCalendarType> holidayCalendars; // Essentially a wrapper for a number of collections.
	//protected List defaultLegTypes;
	//protected List premuimLegTypes;
	//protected Hashtable feeDefinitions;  
	//protected Hashtable scctFeeDefinitions;
	//protected List draftTypes;
	//protected List terminationTypes;
	//protected int CACHE_SIZE = 200;
	//protected static final String SKEW_TYPES_KEY = "skewTypes";
	//protected static final String STRUCTURE_TYPE_KEY="structureType";
	//protected static final String STRUCTURE_DESK_KEY="structureDesk";
	//protected static final String CDO_PREMUIM_LEG_TYPES_KEY="cdoPremuimLegTypes";
	//protected static final String CDO_DEFAULT_LEG_TYPES_KEY="cdoDefaultLegTypes";
	//protected static final String DRAFT_TYPES_KEY = "draftTypes";
	//protected static final String TERMINATION_TYPES_KEY = "terminationTypes";	
	protected ObjectFactory objectFactory;
	private com.calypso.tk.util.Timer psTimer;
	protected Hashtable userDefaults;
	protected HashMap allIndexTradeCaptureData;
	protected HashMap forwardTypes;
	protected Hashtable ratings;
	protected List terminationReason;
	protected List hedgeFundMnemonics;
	protected Hashtable cdsTradeTemplates;
	protected Hashtable cdoTradeTemplates;
	protected Hashtable cdsABSTradeTemplates;
	protected Hashtable cdsABXTradeTemplates;
	protected Hashtable bondContainers;
	protected Hashtable ratingAgencies;
	protected Hashtable productsById;
	protected Hashtable productsABS;
	protected Hashtable productsABSSecs;
	protected Hashtable productsWithId;
	protected Hashtable bondHashByCusip;
	protected Hashtable bondHashByIsin;
	protected Hashtable boFees;
	protected Vector feeDefns;
	protected Vector feeRoles;
	protected Vector leBookRoles;
	protected Vector leBrokerRoles;
	protected Vector leCptyRoles;
	protected Vector leFirmLERoles;
	protected Vector leSalesRoles;
	protected Vector seniority;
	protected Vector terminationPmt;
	protected Hashtable marxIds;
	protected Hashtable prodHashByCusip;
	protected Hashtable prodHashByIsin;
	protected Hashtable abxDefByIsin;
	protected Hashtable abxIndexDefn;
	
	protected Pattern validTickerFilter = Pattern.compile(".*-pc-.*[0-9]+$");
	
	public CalypsoAdapterCache(CalypsoDSConnection ds_){
		dsConnection = ds_;
		objectFactory = new ObjectFactory();
	}
	
	public void initialize() throws Exception {
		// create cache objects
		domainValues = new HashMap();
		bookIds = new Hashtable();
		bookNames = new Hashtable();
		legalEntityIds = new Hashtable();
		legalEntityNames = new Hashtable();
		tradeFilters = new Hashtable();
		tickerIds = new Hashtable();
		tickerNames = new Hashtable();
		//_countries = new Hashtable();
		rollConventions = new ArrayList();
		dayCounts = new ArrayList();
		//currencies = new ArrayList();
		holidayCalendars = new HashMap();
		portfolios = new Hashtable();
		userDefaults = new Hashtable();
		tradeGroups = new HashMap();
		allIndexTradeCaptureData = new HashMap();
		forwardTypes = new HashMap();
		ratings = new Hashtable();
		terminationReason = new ArrayList();
		hedgeFundMnemonics = new ArrayList();
		cdsTradeTemplates = new Hashtable();
		cdoTradeTemplates = new Hashtable();
		cdsABSTradeTemplates = new Hashtable();
		cdsABXTradeTemplates = new Hashtable();
		bondContainers = new Hashtable();
		ratingAgencies = new Hashtable();
		productsById = new Hashtable();
		productsABS = new Hashtable();
		productsABSSecs = new Hashtable();
		productsWithId = new Hashtable();
		bondHashByCusip = new Hashtable();
		bondHashByIsin = new Hashtable();
		prodHashByCusip = new Hashtable();
		prodHashByIsin = new Hashtable();
		boFees = new Hashtable();
		feeDefns = new Vector();
		feeRoles = new Vector();
		seniority = new Vector();
		terminationPmt = new Vector();
		marxIds = new Hashtable();
		abxDefByIsin = new Hashtable();
		abxIndexDefn = new Hashtable();
		
		// subscribe to ds connections
		loadData();
		
		dsConnection.getDSConnection().addListener(this);
		connectToPSServer(false);
	}
	
	public void loadData() throws Exception{
		loadMarxIds();
		getBooks();
		loadLegalEntities();
		loadSkewTypes();
		loadRollConventions();
		loadDayCounts();
		getCountries();
		loadCurrencies();
		loadHolidayCalendars();
		loadStructureDesks();
		loadStructureTypes();
		getTraders();
		getSalesPersons();
		getStructurers();
		loadCDOProductLegTypes();
		loadFeeDefinitions();
		loadTickers();
		getPortfolios();
		loadDraftTypes();
		loadTerminationTypes();
		loadUserDefaults();
		getInitialMarginPercent();
		getInitialMarginRole();
		getRTTRule(); //RTT stands for RightToTerminate
		getRTTFrequency();
		getPremLegFrequency();		
		getPremiumStubRule();
		getCDOClassNames();
		loadTradeGroups();
		loadIndexTradeCaptureData();
	    getNonModifiableTradeStatus();
	    getNonModExternalSystem();
	    getCitiBooksInvalidWithoutMirror();
		loadForwardTypes();
		loadRatings();
		loadTerminationReasonTypes();
		loadHedgeFundMnemonics();
		loadTradeTemplates();
		loadBondContainer();
		loadRatingAgencies();
		loadSeniority();
		loadTerminationPmt();
	}
	
	protected void loadCDOProductLegTypes(){
		/*this.defaultLegTypes = new ArrayList();
		this.premuimLegTypes = new ArrayList();
		
		// to load later from domain values
		
		defaultLegTypes.add("StandardDefaultLeg");
		defaultLegTypes.add("ZeroDefaultLeg");
		defaultLegTypes.add("SingleDefaultPaymentAtEnd");
		
		premuimLegTypes.add("StandardPremiumLeg");
		premuimLegTypes.add("SinglePremiumPaymentAtEnd");
		premuimLegTypes.add("Floating");

    	domainValues.put(CalypsoAdapterCache.CDO_DEFAULT_LEG_TYPES_KEY, defaultLegTypes);
    	domainValues.put(CalypsoAdapterCache.CDO_PREMUIM_LEG_TYPES_KEY, premuimLegTypes);*/
		getCDODefaultLegTypes();
		getCDOPremiumLegTypes();
	}
	
	public List getCDODefaultLegTypes(){
		//return (List)domainValues.get(CalypsoAdapterCache.CDO_DEFAULT_LEG_TYPES_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_PROT_LEG_TYPE);
	}
	
	
	public List getCDOPremiumLegTypes(){
		//return (List)domainValues.get(CalypsoAdapterCache.CDO_PREMUIM_LEG_TYPES_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_PREM_LEG_TYPE);		
	}

	
    public List getTraders(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.TRADER);
    }
    
    public List getSalesPersons(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.SALES_PERSON);
    }
    
   
    public List getStructurers(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.TRADER);
    }
    
    public List getBrokers(){
    	// TBD
    	throw new RuntimeException("Not Supported yet");
    }
    
    // get from adapter domain cache
	public List getDomainValues(String domainName) {

		synchronized (domainValues) {
			List v = (List) domainValues.get(domainName);
			if (v != null)
				return v;
			return null;
		}
	}
	
	protected void loadHolidayCalendars() {
		try {
			Holiday currentHoliday = dsConnection.getDSConnection().getRemoteReferenceData().getHolidays();			
			HashMap holidayDates = currentHoliday.getDates();
			for(Object o_code : holidayDates.keySet()) {
				String code = (String) o_code;
				HashMap dates = (HashMap) holidayDates.get(code);
				ScctHolidayCalendarType  scctHoliday = new ScctHolidayCalendarType();
				scctHoliday.setCode(code);
				for(Object o_date : dates.keySet()) {
					JDate date = (JDate) o_date;				
					scctHoliday.getDate().add(DateUtil.convertJDatetoString(date));
				}
				Collections.sort(scctHoliday.getDate());
				holidayCalendars.put(code, scctHoliday);
			}
		} catch (RemoteException e) {
			logger.warn("Unable to load Holiday Calendars");
		}
	}
	
	public List getHolidayCalendars() {
		Vector v = Holiday.getCurrent().getHolidayCodeList();
		return Collections.unmodifiableList(ScctUtil.vectorToList(v));
	}

	public HashMap getHolidayData() {
		return holidayCalendars;
	}
	
	public List<ScctHolidayCalendarType> getHolidayCalendars(Vector holidayCodes) {
		List<ScctHolidayCalendarType> result = new Vector<ScctHolidayCalendarType>();
		for(Object o_holidayCode : holidayCodes) {
			String holidayCode = (String) o_holidayCode;
			ScctHolidayCalendarType holidayData = holidayCalendars.get(holidayCode);
			if(holidayData == null) {
				result.add(holidayData);
			}
		}
		return result;
	}
	
	public Hashtable getCDSTradeTemplates() {
		return cdsTradeTemplates;
	}
	
	public Hashtable getCDOTradeTemplates() {
		return cdoTradeTemplates;
	}
	
	public Hashtable getCDSABSTradeTemplates() {
		return cdsABSTradeTemplates;
	}
	
	public Hashtable getCDSABXTradeTemplates() {
		return cdsABXTradeTemplates;
	}
	
	public Hashtable getRatingAgency() {
		return ratingAgencies;
	}
	
	protected void loadRollConventions() throws Exception{
		// a vector of Strings.
		rollConventions.addAll(DateRoll.getDomain());
	}

	public List getRollConvetions(){
		return Collections.unmodifiableList(rollConventions);
	}
	
	protected void loadDayCounts(){
		dayCounts.addAll(DayCount.getDomain());
	}
	
	public List getDayCounts(){
		// returns a vector of Strings
		return Collections.unmodifiableList(dayCounts);
	}
	
	protected void loadCurrencies(){
		// a vector of scrtings
		LocalCache.refreshCurrencyUtil(dsConnection.getDSConnection());
	}
	
	public List getCurrencies(){
		List l = ScctUtil.vectorToList(LocalCache.getCurrencies());
		return Collections.unmodifiableList(l);
	}
	
	
	// CACHE GET METHODS INTERFACE SECTION
	public Book getBook(int id) {
		if (id <= 0)
			return null;

		Book book;
		synchronized (bookIds) {
			book = (Book) bookIds.get(new Integer(id));
		}
		if (book != null)
			return book;
		else
		{
			Book b = BOCache.getBook(dsConnection.getDSConnection(), id);
			if(b == null){
				try{
					dsConnection.getDSConnection().getRemoteReferenceData().getBook(id);
				}
				catch(Exception e){
					logger.error(e.getMessage(),e);
				}
			}
			if (b != null) {
				bookIds.put(new Integer(id), b);
				bookNames.put(b.getName(), b);
			}
			return b;
		}
	}


	public Book getBook(String name) {
    	if (!StringUtilities.isFilled(name ))
    		return null;

		Book book;
		synchronized (bookNames) {
			book = (Book) bookNames.get(name);
		}
		if (book != null)
			return book;
		else
		{
			Book b = BOCache.getBook(dsConnection.getDSConnection(), name);
			if(b == null){
				try{
					dsConnection.getDSConnection().getRemoteReferenceData().getBook(name);
				}
				catch(Exception e){
					logger.error(e.getMessage(),e);
				}
			}
			if (b != null) {
				bookIds.put(new Integer(b.getId()), b);
				bookNames.put(name, b);
			}
			return b;
		}
	}
	
    public Hashtable getBooks() throws Exception{
    	logger.debug("loading books ...");
		long before=System.currentTimeMillis();
		Book book = null;
    	synchronized (bookIds) {
			synchronized (bookNames) {
				bookNames = BOCache.getBooks(dsConnection.getDSConnection());
				if(bookNames == null || bookNames.size() == 0){
					bookNames = dsConnection.getDSConnection().getRemoteReferenceData().getBooks();
				}
				Iterator iter = bookNames.keySet().iterator();
				while (iter.hasNext()) {
					book = (Book)bookNames.get((String)iter.next());
					bookIds.put(new Integer(book.getId()), book);
				}
			}
    	}
		long after=System.currentTimeMillis();	
		logger.debug("Loaded Books " + bookIds.size() +
			       " in " + ((double)(after-before))/1000. 
			       + " seconds.");
    	return bookNames;
	}
    
    public Hashtable getBookNames() throws Exception {
    	if(bookNames == null || bookNames.size() == 0) {
    		getBooks();
    	}
    	return bookNames;
    }
    
    protected void loadSkewTypes(){
    	// TODO need to add to calypso as domain value pairs.
    	// this is the current list as of Nov 10th, 2006
    	/*ArrayList skewTypes = new ArrayList();
    	
    	skewTypes.add("cdx");
    	skewTypes.add("skew");
       	skewTypes.add("hyboxx");
    	skewTypes.add("itraxx");
    	skewTypes.add("itraxxasia");
    	skewTypes.add("cdx1");
    	skewTypes.add("cdx2");
    	skewTypes.add("cdx3");
    	skewTypes.add("cdx4");
    	skewTypes.add("cdx5");
    	skewTypes.add("itraxxJapan");
    	skewTypes.add("EMCDX");
     	skewTypes.add("cdx6");
     	skewTypes.add("hycdx6");
     	skewTypes.add("hycdx5");
     	skewTypes.add("clo");
     	skewTypes.add("iopo");
     	skewTypes.add("cdx7");     	
     	skewTypes.add("hycdx7");
    
    	domainValues.put(CalypsoAdapterCache.SKEW_TYPES_KEY, skewTypes);*/
    	getSkewTypes();
    }
    
    public Product getProduct(String cusip){
    	Product product = null;
    	try{
    		if(cusip != null){
	    		if(bondHashByCusip != null){
	    			product = (Product)bondHashByCusip.get(cusip);
	    		}
	    		if(product == null){
		    		product = dsConnection.getDSConnection().getRemoteProduct().
		    		getProductByCode(CalypsoAdapterConstants.CUSIP,cusip);
	    		}
    		}
    	}
    	catch(Exception e){
    		logger.error(e);
    	}
    	return product;
    }
    
    public Product getProduct(int id){
    	Product product = null;
    	try{
    		product = dsConnection.getDSConnection().getRemoteProduct().getProduct(id);
    	}
    	catch(Exception e){
    		logger.error(e);
    	}
    	return product;
    }
    
    public Product getProductByIsin(String isin){
    	Product product = null;
    	try{
    		if(isin != null){
	    		if(bondHashByIsin != null){
	    			product = (Product)bondHashByIsin.get(isin);
	    		}
	    		if(product == null){
		    		product = dsConnection.getDSConnection().getRemoteProduct().
		    		getProductByCode(CalypsoAdapterConstants.ISIN,isin);
	    		}
    		}
    	}
    	catch(Exception e){
    		logger.error(e);
    	}
    	return product;
    }
    
    public Product getBondById(int id){
    	Product product = null;
    	try{
    		if(id > 0){
	    		if(productsWithId != null){
	    			product = (Product)productsWithId.get(new Integer(id));
	    		}
	    		if(product == null){
		    		product = getProduct(id);
	    		}
    		}
    	}
    	catch(Exception e){
    		logger.error(e);
    	}
    	return product;
    }
    
    protected void loadStructureDesks(){
    	// TODO this is the list taken from PT database as of Nov 2006
    	// need to add to calypso as domain value pairs.
    	/*ArrayList desk = new ArrayList();
  
    	desk.add("CT NY");
		desk.add("CT LN");
		desk.add("CT TK");
		desk.add("CT HK");
    	domainValues.put(CalypsoAdapterCache.STRUCTURE_DESK_KEY, desk);*/
    	getStructureDesks();
    }
    
    public List getStructureDesks(){
    	//return this.getDomainValues(CalypsoAdapterCache.STRUCTURE_DESK_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_STRUCTURE_DESK);    	
    }
    
    public List getStructureTypes(){
    	//return this.getDomainValues(CalypsoAdapterCache.STRUCTURE_TYPE_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_STRUCTURE_TYPE);
    }
    
    protected void loadStructureTypes(){
    	/*ArrayList types = new ArrayList();
    	// this is what pt has today, we need to add it to domain value pairs.
    	types.add("Swap");
    	types.add("Repack");
    	types.add("CLN");
    	domainValues.put(CalypsoAdapterCache.STRUCTURE_TYPE_KEY, types);*/
    	getStructureTypes();
    }
    
    public  List getSkewTypes(){
    	//return this.getDomainValues(CalypsoAdapterCache.SKEW_TYPES_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_SKEW_TYPE);
    }
    
    public LegalEntity getLegalEntity( int id) {
		if (id <= 0)
			return null;
		LegalEntity le = null;
	
		synchronized (legalEntityIds) {
			le = (LegalEntity)legalEntityIds.get(new Integer(id));
		}
		if (le != null && le.getRoleList() != null && le.getRoleList().size() > 0){
			return le;
		}
		le = BOCache.getLegalEntity(dsConnection.getDSConnection(), id);
		if(le == null || le.getRoleList() == null || le.getRoleList().size() == 0){
			try{
				le = dsConnection.getDSConnection().getRemoteReferenceData().getLegalEntity(id);
			}
			catch(Exception e){
				logger.error(e.getMessage(),e);
			}
		}
		if (le != null) {
			synchronized (legalEntityIds) {
				legalEntityIds.put(new Integer(id), le);
			}
			synchronized (legalEntityNames) {
				legalEntityNames.put(le.getCode(), le);
			}
		}
		return le;
	}

    public LegalEntity getLegalEntity(String name) {
    	if (!StringUtilities.isFilled(name ))
    		return null;
    	LegalEntity le=null;
    	
    	synchronized(legalEntityNames) {
    		le=(LegalEntity)legalEntityNames.get(name);
    	}
	
    	if (le != null && le.getRoleList() != null && le.getRoleList().size() > 0){
    		return le;
    	}
	
		le = BOCache.getLegalEntity(dsConnection.getDSConnection(), name);
		if(le == null || le.getRoleList() == null || le.getRoleList().size() == 0){
			try{
				le = dsConnection.getDSConnection().getRemoteReferenceData().getLegalEntity(name);
			}
			catch(Exception e){
				logger.error(e.getMessage(),e);
			}
		}
		if (le != null) {
			synchronized (legalEntityIds) {
				legalEntityIds.put(new Integer(le.getId()), le);
			}
			synchronized (legalEntityNames) {
				legalEntityNames.put(name, le);
			}
		}
		return le;
	}
    
    public void loadLegalEntities() {
    	logger.debug("loading legal entities ...");
    	try {
    		long before=System.currentTimeMillis();
			Vector v = dsConnection.getDSConnection().getRemoteReferenceData().getAllLE(null);
			BOCache.putLEInCache(v);
			LegalEntity le = null;
	    	synchronized (legalEntityIds) {
				synchronized (legalEntityNames) {
					Iterator iter = v.iterator();
					while (iter.hasNext()) {
						le = (LegalEntity)iter.next();
						legalEntityIds.put(le.getId(), le);
						legalEntityNames.put(le.getCode(), le);
						if(le != null && (le.getRoleList() == null || le.getRoleList().size() == 0)){
							logger.debug("No roles on legal entity="+le.getCode());
						}
					}
				}
	    	}
	    	long after = System.currentTimeMillis();	
			logger.debug("Loaded Legal entities and attributes for " + v.size() +
				       " legal entities in " + ((double)(after-before))/1000.0 
				       + " seconds.");

			leBookRoles = BOCache.getLegalEntitiesForRole(dsConnection.getDSConnection(), "Book");
	    	leBrokerRoles = BOCache.getLegalEntitiesForRole(dsConnection.getDSConnection(), "Broker");
	    	leCptyRoles = BOCache.getLegalEntitiesForRole(dsConnection.getDSConnection(), "CounterParty");
	    	leFirmLERoles = BOCache.getLegalEntitiesForRole(dsConnection.getDSConnection(), "FirmLegalEntity");
	    	leSalesRoles = BOCache.getLegalEntitiesForRole(dsConnection.getDSConnection(), "SalesPerson");

	    	long last = System.currentTimeMillis();	
			logger.debug("Loaded Legal entities roles : " + ((double)(last-after))/1000.0 + " seconds.");
		} catch (Exception e) {
			Log.error(this, e);
		}
	}
 
    private void loadMarxIds() {
      logger.debug("loading MARX ids ...");
      try {
    	long before=System.currentTimeMillis();
		Vector v = dsConnection.getDSConnection().getRemoteReferenceData().getLegalEntityAttributes("legal_entity_role", " legal_entity_role.legal_entity_id = le_attribute.legal_entity_id and legal_entity_role.role_name = 'Issuer' and le_attribute.attribute_type = 'CITI_GCDR_ID'");
		LegalEntityAttribute lea = null;
		Iterator iter = v.iterator();
		while (iter.hasNext()) {
			lea = (LegalEntityAttribute) iter.next();
			marxIds.put(lea.getLegalEntityId(), lea.getAttributeValue());
		}
	    	
		long after = System.currentTimeMillis();	
		logger.debug("Loaded MARX ids for " + v.size() + "\t" + ((double)(after-before))/1000.0 + " seconds.");
      } catch (Exception e) {
		Log.warn(e.getMessage(), e);
      }
	}

    public List getLegalEntityByCodeLike(String code, String role) {
		long before=System.currentTimeMillis();
		if (!StringUtilities.isFilled(code))
			return null;
		String name;
		List leList = new ArrayList();
		LegalEntity le = null;

		String like = code.toUpperCase();
		if (legalEntityNames == null) return null;
	    Iterator iter = legalEntityNames.keySet().iterator();
	    while (iter.hasNext()) {
	        name = (String)iter.next();
			if (name.startsWith(like)) {
		        le = (LegalEntity)legalEntityNames.get(name);
				if (le.getRoleList().contains(role)) 
					leList.add(le);

			}
		}
		long after=System.currentTimeMillis();	
		logger.debug("Lookup of legal entities by " + code + " returned "+ leList.size() +
			       " in " + ((double)(after-before))/1000. 
			       + " seconds.");
		return leList;
    }
    
    public Hashtable getLegalEntities()
    {
    	Hashtable retVal;
    	synchronized(legalEntityNames) {
    		retVal = (Hashtable) legalEntityNames.clone();
    	}
    	return retVal;
    }
    
    public LegalEntity getLegalEntity(String name, String role) {
    	if (!StringUtilities.isFilled(name) || !StringUtilities.isFilled(role))
    		return null;
    	LegalEntity le=null;
    	
    	synchronized(legalEntityNames) {
    		le=(LegalEntity)legalEntityNames.get(name);
    	}
	
    	if (le != null) {
    		if (le.hasRole(role))
    			return le;
    	}
    	else {
			le = BOCache.getLegalEntity(dsConnection.getDSConnection(), name);
			if (le != null) {
				synchronized (legalEntityIds) {
					legalEntityIds.put(new Integer(le.getId()), le);
				}
				synchronized (legalEntityNames) {
					legalEntityNames.put(name, le);
				}
				if (le.hasRole(role))
					return le;
			}
    	}
		return le;
	}
    
    public List getLegalEntities(String role){
		long before=System.currentTimeMillis();    	
 		LegalEntity le = null;
		List leList = new ArrayList();

		String name = null;
	    Iterator iter = legalEntityNames.keySet().iterator();
	    while (iter.hasNext()) {
	        name = (String)iter.next();
	        le = (LegalEntity)legalEntityNames.get(name);
			if (le.getRoleList().contains(role)) {
				leList.add(le);
			}
	    }
		long after=System.currentTimeMillis();	
		logger.debug("Lookup of legal entities by " + role + " returned "+ leList.size() +
			       " in " + ((double)(after-before))/1000. 
			       + " seconds.");
		/*if (leList.size() > LE_LIST_SIZE )
			return leList.subList(0, LE_LIST_SIZE);
		else */
			return leList;
    }

    public LegalEntityAttribute getLEAttribute(int id) {
    	LegalEntityAttribute lea = null;
    	if (id>0) {
    		lea = (LegalEntityAttribute) marxIds.get(id);
    	}
    	return lea;
    }
    
    public String getMarxId(int leId) {
    	String attrVal = null;
    	if (leId>0 && marxIds.get(leId) !=null) {
    		attrVal = (String) marxIds.get(leId);
    	}
    	if(leId> 0 && attrVal == null){
    		Vector v = null;
    		try{
    			v = dsConnection.getDSConnection().getRemoteReferenceData().getAttributes(leId);
    		}
    		catch(Exception e){
    			logger.error(e.getMessage(),e);
    		}
    		if(v != null){
	    		for(int i=0;i<v.size();i++){
	    			LegalEntityAttribute leAttribute = (LegalEntityAttribute)v.get(i);
	    			if(leAttribute != null && leAttribute.getAttributeType().equalsIgnoreCase("CITI_GCDR_ID")){
	    				attrVal = leAttribute.getAttributeValue();
	    			}
	    		}
    		}
    	}
    	return attrVal;
    }
    
    public TradeFilter getTradeFilter(String filterName) {
		TradeFilter filter = null;
		synchronized (tradeFilters) {
			filter = (TradeFilter) tradeFilters.get(filterName);
			if (filter == null) {
				try {
					filter = dsConnection.getDSConnection().getRemoteReferenceData().getTradeFilter(filterName);
				} catch (Exception e) {
					Log.error(this, e);
				}
				if (filter == null)
					return null;
				tradeFilters.put(filterName, filter);
			}
		}
		return filter;
	}
	
    public List getScctFeeDefinitions(){
    	List list = this.getFeeDefintions();
    	ArrayList sfees = new ArrayList(list.size());
    	FeeDefinition fd = null;
    	ScctFeeDefinitionType sfd = null;
    	for (Iterator it = list.iterator(); it.hasNext(); ) {
    		fd = (FeeDefinition)it.next();
    		try {
    			sfd = ConverterFactory.getInstance().getFeeDefinitionConverter().convertFromCalypso(fd);
    			sfees.add(sfd);
    		}
    		catch(ConverterException e) {
    			System.err.println("Error converting FeeDefinition to ScctFeeDefinition type: " + e.toString());
    		}
    	}
    	return sfees;
    }
    
    protected void loadFeeDefinitions(){
		try {
			RemoteBackOffice bo = this.dsConnection.getDSConnection().getRemoteBO();
			Vector definitions = bo.getFeeDefinitions();
			int size = (definitions !=null ? definitions.size() : 0);
			FeeDefinition defn = null;
			synchronized (boFees) {
				for (int i = 0; i < size; i++) {
					defn = (FeeDefinition) definitions.get(i);
					boFees.put(defn.getType(), defn);
					feeDefns.add(defn.getType());
					feeRoles.add(defn.getRole());
				}
			}
		} catch (RemoteException e) {
			logger.warn("Caught Exception loading FeeDefinition : " + e.getMessage(), e);
		}
    }
    
    public List getFeeDefintions(){
	    return feeDefns;
    }
    
    public List getFeeRoles() {
    	return feeRoles;
    }
    
    public FeeDefinition getFeeDefinition(String type) {
    	FeeDefinition feeDefn = null;
    	if (StringUtilities.isFilled(type)) {
    		feeDefn = (FeeDefinition) boFees.get(type);
    	}

    	return feeDefn;
    }
    
    public ScctFeeDefinitionType getScctFeeDefinition(String type){
    	FeeDefinition fd = this.getFeeDefinition(type);
    	if (fd == null)
    		return null;
    	ScctFeeDefinitionType sfd = null;
		try {
			sfd = ConverterFactory.getInstance().getFeeDefinitionConverter().convertFromCalypso(fd);
		}
		catch(ConverterException e) {
			Log.error(this, "Error converting FeeDefinition to ScctFeeDefinition type: " + e);
		}
    	return sfd;
    }
    
    protected static Vector getSupportedFeeTypes() {
		Vector v = new Vector();
		v.addElement(FEE_DEFINITION_ASSIGNMENT);
		v.addElement(FEE_DEFINITION_BRK);
		v.addElement(FEE_DEFINITION_COUPON);
		v.addElement(FEE_DEFINITION_EXPECTED_VALUE); // gg73248: this is additional. But dont want to break the existing code.
		v.addElement(FEE_DEFINITION_UPFRONT);
		v.addElement(FEE_DEFINITION_TERMINATION);
		v.addElement(FEE_DEFINITION_OFF_MARKET);
		
		//gg73248: Added the below code to support multiple EV Fee Types
		v.addElement(ScctUtil.getEVFeesVector());
		return v;
    }
    
    public FeeGrid getFeeGrid(int id) {
 
    	if(id==0) return null;
    	return null;
    	/*
	FeeGrid found=null;
	if (!_isCaching)  {
	    try {
		found=ds.getRemoteReferenceData().getFeeGrid(id);
	    } catch (Exception e) {Log.error(this, e);}
	    return found;
	}
	//checkCache(_feeGridIds);
	synchronized(_feeGridIds) {
	    found=(FeeGrid)_feeGridIds.get(new Integer(id));
	}
	if(found != null) return found;
	try {
	    found=ds.getRemoteReferenceData().getFeeGrid(id);
	    if(found != null) {
		synchronized(_feeGridIds) {
		    _feeGridIds.put(new Integer(id),found);
		}
	    }
	} catch (Exception e) { Log.error(this, e);}
	return found;
	*/
    }
    

    public void loadTickers() {
    	logger.debug("loading tickers ...");
		long before=System.currentTimeMillis();
		// Valid tickers have the following format:  IBM-pc-USD-SEN-12412
    	Vector v = null;
		try {
			v = dsConnection.getDSConnection().getRemoteProduct().getTickers(null, null);
		}
		catch (Exception e) {
			Log.error(this, "Exception occurred when loading tickers: " + e.getMessage());
		}
		if (v != null) {
			BOCache.putTickersInCache(v);
			Ticker ticker = null;
			synchronized (tickerIds) {
				synchronized (tickerNames) {
					Iterator iter = v.iterator();
					while (iter.hasNext()) {
						ticker = (Ticker)iter.next();
						updateTickerMaps(ticker);
					}
				}
			}
    	}
		long after=System.currentTimeMillis();	
		logger.debug("Loaded PC type tickers " + tickerIds.size() +
			       " in " + ((double)(after-before))/1000. 
			       + " seconds.");
    }

	private void updateTickerMaps(Ticker ticker) {
		// Should be synchronized from the outside.
		if (validTickerFilter.matcher(ticker.getName()).matches()) {
			tickerIds.put(ticker.getId(), ticker);
			tickerNames.put(ticker.getName(), ticker);
		}
	}
	
	public Ticker getTickerByIdName(Integer tickerId,String tickerName)
    {
    	if(tickerId == null && tickerName == null)
    	{
    		return null;
    	}
    	
		logger.debug("getTicker() : reading tickerid = [" + tickerId + "] tickerName = [" + tickerName +"]");
		Ticker retVal = null;
		
		if(tickerId != null && tickerId.intValue() > 0)
		{
			retVal = getTicker(tickerId);
			logger.debug("processing via tickerid");
			if(retVal != null && tickerName != null && !tickerName.trim().equals(""))
			{
				if(!retVal.getName().equals(tickerName))
				{
					logger.debug("ticker id does not matched ticker name");
					// Provided name and id do not match to same ticker
					retVal = null;
				}
				else
				{
					logger.debug("ticker id matched ticker name");
					// This ticker matches both the id and name provided
				}
			}
		}
		else
		{
			logger.debug("processing via tickername");
			retVal = getTicker(tickerName);
		}
		return retVal;
    }
    
    public Ticker getTicker(ScctCDSType cds)
    {
    	if(cds == null)
    	{
    		return null;
    	}
    	
		Integer tickerId = cds.getTickerId();
		String tickerName = cds.getTickerName();
		logger.debug("getTicker() : reading CDS. tickerid = [" + tickerId + "] tickerName = [" + tickerName +"]");
		Ticker retVal = null;
		
		if(tickerId != null && tickerId.intValue() > 0)
		{
			retVal = getTicker(tickerId);
			logger.debug("processing via tickerid");
			if(retVal != null && tickerName != null && !tickerName.trim().equals(""))
			{
				if(!retVal.getName().equals(tickerName))
				{
					logger.debug("ticker id does not matched ticker name");
					// Provided name and id do not match to same ticker
					retVal = null;
				}
				else
				{
					logger.debug("ticker id matched ticker name");
					// This ticker matches both the id and name provided
				}
			}
		}
		else
		{
			logger.debug("processing via tickername");
			retVal = getTicker(tickerName);
		}
		return retVal;
    }
    
    public Ticker getTicker(int id) {
    		if (id == 0)
			return null;
		Ticker ticker = null;

		synchronized (tickerIds) {
			ticker = (Ticker) tickerIds.get(new Integer(id));
		}
		if (ticker != null)
			return ticker;
		try {
	    	ticker = BOCache.getTicker(dsConnection.getDSConnection(), id);
	    	if(ticker == null){
	    		ticker = dsConnection.getDSConnection().getRemoteProduct().getTicker(id);
	    	}
			if (ticker != null) {
				synchronized (tickerIds) {
					synchronized (tickerNames) {
						// FIXME: this function invoked from psevent handler.
						// ticker is removed from tickerId Collection, but not from tickerName handler?
						updateTickerMaps(ticker);
					}
				}
			} else {
				if (Log.isCategoryLogged("TickerCache"))
					Log.debug("TickerCache", "Ticker with id =>" + id + " not "
							+ "found in CacheClient and in DataServer",
							new Throwable());
			}
		} catch (Exception e) {
			Log.error(this, e);
		}
		return ticker;
	}
 
    public Hashtable getTickerNames() {
    	Hashtable retVal = null;
    	synchronized (tickerNames) {
    		retVal = (Hashtable) tickerNames.clone();
    	}
    	return retVal;
    }
    
    public Ticker getTicker(String name) {
	    if (name == null)
			return null;
		Ticker ticker = null;
	
		synchronized (tickerNames) {
			ticker = (Ticker) tickerNames.get(name);
		}
		if (ticker != null)
			return ticker;
		try {
	    	ticker = BOCache.getTicker(dsConnection.getDSConnection(), name);
	    	if(ticker == null){
	    		ticker = dsConnection.getDSConnection().getRemoteProduct().getTicker(name);
	    	}
			if (ticker != null) {
				synchronized (tickerNames) {
					synchronized (tickerIds) {
						tickerNames.put(name, ticker);
						tickerIds.put(new Integer(ticker.getId()), ticker);
					}
				}
			} else {
				if (Log.isCategoryLogged("TickerCache"))
					Log.debug("TickerCache", "Ticker with name =>" + name + " not "
							+ "found in CacheClient and in DataServer",
							new Throwable());
			}
		} catch (Exception e) {
			Log.error(this, e);
		}
		return ticker;
	}
    
    public Vector getABXDefs() throws Exception{
    	Product product  = null;
    	Vector v = new Vector();
    	ScctABXDefType sABX = null;
    	Iterator iter = abxIndexDefn.keySet().iterator();
    	while(iter.hasNext()){
    		product = (Product)abxIndexDefn.get((String)iter.next());
    		v.add(product);
    	}
    	return v;
    }
    
    
    public Vector getTickers() throws Exception{
		Ticker ticker = null;
		Vector v = new Vector();
		ScctTickerType sTicker = null;
    	synchronized (tickerIds) {
			Iterator iter = tickerIds.keySet().iterator();
			while (iter.hasNext()) {
				ticker = (Ticker)tickerIds.get((Integer)iter.next());
				sTicker = ConverterFactory.getInstance().getTickerConverter().convertFromCalypso(ticker);
				v.add(sTicker);
			}
    	}
    	return v;
	}
    
    public Hashtable getProductsById(){
    	return productsById;
    }
    
    public Hashtable getProductsWithId(){
    	return productsWithId;
    }
    
    public Hashtable getProductsABS(){
    	return productsABS;
    }
    
    public Hashtable getProductsABSSecs(){
    	return productsABSSecs;
    }

   
    public HashMap getCacheStats() {
		HashMap map = new HashMap();

		map.put("LegalEntityId", new Integer(legalEntityIds.size()));
		map.put("LegalEntityName", new Integer(legalEntityNames.size()));
		map.put("BOOKIDS", new Integer(bookIds.size()));
		map.put("BOOKNAMES", new Integer(bookNames.size()));
		map.put("TRADEFILTERS", new Integer(tradeFilters.size()));
		map.put("TICKERIDS", new Integer(tickerIds.size()));
		map.put("PORTFOLIOS", new Integer(portfolios.size()));		
		return map;
	}
    
    public Vector getCountries() throws Exception{
    	Vector v = BOCache.getCountries(dsConnection.getDSConnection());
    	Vector scctCountries = new Vector();
    	Country c = null;
    	ScctCountryType sc = null;
		if (v != null) {
			for (int i = 0; i < v.size(); i++) {
				c = (Country) v.elementAt(i);
				sc = ConverterFactory.getInstance().getCountryConverter().convertFromCalypso(c);
				// Convert name to upper case to be backward-compatible
				// with
				// domain data country names.
				scctCountries.add(sc);
			}
		}
		return scctCountries;
	}

    public ScctCountryType getCountry(String name) {
    	// We need to be backward-compatible with country domain data.
    	// Since EURO is not a country we'll use Belgium, where brussels
    	// is located.
    	name = name.toUpperCase();
    	if (name.equals("UK"))
    	    name = "UNITED KINGDOM";
    	else if (name.equals("USA"))
    	    name = "UNITED STATES";
    	else if (name.equals("EURO"))
    	    name = "BELGIUM";
    	Country c = BOCache.getCountry(dsConnection.getDSConnection(), name);
    	ScctCountryType sct = null;
    	try {
    		sct = ConverterFactory.getInstance().getCountryConverter().convertFromCalypso(c);
		}
		catch(ConverterException e) {
			Log.error(this, "Error converting Country to ScctCountry type: " + e);
		}
		return sct;
    	//return (ScctCountryType)_countries.get(name);
    }
    
    public Vector getPortfolios() throws Exception{
    	ScctPortfolioType sp = null;
		synchronized (portfolios) {
			if (portfolios.size() == 0) {
				try {
					RemoteCitiPortfolio rmCitiPortfolio = 
						(RemoteCitiPortfolio)dsConnection.getDSConnection().getRMIService(CitiPortfolioServer.NAME);

		    		//load all the portfolios
					Collection c = null; 
					c =	rmCitiPortfolio.getAllCdoPortfolios();
					Iterator iter = c.iterator();
					CitiPortfolioCDO cdo = null;
					while (iter.hasNext()) {
						cdo = (CitiPortfolioCDO)iter.next();
						sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(cdo);
						portfolios.put(sp.getPortfolioID(), sp);
					}
					logger.debug("Loaded " + c.size() + " CDO portfolios");
					
					c =	rmCitiPortfolio.getAllIndexPortfolios();
					iter = c.iterator();
					CitiPortfolioIndex index = null;
					while (iter.hasNext()) {
						index = (CitiPortfolioIndex)iter.next();
						sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(index);
						portfolios.put(sp.getPortfolioID(), sp);
					}
					logger.debug("Loaded " + c.size() + " Index portfolios");					
					c =	rmCitiPortfolio.getAllCdo2Portfolios();
					iter = c.iterator();
					CitiPortfolioCDO2 cdo2 = null;
					while (iter.hasNext()) {
						cdo2 = (CitiPortfolioCDO2)iter.next();
						sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(cdo2);
						portfolios.put(sp.getPortfolioID(), sp);
					}
					logger.debug("Loaded " + c.size() + " CDO2 portfolios");
				} catch (Exception ex) {
					Log.error(this, ex);
				}
			}
			Vector p = new Vector();
			Iterator iter = portfolios.keySet().iterator();
			while (iter.hasNext()) {
				p.addElement(portfolios.get(iter.next()));
			}
			return p;
		}
	}

    public ScctPortfolioType getPortfolio(long id) {
    	return (ScctPortfolioType)portfolios.get(new Long(id));
    }

    public void setPortfolio(long id, ScctPortfolioType portfolio) {
    	portfolios.put(new Long(id), portfolio);
    }
    
    public void loadPortfolio(long id, String type) {
    	if (id <= 0)
    		return;
    	if (!StringUtilities.isFilled(type))
    		return;
		synchronized (portfolios) {
			try {
				RemoteCitiPortfolio rmCitiPortfolio = 
					(RemoteCitiPortfolio)dsConnection.getDSConnection().getRMIService(CitiPortfolioServer.NAME);
				ScctPortfolioType sp = null;
				if (CitiPortfolioCDO.TYPE.equals(type)) {
					CitiPortfolioCDO cdo = rmCitiPortfolio.getCdoPortfolio(id);
					sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(cdo);
					setPortfolio(id, sp);
				}
				else if (CitiPortfolioCDO2.TYPE.equals(type)) {
					CitiPortfolioCDO2 cdo2 = rmCitiPortfolio.getCdo2Portfolio(id);
					sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(cdo2);
					setPortfolio(id, sp);
				}
				else if (CitiPortfolioIndex.TYPE.equals(type)) {
					CitiPortfolioIndex index = rmCitiPortfolio.getIndexPortfolio(id);
					sp = ConverterFactory.getInstance().getPortfolioConverter().convertFromCalypso(index);
					setPortfolio(id, sp);
				}
			}
			catch (Exception e) {
				logger.error("Error occurred loading portfolio: " + id + " : " + e);
			}
		}
    }
    
    public void loadDraftTypes() {
		/*this.draftTypes = new ArrayList();
		draftTypes.add("Blot");
		draftTypes.add("Callable");
		draftTypes.add("Pipeline");
		draftTypes.add("Risk");
		draftTypes.add("StdIndex");
		draftTypes.add("Testing");
		
    	domainValues.put(CalypsoAdapterCache.DRAFT_TYPES_KEY, draftTypes);*/
    	getDraftTypes();
    }

    public void loadTerminationTypes() {
    	/*this.terminationTypes = new ArrayList();
    	terminationTypes.add("Matured");
    	terminationTypes.add("Sold");
    	terminationTypes.add("Called");
    	terminationTypes.add("Converted");
    	domainValues.put(CalypsoAdapterCache.TERMINATION_TYPES_KEY, terminationTypes);*/
    	getTerminationTypes();
    }

    
    public  List getDraftTypes(){
    	//return this.getDomainValues(CalypsoAdapterCache.DRAFT_TYPES_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_DRAFT_TYPE);
    }

    public  List getTerminationTypes(){
    	//return this.getDomainValues(CalypsoAdapterCache.TERMINATION_TYPES_KEY);
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_TERMINATION_TYPE);
    }
    
    public void loadUserDefaults() {
    	logger.debug("loading user defaults ...");
		long before=System.currentTimeMillis();
    	try { 
    		userDefaults = 
    			dsConnection.getDSConnection().getRemoteAccess().getAllUserDefaults();
    	}
		catch (Exception ex) {
			logger.error("Error occurred loading user defaults: " + ex);
		}
		long after=System.currentTimeMillis();	
		logger.debug("Loaded all user defaults " + userDefaults.size() +
			       " in " + ((double)(after-before))/1000. 
			       + " seconds.");
    }
    
    public UserDefaults getUserDefaults(String userName) {
    	Object o = userDefaults.get(userName);
    	if (o != null)
    		return (UserDefaults)o;
    	else
    		return null;
    }
    
    public Hashtable getUserDefaults() {
    	return userDefaults;
    }

    public List getInitialMarginPercent(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.KEYWORD_INITIAL_MARGIN_PERCENT);
    }

    public List getInitialMarginRole(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.KEYWORD_INITIAL_MARGIN_ROLE);
    }

    public List getRTTRule(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.RTT_RULE);
    }

    public List getRTTFrequency(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.RTT_FREQUENCY);
    }
    
    public  List getPremLegFrequency(){
    	return Frequency.getDomain();
    	//return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_PREM_LEG_FREQ);
    }
    
	public List getPremiumStubRule() {
		Vector v = StubRule.getDomain();
		return Collections.unmodifiableList(ScctUtil.vectorToList(v));
	}
	
    public  List getCDOClassNames(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_INSTRUMENT_CLASS);
    }
    
    public Vector getCalcCities() {
		return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.CALC_CITIES);
    }
	
    public Hashtable getBondHashByCusip() {
    	return bondHashByCusip;
    }
    
    public Hashtable getBondHashByIsin() {
    	return bondHashByIsin;
    }
    
    public Hashtable getProductHashByCusip() {
    	return prodHashByCusip;
    }
    
    public Hashtable getProductHashByIsin(){
    	return prodHashByIsin;
    }
    
    public void loadTradeGroups() {
    	tradeGroups.clear();
        Vector values = LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_TRADE_GROUPS);
        ScctTradeGroupType sgroup = null;
        for(Iterator iter = values.iterator(); iter.hasNext();) {
        	String id = (String)iter.next();
            String desc = 
            	LocalCache.getDomainValueComment(dsConnection.getDSConnection(), DOMAIN_TRADE_GROUPS, id);
            sgroup = objectFactory.createScctTradeGroupType();
            sgroup.setId(new Integer(id));

            // expected format DESCRIPT:20070314:USD
            StringTokenizer st = new StringTokenizer(desc, ":");
            sgroup.setDescription(st.nextToken());
            if (st.hasMoreTokens()) {
            	sgroup.setMaturityDate(st.nextToken());
                if (st.hasMoreTokens())
                	sgroup.setCurrency(st.nextToken());
            }
            tradeGroups.put(sgroup.getId(), sgroup);
        }
    }

    public List getTradeGroups() {
    	ScctTradeGroupType[] groups = (ScctTradeGroupType[])tradeGroups.values().toArray(new ScctTradeGroupType[tradeGroups.size()]);
    	return Arrays.asList(groups);
    }
    
    protected Integer getNewTradeGroupId() {
   		Integer[] ids = (Integer[])tradeGroups.keySet().toArray(new Integer[tradeGroups.size()]);
   		return ((Integer)Collections.max(Arrays.asList(ids))) + 1;
    }

    protected void loadIndexTradeCaptureData() {
    	try {
    		logger.debug("loading index trade capture data...");
    		RemoteIssuerData ri = (RemoteIssuerData)dsConnection.getDSConnection().getRMIService("XtendedServer");
    		allIndexTradeCaptureData = (HashMap)ri.getAllIndexTradeCaptureData();
    	}
    	catch (Exception ex) {
    		logger.error("Error loading IndexTradeCaptureData cache: " + ex);
    	}
    }

    protected HashMap getAllIndexTradeCaptureData() {
    	return allIndexTradeCaptureData;
    }

    public IndexTradeCaptureData getIndexTradeCaptureData(String issuerCode) {
   		IndexTradeCaptureData itcd = null;
   		if(allIndexTradeCaptureData != null){
   			itcd  = (IndexTradeCaptureData)allIndexTradeCaptureData.get(issuerCode);
   		}
   		if(itcd == null){
   			String value = CalypsoAdapter.getAdapter().getProperty(CalypsoAdapterConstants.INDEX_TRADE_LOAD);
   			if(value != null && value.equalsIgnoreCase(CalypsoAdapterConstants.TRUE)){
	   			try{
	   				RemoteIssuerData ri = (RemoteIssuerData)dsConnection.getDSConnection().getRMIService("XtendedServer");
	   				if(ri != null){
		   				allIndexTradeCaptureData = (HashMap)ri.getAllIndexTradeCaptureData();
		   				if(allIndexTradeCaptureData != null){
		   					itcd = (IndexTradeCaptureData)allIndexTradeCaptureData.get(issuerCode);
		   				}
	   				}
	   			}
	   			catch(Exception e){
	   				logger.error(e);
	   			}
   			}
   		}
   		return itcd;
    }
    public  List getNonModifiableTradeStatus(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.NONMODIFIABLESTAUS);
    }

    public  List getNonModExternalSystem(){
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(), DomainValuesKey.NONMODEXTERNALSYSTEM);
    }

    public Vector getCitiBooksInvalidWithoutMirror() {
    	return LocalCache.getDomainValues(dsConnection.getDSConnection(),
    			DomainValuesKey.CITI_BOOK_IN_VALID_WITHOUT_MIRROR);
    }
    
    public List getForwardTypes() {
		Iterator iter = forwardTypes.keySet().iterator();
		Vector v = new Vector();
		while (iter.hasNext()) {
			ScctForwardTypeType type = (ScctForwardTypeType)forwardTypes.get((Integer)iter.next());
			v.add(type);
		}
		return Collections.unmodifiableList(ScctUtil.vectorToList(v));
    }

    public List getTerminationReason() {
    	return terminationReason;
    }
    
    public List getHedgeFundMnemonics() {
    	return hedgeFundMnemonics;
    }
    
    public Vector getSeniority() {
    	return seniority;
    }

    public Vector getTerminationPmt() {
    	return terminationPmt;
    }

    public void loadForwardTypes() {
        Vector values = LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_FORWARD_TYPE);
        for(Iterator iter = values.iterator(); iter.hasNext();) {
        	String id = (String)iter.next();
            String desc = 
            	LocalCache.getDomainValueComment(dsConnection.getDSConnection(), DOMAIN_FORWARD_TYPE, id);
            ScctForwardTypeType type = objectFactory.createScctForwardTypeType();
            type.setId(new Integer(id));
            type.setDescription(desc);
            forwardTypes.put(type.getId(), type);
        }
    }

    public void loadTerminationReasonTypes() {
        Vector values = LocalCache.getDomainValues(dsConnection.getDSConnection(), DOMAIN_TERMINATION_REASON);
        for (Iterator iter = values.iterator(); iter.hasNext();) {
			String value = (String) iter.next();
			terminationReason.add(value);
		}
    }

    public void loadHedgeFundMnemonics() {
		logger.debug("loading hedge fund entities ...");
		try {
			RemoteSTM STM = (RemoteSTM) dsConnection.getDSConnection()
					.getRMIService(RemoteSTM.SERVICE_NAME);
			LegalEntity le = null;
			ScctHedgeFundMnemonicType mnemonic;
			synchronized (hedgeFundMnemonics) {
				Hashtable tmp = STM.getLEData();
				if (tmp != null) {
					Set entries = tmp.entrySet();
					Iterator itr = entries.iterator();
					String key;
					StringBuffer buf;
					while (itr.hasNext()) {
						Map.Entry entry = (Map.Entry) itr.next();
						key = (String) entry.getKey();
						if (key != null) {
							buf = new StringBuffer();
							buf.append(key).append("_BO");
							le = (LegalEntity) legalEntityNames.get(buf
									.toString());
							if (le != null) {
								mnemonic = new ScctHedgeFundMnemonicType();
								mnemonic.setId(le.getId());
								mnemonic.setShortName(le.getCode());
								mnemonic.setLongName(le.getName());
								hedgeFundMnemonics.add(mnemonic);
								buf.delete(0, buf.length());
							}
						}
					}
				}
			}
		} catch (Exception e) {
			Log.error(this, e);
		}
	}
    
    public void loadTradeTemplates() {
      Iterator itr = null;
      long start = System.currentTimeMillis();
      
      loadProductTemplates(Product.CREDITDEFAULTSWAP, cdsTradeTemplates);
      loadProductTemplates(CitiCreditSyntheticCDO.PRODUCT_TYPE, cdoTradeTemplates);
      loadProductTemplates(CalypsoAdapterConstants.ASSET_BACKED_SWAP, cdsABSTradeTemplates);
      loadProductTemplates(CalypsoAdapterConstants.ABX, cdsABXTradeTemplates);
    }

    private void loadProductTemplates(String productType, Hashtable table) {
	    long start = System.currentTimeMillis();
		Vector tmp = new Vector();
		try {
		  tmp = dsConnection.getDSConnection().getRemoteProduct().getAllTemplateNames(productType);
		  Iterator itr = tmp.iterator();
		  while (itr.hasNext()) {
		    String name = (String) itr.next();
		    if (name !=null) {
		    	TemplateInfo templateInfo = dsConnection.getDSConnection().getRemoteProduct().getTemplateInfo(productType, name, null);
		    	// Skip private template
		    	if (templateInfo != null && !templateInfo.getIsPrivateB() && templateInfo.getProduct()!=null) {
		    		table.put(name, templateInfo);
		    	} else if (templateInfo!=null && templateInfo.getIsPrivateB()) {
		    		logger.debug("Private Template : [" + name + "]");
		    	}
		    }
		  }
		} catch (RemoteException e) {
			logger.warn("Unable to load " + productType + " templates");
		}
		long end = System.currentTimeMillis();
	    logger.debug(productType + " Templates Loading time = " + (end-start)/1000.0 + "\t" + " size = " +(table != null ? table.size() : 0 ));
    }
    
    private void loadBondContainer() {
	  long start = System.currentTimeMillis();
	  Vector results = loadBondProducts("Bond");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondFRN");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondDualCurrency");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondIndexLinked");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondIndexLinkedGilt");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondCorpus");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadBondProducts("BondAssetBacked");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP,productsABS,results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN,productsABSSecs,results);

	  results = loadBondProducts("BondConvertible");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);

	  results = loadProducts("CDSABSIndexDefinition");
	  mapProductBySecCode(CalypsoAdapterConstants.CUSIP, prodHashByCusip, results);
	  mapABXBySecCode(CalypsoAdapterConstants.CUSIP,abxIndexDefn,results);
	  mapProductBySecCode(CalypsoAdapterConstants.ISIN,prodHashByIsin,results);
	  mapABXBySecCode(CalypsoAdapterConstants.ISIN,abxDefByIsin,results);

/*	  results = loadBondProducts("XSPNote");
	  mapBondBySecCode(CalypsoAdapterConstants.CUSIP, bondHashByCusip, results);
	  mapBondBySecCode(CalypsoAdapterConstants.ISIN, bondHashByIsin, results);
*/	  
	  long end = System.currentTimeMillis();
	  logger.debug("Loading Bond Container ALL time : " + (end-start)/1000.0);
	}
	
	private void loadRatingAgencies() {
        Vector<String> agencies = LocalCache.getDomainValues(dsConnection.getDSConnection(), RATING_AGENCY);
		if (agencies != null) {
			for (String agency : agencies) {
				if (agency != null) {
					try {
						Vector values = dsConnection.getDSConnection()
								.getRemoteReferenceData()
								.getAgencyRatingValues(agency, "Current");
						if (values != null)
							ratingAgencies.put(agency, values);
					} catch (RemoteException e) {
						logger.warn("Unable to load Rating Agency ratings : "
								+ agency, e);
					}
				}
			}
		}        
	}

    private Vector loadBondProducts(String type) {
    	
	  long start = System.currentTimeMillis();
	  Vector<Bond> bonds = null;
	  try {
		bonds = dsConnection.getDSConnection().getRemoteProduct().getProducts(type, null);
	  } catch (RemoteException e) {
		e.printStackTrace();  
		logger.warn("Unable to load BOND : " + type + e.getMessage(), e);
	  }

	  long end = System.currentTimeMillis();

	  logger.debug("BOND container('" + type + "') cnt : " + (bonds != null ? bonds.size() : 0) + " time = " + (end-start)/1000.0);
    
	  return bonds;
    }
    
    private Vector<Product> loadProducts(String type) {
    	
  	  long start = System.currentTimeMillis();
  	  Vector<Product> products = null;
  	  try {
  		products = dsConnection.getDSConnection().getRemoteProduct().getProducts(type, null);
  	  } catch (RemoteException e) {
  		e.printStackTrace();  
  		logger.warn("Unable to load Product : " + type + e.getMessage(), e);
  	  }

  	  long end = System.currentTimeMillis();

  	  logger.debug("Product container('" + type + "') cnt : " + (products != null ? products.size() : 0) + " time = " + (end-start)/1000.0);
      
  	  return products;
    }
    


    private void mapBondBySecCode(String secCode, Hashtable table,
			Vector<Bond> bonds) {
		if (table != null && bonds != null) {
			for (Bond bond : bonds) {
				if (bond != null && bond.getSecCode(secCode) != null){
					table.put(bond.getSecCode(secCode), bond);
					if(bond.getSecCode(CalypsoAdapterConstants.CUSIP) != null){
						productsById.put(new Integer(bond.getSecurity().getId()),bond.getSecCode(CalypsoAdapterConstants.CUSIP));	
					}
					productsWithId.put(new Integer(bond.getSecurity().getId()),bond);
				}
			}
		}
	}

    private void mapProductBySecCode(String secCode, Hashtable table, Vector<Product> products) {
		if (table != null && products != null) {
			for (Product prod : products) {
				if (prod != null && prod.getSecCode(secCode) != null){
					table.put(prod.getSecCode(secCode), prod);
					if(prod.getSecCode(CalypsoAdapterConstants.CUSIP) != null){
						productsById.put(new Integer(prod.getId()),prod.getSecCode(CalypsoAdapterConstants.CUSIP));
					}
				}
			}
		}
	}
    
    private void mapABXBySecCode(String secCode, Hashtable table, Vector<Product> products) {
		if (table != null && products != null) {
			for (Product prod : products) {
				if (prod != null && prod.getSecCode(secCode) != null){
					table.put(prod.getSecCode(secCode), prod);
				}
			}
		}
	}


    private void loadSeniority() {
    	seniority = LocalCache.cloneDomainValues(DSConnection.getDefault(), "securityCode.DebtSeniority");
    }
    
    private void loadTerminationPmt() {
    	terminationPmt = LocalCache.getDomainValues(DSConnection.getDefault(), "terminationPmtType");
    }
    
    public void newEvent( PSEvent event)  {
		if(event instanceof PSEventAdmin) {
		    final PSEventAdmin ad=(PSEventAdmin)event;
		    if(ad.getType()==PSEventAdmin.CLEAR_CACHE) {
	
		    	//OCache.clear();		
		    }
		    LocalCache.newEvent(ad);
		}
		else if (event instanceof PSEventDomainChange) {
		    PSEventDomainChange evdem=(PSEventDomainChange)event;
			BOCache.newEvent(dsConnection.getDSConnection(), evdem);
			handleDomainChangeEvent(evdem);
		}
		else if (event instanceof PSEventCitiPortfolio) {
			PSEventCitiPortfolio evport = (PSEventCitiPortfolio)event;
			logger.debug("Processing citi portfolio event " + evport.getEventType() + 
					" for id: " + evport.getPortfolioId());
			synchronized(portfolios) {
				long id = evport.getPortfolioId();
				portfolios.remove(new Long(id));
				if (evport.getAction() == PSEventCitiPortfolio.SAVE_ACTION)
					loadPortfolio(id, evport.getPortfolioType());
			}
		}
	 }

	 public void handleDomainChangeEvent(PSEventDomainChange event ){
		switch(event.getType()) {
		case PSEventDomainChange.LEGAL_ENTITY:
			logger.info("Processing event: " + event.toString() + " Event Id = " + event.getValueId());			
		    LegalEntity le=null;
		    synchronized(legalEntityIds) {
				 le = (LegalEntity)legalEntityIds.get(new Integer(event.getValueId()));
				 logger.info("Updating le '" + (le!=null ? le.getName() : null) + "'");			
				 if (le != null) {
				     legalEntityIds.remove(new Integer(event.getValueId()));
				     legalEntityNames.remove(le.getCode());
				 }
				// reload into cache and set le on book
				if (event.getAction() != PSEventDomainChange.REMOVE) { 
					le = getLegalEntity(event.getValueId());
					if (le != null) {
					    synchronized(bookIds) {
							Iterator iter = bookIds.keySet().iterator();
							while(iter.hasNext()) {
							    Book b = (Book)bookIds.get((Integer)iter.next());
							    if(b.getLegalEntity() != null &&
							       b.getLegalEntity().getId()== le.getId()) {
							    	b.setLegalEntity(le);
							    	Book bn = (Book)bookNames.get(new Integer(b.getId()));
							    	bn.setLegalEntity(le);
							    }
							}
					   }
					}
				}
		    }
			try {
				CalypsoAdapter.getAdapter().handleLegalEntityEvent(le, getEventAction(event.getAction()));
			}
			catch (Exception ex) {
				logger.error("Exception processing legal entity event: " + ex);
			}

		    break;
		case PSEventDomainChange.CURRENCY_DEFAULT:
		   // refreshCurrencyUtil(ds);
		    break;
		case PSEventDomainChange.FEE_DEFINITION:
			feeDefns.clear();
			boFees.clear();
			feeRoles.clear();
			loadFeeDefinitions();
			break;
		case PSEventDomainChange.MIME_TYPE:
		  //  refreshMimeTypes(ds);
		    break;
		case PSEventDomainChange.HOLIDAY:
			loadHolidayCalendars();
		 //   refreshHolidays(ds,event);
	     //       ResetRateCalcUtil.clearCache();
		    break;
		case PSEventDomainChange.BOOK:
			synchronized(bookIds) {
				Integer key = new Integer(event.getValueId());
				Book book = (Book)bookIds.get(key);
				logger.info("Processing event: " + event.toString());				
				if (book != null) {
				    bookIds.remove(key);
				    bookNames.remove(book.getName());
				}
				// reload into cache
				if (event.getAction() != PSEventDomainChange.REMOVE) 
					getBook(event.getValueId());
				
				try {
					CalypsoAdapter.getAdapter().handleBookEvent(book, getEventAction(event.getAction()));
				}
				catch (Exception ex) {
					logger.error("Exception processing book event: " + ex);
				}
			}
		    break;
		case PSEventDomainChange.PORTFOLIO:
		    break;
		case PSEventDomainChange.DOMAIN_VALUE:
		    break;	 	
		// from BOCache
	 	case PSEventDomainChange.USER_PROP:
	   // if(event.getValue() != null) {
	//	_userProperties.remove(event.getValue());
	//	_userPropertiesNotFound.remove(event.getValue());
	  //  }
	    break;
		case PSEventDomainChange.USER_DEFAULTS:
			synchronized (userDefaults) {
				String username = event.getValue();
				String eventType = event.getEventType().substring(0,event.getEventType().indexOf("_"));
				logger.debug("USER_DEFAULTS EVENT : " + username + "/" + event.getEventType());
				if (username != null && ("NEW".equals(eventType) || "MODIFY".equals(eventType))) {
					try {
						UserDefaults updatedDefaults = dsConnection.getDSConnection().getRemoteAccess().getUserDefaults(username);
						userDefaults.remove(username);
						if (updatedDefaults != null) {
							userDefaults.put(username, updatedDefaults);
						}
						logger.debug("NEW/UPDATED USER_DEFAULTS " + updatedDefaults.getUserType() + "/" + updatedDefaults.getTraderName() + "/" + updatedDefaults.getSalesPerson() + "/" +updatedDefaults.getTimeZone());
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						logger.warn("Caught Exception refreshing " + username + " user defaults cache", e);
					}
				} else if ("REMOVE".equals(eventType)) {
					userDefaults.remove(username);
					logger.debug("REMOVED USER_DEFAULTS " + username);
				}
			}

			break;
		case PSEventDomainChange.CFD_COUNTRY_GRID:
/*
			CFDCountryGrid cg = (CFDCountryGrid)_cfdCountryGridIds.
			get(new Integer(event.getValueId()));
		    if (cg != null){
			synchronized(_cfdCountryGridIds) {
			    _cfdCountryGridIds.remove(new Integer(event.getValueId()));
			}
		    }
		    synchronized (_cfdCountryGrids) {
			_cfdCountryGrids.clear();
		    }
	*/
		    	    break;
		case PSEventDomainChange.CFD_CONTRACT:
		/*
			CFDContractDefinition cfd = (CFDContractDefinition)_cfdContractIds.
			get(new Integer(event.getValueId()));
		    if (cfd != null){
			synchronized(_cfdContractIds) {
			    _cfdContractIds.remove(new Integer(event.getValueId()));
			}
			synchronized(_cfdContractPOIds) {
			    _cfdContractPOIds.
				remove(new Integer(cfd.getProcessingOrgId()));
			}
		    } else {
			try {
			    cfd = ds.getRemoteProduct().
				getCFDContract(event.getValueId());
			}catch (Exception e) {Log.error(this, e);}
			if (cfd != null) {
			    synchronized(_cfdContractPOIds) {
				_cfdContractPOIds.
				    remove(new Integer(cfd.getProcessingOrgId()));
			    }
			}
		    }
		    */
		    break;
		case PSEventDomainChange.STATIC_DATA_FILTER :
		    {
			String name = event.getValue();
//			synchronized (_staticDataFilters) {
	//		    _staticDataFilters.remove(name);
		//	}
		   }
		    break;
		case PSEventDomainChange.CACHE_LIMITS:
		 //   _limits = null;
		    break;
		case PSEventDomainChange.FILTER_SET :
		    {
			String name = event.getValue();
	//		synchronized (_filterSets) {
		//	    _filterSets.remove(name);
			//}
		    }
		    break;
		case PSEventDomainChange.WORKFLOW:
/*
			synchronized (_tradeHashLookup) {
		        _tradeHashLookup.clear();
		    }
		    synchronized (_messageHashLookup) {
		        _messageHashLookup.clear();
		    }
		    synchronized (_paymentHashLookup) {
		        _paymentHashLookup.clear();
		    }
		    synchronized (_workflowHashLookup) {
		        _workflowHashLookup.clear();
		    }
		    synchronized (_tradeHashStatus) {
		        _tradeHashStatus.clear();
		    }
		    synchronized (_messageHashStatus) {
		        _messageHashStatus.clear();
		    }
		    synchronized (_paymentHashStatus) {
		        _paymentHashStatus.clear();
		    }
		    synchronized (_workflowHashStatus) {
		        _workflowHashStatus.clear();
		    }
		    synchronized (_eventClassWorkflows) {
			_eventClassWorkflows.clear();
		    }
		    synchronized(_matchingContextByName) {
			_matchingContextByName.clear();
		    }
		    synchronized(_matchingContextByMsgType) {
			_matchingContextByMsgType.clear();
			_notfoundMatchingContextByMsgType.clear();
		    }
	*/
		    	    break;
		case PSEventDomainChange.KICKOFF_CUTOFF:
	//	    synchronized(_kickoffLock) {
	//		_kickoff=null;
	//		_kickoffIds=null;
	//	    }
		    break;
		case PSEventDomainChange.LE_ATTRIBUTE:
	//	    synchronized (_leAttributes) {
	//		_leAttributes.clear();
	//	    }
		    break;
		case PSEventDomainChange.BOOK_VAL_CCY:
	//	    synchronized(_bookValCcyLock) {_bookValCcy = null;}
		    break;
		case PSEventDomainChange.TICKER:
			synchronized(tickerIds) {
				tickerIds.remove(new Integer(event.getValueId()));
				getTicker(event.getValueId());
		    }
		    break;
	    case PSEventDomainChange.BONDBENCHMARK:
	    	/*
	        String key = event.getValue();
	        synchronized(_bondBenchmarks) {
	            _bondBenchmarks.remove(key);
	            if (Log.isCategoryLogged("TickerCache"))
	                Log.debug("BondBenchmarkCache","BondBenchmark with key "+
	                          key+ " removed from CacheClient"
	                          , new Throwable());
	        }
	        */
	        break;
	    case PSEventDomainChange.TEMPLATE_PRODUCT:
			String eventType = event.getEventType().substring(0,event.getEventType().indexOf("_"));
			// tmp string 'val=CreditDefaultSwap|zzz abs buy protection 2|calypso_ny'
			String tmp = event.getValue().replace('|',':');
			String[] tokens = tmp.split(":");
			String product = tokens[0];
			String templateName = tokens[1];
			logger.debug(PSEventDomainChange.TEMPLATE_PRODUCT + " event type: " + event.getEventType() + "/ val=" + event.getValue());
			if ("MODIFY".equals(eventType)) {
				if (templateName !=null && product !=null) {
					if (Product.CREDITDEFAULTSWAP.equals(product)) {
					  updateTemplateCache(product, templateName, cdsTradeTemplates);
					} else if (CitiCreditSyntheticCDO.PRODUCT_TYPE.equals(product)) {
					  updateTemplateCache(product, templateName, cdoTradeTemplates);
					} else if (CalypsoAdapterConstants.ASSET_BACKED_SWAP.equals(product)) {
					  updateTemplateCache(product, templateName, cdsABSTradeTemplates);
					} else if (CalypsoAdapterConstants.ABX.equals(product)) {
					  updateTemplateCache(product, templateName, cdsABXTradeTemplates);
					} else {
					  logger.warn("Unsupported Product '" + product + "'");
					}
				}
			} else if ("REMOVE".equals(eventType)) {
				if (templateName !=null && cdsTradeTemplates.get(templateName) !=null) {
				  synchronized(cdsTradeTemplates) {
				    cdsTradeTemplates.remove(templateName);
				  }
				} else if (templateName !=null && cdoTradeTemplates.get(templateName) !=null) {
				  synchronized(cdoTradeTemplates) {
					cdoTradeTemplates.remove(templateName);
				  }
				} else if (templateName !=null && cdsABSTradeTemplates.get(templateName)!=null) {
				  synchronized(cdsABSTradeTemplates) {
					cdsABSTradeTemplates.remove(templateName);
				  }
				} else if (templateName !=null && cdsABXTradeTemplates.get(templateName)!=null) {
				  synchronized(cdsABXTradeTemplates) {
					cdsABXTradeTemplates.remove(templateName);
				  }
				}
			}
			break;
			case PSEventDomainChange.RATING_VALUE:
				logger.debug(PSEventDomainChange.RATING_VALUE + " event type: " + event.getEventType() + "/ val=" + event.getValue());
				synchronized(ratingAgencies) {
					ratingAgencies.clear();
					loadRatingAgencies();
				}
			break;
		}
	 }
	 
	private void updateTemplateCache(String product, String templateName, Hashtable table) {
	  TemplateInfo template = null;
	    try {
		  template = dsConnection.getDSConnection().getRemoteProduct().getTemplateInfo(product, templateName, null);
		  if (template != null) {
			if (table.get(templateName) !=null) {
				table.remove(templateName);
				table.put(templateName, template);
				logger.warn("Updating " + product + " template cache : '" + templateName + "'");
			  } else {
				table.put(templateName, template);
				logger.warn("Inserting " + product + " template cache : '" + templateName + "'");
			  }
		  }
		} catch (RemoteException e) {
		  logger.warn("Unable to update " + product + " template cache : '" + templateName + "'");
		}
	}

	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#connectionClosed(com.calypso.tk.service.DSConnection)
	 */
	public void connectionClosed(DSConnection arg0) {
		// TODO Auto-generated method stub

	}
	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#reconnected(com.calypso.tk.service.DSConnection)
	 */
	public void reconnected(DSConnection arg0) {
		// TODO Auto-generated method stub

	}
	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#tryingBackup(com.calypso.tk.service.DSConnection)
	 */
	public void tryingBackup(DSConnection arg0) {
		// TODO Auto-generated method stub

	}
	/* (non-Javadoc)
	 * @see com.calypso.tk.service.ConnectionListener#usingBackup(com.calypso.tk.service.DSConnection)
	 */
	public void usingBackup(DSConnection arg0) {
		// TODO Auto-generated method stub

	}
	
	/* (non-Javadoc)
	 * @see com.calypso.tk.event.PSSubscriber#onDisconnect()
	 */
	public void onDisconnect() {
		// TODO Auto-generated method stub
		logger.debug("Disconnected from Event Server");
		internalPSStop();
		startPSTimer();
	}

	 protected void stopPSTimer() {
		if (psTimer == null)
			return;
		logger.debug("Stopping Timer to reconnect to EventServer");
		com.calypso.tk.util.Timer timer = psTimer;
		psTimer = null;
		timer.stop();
	}
	
	 protected int getTimeoutReconnect() {
		return dsConnection.getDSConnection().getTimeoutReconnect();
	}
	  
	 protected void startPSTimer() {
		if (psTimer != null)
			return;
		logger.debug("Starting Reconnect Timer");
		TimerRunnable r = new TimerRunnable() {
			public void timerRun() {
				if (dsConnection.getDSConnection() == null || dsConnection.getDSConnection().isClosed()) {
					logger.debug("PSTimer Not connected to DS skip try");
					return;
				}
				logger.debug( "PSTimer trying reconnect to ES");
				internalPSStop();
				boolean v = connectToPSServer(false);
				if (v) {
					logger.debug( "PSTimer Reconnected to ES");
					stopPSTimer();
				}
			}
		};
		psTimer = new com.calypso.tk.util.Timer(r, getTimeoutReconnect());
		psTimer.start();
	}
	 protected void internalPSStop() {
		if (ps != null){
			try {
				ps.stop();
			} catch (Exception e) {
			}
		}
	}

	boolean connectToPSServer(boolean wait) {
		try {
			ps = ESStarter.startConnection(dsConnection.getDSConnection(), this);
			if (ps == null)
				return false;
			ps.start(wait);
			subscribe();
			return true;
		} catch (Exception e) {
			Log.error(this, e);
			return false;
		}
	}
	
	public void subscribe() throws Exception {
		Vector events = new Vector();
	    events.addElement(PSEventDomainChange.class.getName());
	    events.addElement(PSEventAdmin.class.getName());
	    ps.subscribe(events);	
	}
	
	private String getEventAction(int action) {
		if (action == PSEventDomainChange.REMOVE)
			return ScctConstants.EVENT_ACTION_REMOVE;
		else if (action == PSEventDomainChange.MODIFY)
			return ScctConstants.EVENT_ACTION_MODIFY;
		else if (action == PSEventDomainChange.NEW)
			return ScctConstants.EVENT_ACTION_NEW;
		else
			logger.error("Unknown action: " + action);
		
		return null;
	}

    public void loadRatings() {
    	logger.debug("### loading tickers ratings ###");
    	long start = System.currentTimeMillis();
    	Ticker ticker = null;
		int resCnt = 0;
    	synchronized (tickerIds) {
    		Hashtable industry = loadIndustryAttributes();
    		Hashtable creditRating = loadLatestCreditRatings();
			Iterator iter = tickerIds.keySet().iterator();
			while (iter.hasNext()) {
				ticker = (Ticker) tickerIds.get((Integer) iter.next());
				try {
					int leId = ticker.getIssuerId();
					LegalEntity legalEntity = (LegalEntity) legalEntityIds.get(leId);
					if (leId > 0 && legalEntity != null) {
						String industryAttr = "", ratingValue = "";
						Integer leId2 = new Integer(leId);
						
						if (industry.get(leId2) != null) {
							industryAttr = ((LegalEntityAttribute) industry.get(leId2)).getAttributeValue();
						}
						
						if (creditRating.get(leId2) != null) {
							ratingValue = (String) creditRating.get(leId2);
						}
						

						IssuerAttribute issuerAttr = new IssuerAttribute();
						issuerAttr.setDescription(legalEntity.getName());
						issuerAttr.setIndustry(industryAttr);
						issuerAttr.setRatings(ratingValue);
						resCnt++;
						ratings.put(new Integer(ticker.getId()), issuerAttr);
					}					
				} catch (RuntimeException rex) {
					logger.error("Caught Exception Legal Entity ="
							+ ticker.getIssuerId(), rex);
				}
			}
    	}
    	long end = System.currentTimeMillis();
		logger.debug("### Finished Loading ticker ratings total=" + ratings.size() + " good=" + resCnt + 
			       " in " + ((double)(end-start))/1000 
			       + " seconds. ### ");
    }

	public IssuerAttribute getIssuerAttribute(Integer tickerId) {
		IssuerAttribute issuerAttr = null;
		synchronized(ratings) {
			issuerAttr = (IssuerAttribute) ratings.get(tickerId);
		}
		return issuerAttr;
	}
	
	public String getLEValue(Integer leId,String attributeType){
		String value = null;
		Vector v = null;
		if(leId != null){
			try{
				v = dsConnection.getDSConnection().getRemoteReferenceData().
					getAttributes(leId);
			}
			catch(Exception e){
				logger.error(e);
			}
		}
		if(v != null){
			Iterator i = v.iterator();
			while(i.hasNext()){
				LegalEntityAttribute lea = (LegalEntityAttribute)i.next();
				if(lea.getAttributeType().equalsIgnoreCase(attributeType)){
					value = lea.getAttributeValue();
					break;
				}
			}
		}
		return value;
	}
	
	private Hashtable loadIndustryAttributes() {
    	logger.debug("### loading industry attributes ###");
    	long start = System.currentTimeMillis();
    	Hashtable leAttributes = new Hashtable();
    	List<LegalEntityAttribute> attributes = null;
		try {
			String whereClause = new String(" processing_org_id=0 and legal_entity_role='Issuer' and attribute_type='INDUSTRY' ");
			attributes = ScctUtil.vectorToList(dsConnection.getDSConnection().getRemoteReferenceData().getLegalEntityAttributes(null, whereClause));
			if (attributes != null) {
				for (LegalEntityAttribute attr : attributes) {
					if (attr != null) {
						leAttributes.put(attr.getLegalEntityId(), attr);
					}
				}
			}
		} catch (RemoteException r) {
			logger.debug("Unable to retrieve Industry Attributes");
		} 
    	long end = System.currentTimeMillis();
		logger.debug("### Finished Loading industry attribute total=" + attributes.size() + " in " + ((double)(end-start))/1000 
			       + " seconds. ### ");
	
		return leAttributes;
	}
	
	public String getDomainValue(String cType,String cValue){
		String value = null;
		try{
			value = LocalCache.getDomainValueComment(dsConnection.getDSConnection(),cType,cValue);
			if(value == null){
				DomainValues v = dsConnection.getDSConnection().getRemoteReferenceData().getDomains();
				if(v != null){
					value = v.getDomainValueComment(cType,cValue);
				}
			}
		}
		catch(Exception e){
			logger.error(e);
		}
		return value;
	}
	
	public List getDomainValuesForType(String type){
		List values = null;
		try{
			values = LocalCache.getDomainValues(dsConnection.getDSConnection(),type);
			if(values == null){
				dsConnection.getDSConnection().getRemoteReferenceData().getDomainValues(type);
			}
		}
		catch(Exception e){
			logger.error(e);
		}
		return values;
	}
	
	private Hashtable loadLatestCreditRatings() {
    	logger.debug("### loading credit ratings ###");
    	long start = System.currentTimeMillis();
		int cnt = 0;
		Hashtable ratingsAttribute = new Hashtable();

		Set<Integer> leIds = legalEntityIds.keySet();
		StringBuffer leBuffer = new StringBuffer();
		for (Integer legalEntityId : leIds) {
			leBuffer.append(legalEntityId.intValue()).append(",");
			cnt++;
			if ((cnt % 255) == 0) {
				ratingsAttribute.putAll(populateRatings(leBuffer));
			}
		}
		ratingsAttribute.putAll(populateRatings(leBuffer));
    	long end = System.currentTimeMillis();
		logger.debug("### Finished Loading credit ratings=" + ratingsAttribute.size() + " in " + ((double)(end-start))/1000 
			       + " seconds. ### ");
		
		return ratingsAttribute;
	}

	private Hashtable populateRatings(StringBuffer leBuffer) {
    	Hashtable ratingsAttribute = new Hashtable();
		if (StringUtilities.isFilled(leBuffer.toString())) {
			leBuffer.replace(leBuffer.toString().length() - 1, leBuffer
					.toString().length(), "");
			String leList = leBuffer.toString();
			leBuffer.delete(0, leBuffer.length());
			try {
				StringBuffer from = new StringBuffer();
				StringBuffer where = new StringBuffer();

				where.append(" credit_rating.legal_entity_id in (").append(
						leList).append(")");
				where.append(" AND credit_rating.legal_entity_id <> 0");
				where.append(" AND rating_type='Current'");
				where.append(" AND debt_seniority='SENIOR_UNSECURED'");
				where
						.append(" AND credit_rating.as_of_date in (select max(c1.as_of_date) from credit_rating c1 where c1.legal_entity_id = credit_rating.legal_entity_id and c1.rating_type = credit_rating.rating_type and c1.debt_seniority = credit_rating.debt_seniority)");
				where.append(" ORDER BY credit_rating.rating_agency_name");

				Vector<CreditRating> v = dsConnection.getDSConnection()
						.getRemoteMarketData().getRatings(null,
								where.toString());
				if (v != null) {
					for (CreditRating rating : v) {
						Integer leId = Integer.valueOf(rating
								.getLegalEntityId());

						if (!ratingsAttribute.containsKey(leId)
								&& ratingsAttribute.get(leId) == null) {
							StringBuffer buf2 = new StringBuffer();
							buf2.append(rating.getRatingValue()).append(" (")
									.append(rating.getAgencyName()).append(
											"), ");
							ratingsAttribute.put(leId, buf2.toString());
						} else if (ratingsAttribute.containsKey(leId)
								&& ratingsAttribute.get(leId) != null) {
							StringBuffer buf2 = new StringBuffer();
							buf2.append(ratingsAttribute.get(leId));
							buf2.append(rating.getRatingValue()).append(" (")
									.append(rating.getAgencyName()).append(
											"), ");
							ratingsAttribute.remove(leId);
							ratingsAttribute.put(leId, buf2.toString());
						}
					}
				}
			} catch (RemoteException rex) {
				logger.error("Error retrieving credit ratings", rex);
			} catch (RuntimeException e) {
				logger.error(e);
			}
		}		
		return ratingsAttribute;
	}
}
