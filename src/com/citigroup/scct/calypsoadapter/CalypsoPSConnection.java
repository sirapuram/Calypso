/*
 * Created on Oct 27, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.citigroup.scct.calypsoadapter;

import java.util.*;
import org.apache.log4j.*;
import java.rmi.RemoteException;

import com.calypso.tk.mo.TradeFilter;
import com.calypso.tk.util.ConnectionUtil;
import com.calypso.apps.startup.AppStarter;
import com.calypso.tk.core.*;
import com.calypso.tk.service.*;
import com.calypso.tk.event.ESStarter;
import com.calypso.tk.event.PSConnection;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.util.TradeArray;
import com.calypso.tk.event.*;
import com.calypso.tk.bo.*;
import com.calypso.tk.refdata.AccessUtil; // to get access permissions
import com.calypso.tk.refdata.UserAccessPermission;


/**
 * @author mi54678
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class CalypsoPSConnection implements PSSubscriber , ConnectionListener {
	protected CalypsoDSConnection calypsoDSConnection;
	protected DSConnection ds;
	protected PSConnection ps;
	protected Vector listeners;
	
	public CalypsoPSConnection( CalypsoDSConnection _dsConnection )
	{
		calypsoDSConnection = _dsConnection;
		ds= calypsoDSConnection.getDSConnection();
	}
	
	public void start() throws AdapterException{
		try {
		    ps = ESStarter.startConnection(ds,  this);
			ps.start();
		} catch (Exception ex ){
			System.out.println("Could not start PS connection ");
			throw new AdapterException(ex);			
		}
	}
	
	public void addListener(ConnectionListener l){
		listeners.add(l);
	}
	
	/* (non-Javadoc)
	 * @see com.calypso.tk.event.PSSubscriber#newEvent(com.calypso.tk.event.PSEvent)
	 */
	public void newEvent(PSEvent arg0) {
		// TODO Auto-generated method stub

	}
	/* (non-Javadoc)
	 * @see com.calypso.tk.event.PSSubscriber#onDisconnect()
	 */
	public void onDisconnect() {
		// TODO Auto-generated method stub

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

	/**
	 * @return Returns the ps.
	 */
	public PSConnection getPs() {
		return ps;
	}
	/**
	 * @param ps The ps to set.
	 */
	public void setPs(PSConnection ps) {
		this.ps = ps;
	}
}
