/*
* Author: ps64022
*
*/

package com.citigroup.project.dps;

import java.util.List;
import com.calypso.tk.core.*;

/**
*	This interface provides a generic trade XML Data generator <br>
*	to send trades entered in Calypso to BO Systems DPS/OASYS <br>
*
*/

public interface IDPSDataObject {

	public void init(List args);
	public void set(Trade trade, Object o);
	public String generateXML();
	public String key();
}
