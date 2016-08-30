package calypsox.tk.event.sql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import calypsox.tk.event.PSEventCitiPortfolio;

import com.calypso.tk.core.Log;
import com.calypso.tk.core.PersistenceException;
import com.calypso.tk.core.sql.CacheStatement;
import com.calypso.tk.core.sql.JResultSet;
import com.calypso.tk.core.sql.ioSQL;
import com.calypso.tk.event.PSEvent;
import com.calypso.tk.event.sql.PSEventSQL;

/**
 * The DAO for the CitiPortfolio events
 */
public class PSEventCitiPortfolioSQL extends PSEventSQL {

	/**
	 * Save the event
	 * 
	 * @return boolean true if successful, false otherwise
	 */
	public boolean saveInstance(String className,
                                PSEvent event,
                                Connection dbConn)
        throws PersistenceException {
 
		PSEventCitiPortfolio portfolioEvent = (PSEventCitiPortfolio)event;
		boolean status = true;

        PreparedStatement statement = null; 
        try {
        	StringBuffer sql = new StringBuffer();
        	sql.append("INSERT INTO event_citi_portfolio (");
        	sql.append("event_id, ");
        	sql.append("action, ");
        	sql.append("portfolio_id, ");
        	sql.append("portfolio_type ");
        	sql.append(") VALUES (");
        	sql.append("?, ");
        	sql.append("?, ");
        	sql.append("?, ");
        	sql.append("?");
        	sql.append(")");

            statement = CacheStatement.getPrepared(dbConn, sql.toString());
		    statement.setInt(1, portfolioEvent.getId());
		    statement.setString(2, portfolioEvent.getAction());
		    statement.setBigDecimal(3, BigDecimal.valueOf(portfolioEvent.getPortfolioId()));
		    statement.setString(4, portfolioEvent.getPortfolioType());
            statement.executeUpdate();
            
        } catch (SQLException ex) {
        	StringBuffer msg = new StringBuffer();
        	msg.append("Could not save event[").append(event.toString()).append("]");
        	Log.error(msg.toString(), ex);
		    status = false;
		    ioSQL.display(ex);
		    throw new PersistenceException(ex);
        } finally {
            CacheStatement.release(statement);
        }
        
        return status;
    }


    /**
     * Returns a vector of serialized events sorted by event id.  Restricts the
     * events returned based on the passed where clause and the passed maximum
     * number to return (which only applies if the argument is greater than
     * zero).
     * 
     * @return Vector A collection of serialized PSEventCitiIndexTranche
     */
    public Vector loadInstances(String where,
                                int maxCount, 
                                Connection dbConn)
        throws PersistenceException {

        Vector events = new Vector();
        Statement statement = null;
    	StringBuffer sql = new StringBuffer();

        try {
        	sql.append("SELECT ");
        	sql.append("cp.event_id, ");
        	sql.append("cp.action, ");
        	sql.append("cp.portfolio_id, ");
        	sql.append("cp.portfolio_type ");
        	sql.append(" FROM ");
        	sql.append("ps_event, ");
        	sql.append("event_citi_portfolio cp ");
        	sql.append(" WHERE ");
        	sql.append("ps_event.event_id = cp.event_id ");
        	sql.append(" AND ");
        	sql.append(where);
        	
            statement = newStatement(dbConn);
            statement.setMaxRows(maxCount);

            JResultSet rs = executeQueryDeadLock(statement, sql.toString());

            int count = 0;
            while (rs.next()) {
                count++;
                int j=1;

                PSEventCitiPortfolio event = new PSEventCitiPortfolio();
                event.setId(rs.getInt(j++));
                event.setAction(rs.getString(j++));
                event.setPortfolioId(rs.getLong(j++));
                event.setPortfolioType(rs.getString(j++));

                events.addElement(event);

                // Restrict events based on the passed maximum number to return.
                if ((maxCount > 0) && (count == maxCount)) {
                    break;
                }
            }
            rs.close();
        } catch(Exception ex ) {
        	StringBuffer msg = new StringBuffer();
        	msg.append("Could not load events[").append(sql).append("]");
        	Log.error(msg.toString(), ex);
            display(ex);
            throw new PersistenceException(ex);
        } finally {
            close(statement);
        }

        events = sortEvents(events);

        return events;
    }
	
    
    /**
     * Deletes a range of event instances that fall between the passed minimum
     * and maximum event id's.  
     * 
     * @return boolean true if the delete was successful, false otherwise
     */
    public boolean deleteInstances(String className,
                                   int min,
                                   int max,
                                   Connection dbConn)
        throws PersistenceException {

        boolean status = true;
        Statement statement = null;
        StringBuffer sql = new StringBuffer();
        try {
            statement =  newStatement(dbConn);
            sql.append("DELETE FROM event_citi_portfolio ");
            sql.append("WHERE ");
            sql.append("event_id >= ").append(min);
            sql.append(" AND ");
            sql.append("event_id <= ").append(max);

            statement.executeUpdate(sql.toString());
            
        } catch(Exception e) {
        	StringBuffer msg = new StringBuffer();
        	msg.append("Could not load events[").append(sql).append("]");
        	Log.error(msg.toString(), e);
            status = false;
            display(e);
            throw new PersistenceException(e);
        } finally {
            close(statement);
        }
        
        return status;
    }
    
    /**
     * Deletes a set of events restricted by the passed where clause.  Returns
     * the number of events deleted.  Used by the event server and data server
     * to purge events that have already been consumed by all subscribers.
     */
    public int purgeInstances(String className,
                              Connection connection,
                              String where)
        throws PersistenceException {

        Statement statement = null;
        int count = 0;
        try {
            statement = newStatement(connection);
            StringBuffer sql = new StringBuffer();
            sql.append("DELETE FROM event_citi_portfolio ");
            sql.append("WHERE ").append(where); 

            count = statement.executeUpdate(sql.toString());
        } catch (Exception ex) {
            throw new PersistenceException(ex);
        } finally {
            close(statement);
        }
        
        return count;
    }
    
    /** 
	 * Saves a set of event instances.  Returns true if all saves were successful,
	 * false otherwise.
	 */
	public boolean saveInstancesBatch(String className, Vector events,
			Connection connection) throws PersistenceException {

		boolean status = true;
		for (int i = 0; i < events.size(); i++)
			status = status
					&& saveInstance(className, (PSEvent) events.elementAt(i),
							connection);

		return status;
	}
    
    
}
