package simpledb.query;

import java.util.Iterator;
import java.util.NoSuchElementException;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.record.*;

/** The Plan class corresponding to a table.
  * @author Edward Sciore, Philip Howard
  */
public class TablePlan implements Plan {
   private Transaction tx;
   private TableInfo ti;
   private StatInfo si;
   private String tblname;
   
   /**
    * Creates a leaf node in the query tree corresponding
    * to the specified table.
    * @param tblname the name of the table
    * @param tx the calling transaction
    */
   public TablePlan(String tblname, Transaction tx) {
      this.tblname = tblname;
	  this.tx = tx;
      ti = SimpleDB.mdMgr().getTableInfo(tblname, tx);
      si = SimpleDB.mdMgr().getStatInfo(tblname, ti, tx);
   }
   
   /**
    * Creates a table scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      return new TableScan(ti, tx);
   }
   
   /**
    * Estimates the number of block accesses for the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#blocksAccessed()
    */ 
   public long blocksAccessed() {
      return si.blocksAccessed();
   }
   
   /**
    * Estimates the number of records in the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public long recordsOutput() {
      return si.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct field values in the table,
    * which is obtainable from the statistics manager.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public long distinctValues(String fldname) {
      return si.distinctValues(fldname);
   }
   
   /**
    * Determines the schema of the table,
    * which is obtainable from the catalog manager.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return ti.schema();
   }
   
   /**
    * Returns a string representation of the plan
    * @return string representation of the plan
    */
   public String toString()
   {
	   return "(Table: " + ti.tableName() + ")";
   }
  
   /**
    * compares two plans
    * @return true if the plans are the same
    */
   public boolean equals(Plan p)
   {
       if (!(p instanceof TablePlan)) return false;
       TablePlan tp = (TablePlan)p;
       return tblname.equals(tp.tblname);
   }

   /**
    * Checks if the plan contains p
    * @param p the plan being looked for
    * @return true if the plan contains p
    */
   public boolean contains(Plan p)
   {
       return p.equals(this);
   }
   
   /**
    * Returns an iterator for the plan. Iterator runs through all sub-plans
    * @return iterator for the plan
    */
   public Iterator<Plan> iterator()
   {
       return new TPIter(this);
   }
   
   /**
    * Iterator for TablePlan. Returns the single item which is the table name.
    */
   private class TPIter implements Iterator<Plan>
   {
       private boolean done = false;
       private TablePlan plan;
       
       public TPIter(TablePlan plan)
       { this.plan = plan; }
       
       public boolean hasNext()
       { return !done; }
       
       public Plan next()
       {
           if (done) throw new NoSuchElementException();
           done = true;
           return plan;
       }
       
       public void remove()
       { throw new UnsupportedOperationException(); }
   }
}
