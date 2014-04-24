package simpledb.query;

import java.util.Iterator;
import simpledb.record.Schema;

/** The Plan class corresponding to the <i>select</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class SelectPlan implements Plan {
   private Plan p;
   private Predicate pred;
   
   /**
    * Creates a new select node in the query tree,
    * having the specified subquery and predicate.
    * @param p the subquery
    * @param pred the predicate
    */
   public SelectPlan(Plan p, Predicate pred) {
      this.p = p;
      this.pred = pred;
   }
   
   /**
    * Creates a select scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new SelectScan(s, pred);
   }
   
   /**
    * Estimates the number of block accesses in the selection,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Estimates the number of output records in the selection,
    * which is determined by the 
    * reduction factor of the predicate.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput() / pred.reductionFactor(p);
   }
   
   /**
    * Estimates the number of distinct field values
    * in the projection.
    * If the predicate contains a term equating the specified 
    * field to a constant, then this value will be 1.
    * Otherwise, it will be the number of the distinct values
    * in the underlying query 
    * (but not more than the size of the output table).
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (pred.equatesWithConstant(fldname) != null)
         return 1;
      else {
         String fldname2 = pred.equatesWithField(fldname);
         if (fldname2 != null) 
            return Math.min(p.distinctValues(fldname),
                            p.distinctValues(fldname2));
         else
            return Math.min(p.distinctValues(fldname),
                            recordsOutput());
      }
   }
   
   /**
    * Returns the schema of the selection,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return p.schema();
   }
   
   public String toString()
   {
	   return "(Select: " + p + "{" + pred + "})";
   }
   /**
    * compares two plans
    * @return true if the plans are the same
    */
   public boolean equals(Plan p)
   {
       if (!(p instanceof SelectPlan)) return false;
       SelectPlan sp = (SelectPlan)p;
       return this.p.equals(sp.p) && this.pred.equals(sp.pred);
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
       return new SPIter(this);
   }
   
   /**
    * Iterator for TablePlan. Returns the single item which is the table name.
    */
   private class SPIter implements Iterator<Plan>
   {
       private boolean done = false;
       private SelectPlan plan;
       private Iterator<Plan> iter;
       
       public SPIter(SelectPlan plan)
       { 
    	   this.plan = plan;
    	   iter = plan.iterator();
       }
       
       public boolean hasNext()
       { 
    	   return !done || iter.hasNext(); 
       }
       
       public Plan next()
       {
    	   if (!done)
    	   {
    		   done = true;
    		   return plan;
    	   } else {
  			   return iter.next();
    	   }
       }
       
       public void remove()
       { throw new UnsupportedOperationException(); }
   }
}
