package simpledb.query;

import java.util.Iterator;
import simpledb.record.Schema;
import java.util.Collection;

/** The Plan class corresponding to the <i>project</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class ProjectPlan implements Plan {
   private Plan p;
   private Schema schema = new Schema();
   
   /**
    * Creates a new project node in the query tree,
    * having the specified subquery and field list.
    * @param p the subquery
    * @param fieldlist the list of fields
    */
   public ProjectPlan(Plan p, Collection<String> fieldlist) {
      this.p = p;
      for (String fldname : fieldlist)
         schema.add(fldname, p.schema());
   }
   
   /**
    * Creates a project scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
      return new ProjectScan(s, schema.fields());
   }
   
   /**
    * Estimates the number of block accesses in the projection,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public long blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Estimates the number of output records in the projection,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public long recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct field values
    * in the projection,
    * which is the same as in the underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public long distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the projection,
    * which is taken from the field list.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }
   
   public String toString()
   {
	   String returnVal = "(Project: " + p + "{";
	   for (String field : schema.fields())
	   {
		   returnVal += field + ", ";
	   }
	   // trim the last ", "
	   returnVal = returnVal.substring(0, returnVal.length()-2);   
	   returnVal += "})";
	   return returnVal;
   }

   /**
    * compares two plans
    * @return true if the plans are the same
    */
   public boolean equals(Plan p)
   {
       if (!(p instanceof ProjectPlan)) return false;
       ProjectPlan pp = (ProjectPlan)p;
       return this.p.equals(pp.p) && this.schema.equals(pp.schema);
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
       return new PrPIter(this);
   }
   
   /**
    * Iterator for TablePlan. Returns the single item which is the table name.
    */
   private class PrPIter implements Iterator<Plan>
   {
       private boolean done = false;
       private ProjectPlan plan;
       private Iterator<Plan> iter;
       
       public PrPIter(ProjectPlan plan)
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
