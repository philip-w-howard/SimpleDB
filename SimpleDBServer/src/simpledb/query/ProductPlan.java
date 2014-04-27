package simpledb.query;

import java.util.Iterator;
import java.util.NoSuchElementException;
import simpledb.record.Schema;

/** The Plan class corresponding to the <i>product</i>
  * relational algebra operator.
  * @author Edward Sciore, Philip Howard
  */
public class ProductPlan implements Plan {
   private Plan p1, p2;
   private Schema schema = new Schema();
   
   /**
    * Creates a new product node in the query tree,
    * having the two specified subqueries.
    * @param p1 the left-hand subquery
    * @param p2 the right-hand subquery
    */
   public ProductPlan(Plan p1, Plan p2) {
      this.p1 = p1;
      this.p2 = p2;
      schema.addAll(p1.schema());
      schema.addAll(p2.schema());
   }
   
   /**
    * Creates a product scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new ProductScan(s1, s2);
   }
   
   /**
    * Estimates the number of block accesses in the product.
    * The formula is:
    * <pre> B(product(p1,p2)) = B(p1) + R(p1)*B(p2) </pre>
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public long blocksAccessed() {
      return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
   }
   
   /**
    * Estimates the number of output records in the product.
    * The formula is:
    * <pre> R(product(p1,p2)) = R(p1)*R(p2) </pre>
    * @see simpledb.query.Plan#recordsOutput()
    */
   public long recordsOutput() {
      return p1.recordsOutput() * p2.recordsOutput();
   }
   
   /**
    * Estimates the distinct number of field values in the product.
    * Since the product does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public long distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the product,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }
   
   public String toString()
   {
	   return "(Product: " + p1 + "," + p2 + ")";
   }

   public boolean equals(Plan p)
   {
       if (!(p instanceof ProductPlan)) return false;
       ProductPlan pp = (ProductPlan)p;
       return p1.equals(pp.p1) && p2.equals(pp.p2);
   }
   /**
    * Checks if the plan contains p
    * @param p the plan being looked for
    * @return true if the plan contains p
    */
   public boolean contains(Plan p)
   {
       return p1.equals(p) || p2.equals(p) || p1.contains(p) || p2.contains(p);
   }
   
   /**
    * Returns an iterator for the plan. Iterator runs through all sub-plans
    * @return iterator for the plan
    */
   public Iterator<Plan> iterator()
   {
       return new PPIter(this);
   }
   
   /**
    * Iterator for the ProductPlan. Iterates left plan (p1) then right (p2)
    */
   private class PPIter implements Iterator<Plan>
   {
       private Iterator<Plan> leftIter, rightIter;
       
       public PPIter(ProductPlan plan)
       { 
           leftIter = plan.p1.iterator();
           rightIter = plan.p2.iterator();
       }
       
       public boolean hasNext()
       { 
           return leftIter.hasNext() || rightIter.hasNext(); 
       }
       
       public Plan next()
       {
           if (leftIter.hasNext()) 
               return leftIter.next();
           else if (rightIter.hasNext())
               return rightIter.next();
           else
               throw new NoSuchElementException();
       }
       
       public void remove()
       { throw new UnsupportedOperationException(); }
   }
}
