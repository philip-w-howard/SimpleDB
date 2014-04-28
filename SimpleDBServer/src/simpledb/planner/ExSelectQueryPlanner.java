package simpledb.planner;

import java.util.ArrayList;
import java.util.Iterator;

import simpledb.parse.QueryData;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Program to exhaustively search for a "best" plan
 * 
 * @author Philip Howard 
 * @version 2014-04-25
 */
public class ExSelectQueryPlanner implements QueryPlanner {
    /**
     * Checks if the plan contains tables from p
     * @param p1 the plan being compared against
     * @param p2 the plan being compared against
     * @return true if the plan contains all tables from p and only tables from p
     */
    private boolean containsAll(Plan p1, Plan p2)
    {
        ArrayList<Plan> mine = new ArrayList<Plan>();
        ArrayList<Plan> theirs = new ArrayList<Plan>();
        
        Iterator<Plan> myIter = p1.iterator();
        Iterator<Plan> theirIter = p2.iterator();
        
        while (myIter.hasNext())
        {
            Plan p = myIter.next();
            if (p instanceof TablePlan)	mine.add(p);
        }
        
        while (theirIter.hasNext())
        {
            Plan p = theirIter.next();
            if (p instanceof TablePlan)	theirs.add(p);
        }
        
        if (mine.size() != theirs.size()) return false;
        if (mine.containsAll(theirs)) return true;
        return false;
    }
    /**
     * Checks if the plan contains tables from p
     * @param p the plan being compared against
     * @return true if the plan contains all tables from p and only tables from p
     */
    private boolean containsAny(Plan p1, Plan p2)
    {
        Iterator<Plan> p1Iter = p1.iterator();
        
        while (p1Iter.hasNext())
        {
        	Plan p = p1Iter.next();
        	if (p instanceof TablePlan)
        	{
        		if (contains(p2, (TablePlan)p)) return true;
        	}
        }
        return false;
    }
    
    /**
     * Checks if plan contains the TablePlan t
     * @param plan the plan to inspect
     * @param t the table plan to look for
     * @returns true if plan contains t
     */
    private boolean contains(Plan plan, TablePlan t)
    {
        Iterator<Plan> pIter = plan.iterator();
        
        while (pIter.hasNext())
        {
        	Plan p = pIter.next();
        	if (p instanceof TablePlan)
        	{
        		if (t.equals(p)) return true;
        	}
        }
        
        return false;
    }
   
    private void trimDups(ArrayList<Plan> list)
    {
        for (int ii=0; ii<list.size(); ii++)
        {
            Plan plan = list.get(ii);
            long pCost = plan.blocksAccessed();
            int jj=ii+1;
            while (jj<list.size())
            {
                Plan next = list.get(jj);
                if (containsAll(plan, next))
                {
                    long nextCost = next.blocksAccessed();
                    if (nextCost < pCost)
                    {
                        plan = next;
                        pCost = nextCost;
                        list.set(ii, next);
                    }
                    list.remove(jj);
                } else {
                    jj++;
                }
            }
        }
    }

    public Plan createPlan(QueryData data, Transaction tx)
    {
        ArrayList<ArrayList<Plan>> planList = new ArrayList<ArrayList<Plan>>();
        ArrayList<Plan> newList = null;

//        System.out.println("Generating length 1 lists");
        ArrayList<Plan> plans = new ArrayList<Plan>();
        for (String tblname : data.tables()) {
           String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
           if (viewdef != null)
              plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
           else
           {
        	   TablePlan t = new TablePlan(tblname, tx);
        	   //System.out.printf("TablePlan: %s %d %d\n", tblname, t.blocksAccessed(), t.recordsOutput());
        	   Predicate pred = data.pred().selectPred(t.schema());
        	   if (pred != null)
        	   {
        		   SelectPlan s = new SelectPlan(t,pred);
            	   //System.out.printf("SelectPlan: %s %d %d\n", s, s.blocksAccessed(), s.recordsOutput());
        		   plans.add(s);
        	   } else {
        		   plans.add(t);
        	   }
              
           }
        }
        planList.add(plans);

        // initialize newlist to handle the case where there is only one table
        if (plans.size()==1)
        {
        	newList = new ArrayList<Plan>();
        	newList.add(plans.get(0));
        }
        
        for (int size=2; size<=plans.size(); size++)
        {
            newList = new ArrayList<Plan>();
            for (int ii=1; ii<size; ii++)
            {
                // join plans of size ii to (size-ii)
                for (Plan list : planList.get(ii-1))
                {            
                    for (Plan plan : planList.get(size-ii-1))
                    {
                        if (!containsAny(list, plan)) 
                        {
                        	Plan p = new ProductPlan(list, plan);
                        	Predicate pred = data.pred().joinPred(list.schema(), plan.schema());
                        	if (pred != null)
                        	{
                        		SelectPlan s = new SelectPlan(p,pred);
                        		newList.add(s);
                        	} else {
                        		newList.add(p);
                        	}
                        }
                    }
                }
            }
//            System.out.println("Before trim: " + newList);
            trimDups(newList);
//            System.out.println("After trim: " + newList);
            planList.add(newList);

        }
        Plan p = newList.get(0);
        
        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());
        
        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
//        System.out.printf("Returning: %d %d %s\n", p.blocksAccessed(),p.recordsOutput(), p);

        return p;
    }

}
