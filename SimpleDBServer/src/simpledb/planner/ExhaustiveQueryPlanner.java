package simpledb.planner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import simpledb.parse.QueryData;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * Program to exhausively search for a "best" plan
 * 
 * @author Philip Howard 
 * @version 2014-04-17
 */
public class ExhaustiveQueryPlanner implements QueryPlanner {

    public Plan createPlan(QueryData data, Transaction tx)
    {
        ArrayList<ArrayList<Plan>> planList = new ArrayList<ArrayList<Plan>>();
        ArrayList<Plan> newList = null;

        //Step 1: Create a plan for each mentioned table or view
        ArrayList<Plan> plans = new ArrayList<Plan>();
        for (String tblname : data.tables()) {
           String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
           if (viewdef != null)
              plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
           else
              plans.add(new TablePlan(tblname, tx));
        }
        
        //Step 2: Create the product of all table plans
        // NOTE: this code needs to be replaced with exhaustive search
        //       for best plan.
        Plan p = plans.remove(0);
        for (Plan nextplan : plans)
           p = new ProductPlan(p, nextplan);        
       
        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());
        
        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }

}
