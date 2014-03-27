package studentClient.simpledb;

//import java.sql.*;
//import simpledb.remote.SimpleDriver;
//import simpledb.tx.Transaction;
//import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.LogRecord;

/**
 *
 * @author phil
 */
public class LogPrinter {

   public static void main(String[] args) {
      try {
         // analogous to the driver
         SimpleDB.init("simpleDBData");

         LogRecordIterator iter = new LogRecordIterator();
         while (iter.hasNext())
         {
            LogRecord rec = iter.next();
            System.out.println(rec);
         }

      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}

