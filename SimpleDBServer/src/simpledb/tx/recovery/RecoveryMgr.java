package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import java.util.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    */
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then writes a quiescent checkpoint record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CheckpointRecord().writeToLog();
      SimpleDB.logMgr().flush(lsn);

   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval, newval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval, newval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for a non-committed
    * transaction, it calls undo() on that record.
    * Once a CHECPOINT record is found (or the end of the log), 
    * the method iterates forward through the log redoing the updates
    * from committed transactions. Redos are performed by calling redo()
    * on the relevant records.
    */
   private void doRecover() {
      Collection<Integer> committedTxs = new ArrayList<Integer>();
      
      // undo phase
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.op() == CHECKPOINT)
            break;
         else if (rec.op() == COMMIT)
        	 committedTxs.add(rec.txNumber());
         else if (!committedTxs.contains(rec.txNumber()))
         {
            rec.undo(txnum);
       	    System.out.println("undo: " + rec);
         }
      }
      
      // redo phase
	  iter = new LogRecordFwdIterator((LogRecordIterator) iter);
      while (iter.hasNext()) {
          LogRecord rec = iter.next();
          if (committedTxs.contains(rec.txNumber()))
          {
             rec.redo(txnum);
        	 System.out.println("redo: " + rec);
          }
       }

   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
}
