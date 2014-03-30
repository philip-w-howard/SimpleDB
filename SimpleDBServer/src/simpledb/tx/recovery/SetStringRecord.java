package simpledb.tx.recovery;

import simpledb.server.SimpleDB;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;

class SetStringRecord implements LogRecord {
   private int txnum, offset;
   private String oldval, newval;
   private Block blk;
   
   /**
    * Creates a new setstring log record.
    * @param txnum the ID of the specified transaction
    * @param blk the block containing the value
    * @param offset the offset of the value in the block
    * @param val the new value
    */
   public SetStringRecord(int txnum, Block blk, int offset, String oldval, String newval) {
      this.txnum = txnum;
      this.blk = blk;
      this.offset = offset;
      this.oldval = oldval;
      this.newval = newval;
   }
   
   /**
    * Creates a log record by reading five other values from the log.
    * @param rec the basic log record
    */
   public SetStringRecord(BasicLogRecord rec) {
      txnum = rec.nextInt();
      String filename = rec.nextString();
      int blknum = rec.nextInt();
      blk = new Block(filename, blknum);
      offset = rec.nextInt();
      oldval = rec.nextString();
      newval = rec.nextString();
   }
   
   /** 
    * Writes a setString record to the log.
    * This log record contains the SETSTRING operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * string value at that offset.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
//	   if (val.length() == 0) val="<empty>";
      Object[] rec = new Object[] {SETSTRING, txnum, blk.fileName(),
         blk.number(), offset, oldval, newval};
      return logMgr.append(rec);
   }
   
   public int op() {
      return SETSTRING;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   public String toString() {
      return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + 
           oldval + " " + newval + ">";
   }
   
   /** 
    * Replaces the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls setString to restore the saved value
    * (using a dummy LSN), and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(int txnum) {
      BufferMgr buffMgr = SimpleDB.bufferMgr();
      Buffer buff = buffMgr.pin(blk);
      buff.setString(offset, oldval, txnum, -1);
      buffMgr.unpin(buff);
   }
}
