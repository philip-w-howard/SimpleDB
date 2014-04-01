import java.util.Iterator;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecordIterator;
import simpledb.tx.recovery.LogRecordFwdIterator;
import simpledb.tx.recovery.LogRecord;

/**
 * 
 * @author phil
 */
public class PrintLog {

	public static void main(String[] args) {
		int count = 0;
		try {
			// analogous to the driver
			SimpleDB.initFileLogAndBufferMgr("simpleDBDir");

			Iterator<LogRecord> iter = new LogRecordIterator();
			while (iter.hasNext()) {
				LogRecord rec = iter.next();
				System.out.println(rec);
				if (rec.op()== LogRecord.CHECKPOINT) break;
			}
			
			System.out.println("Going forward...");
			
			Iterator<LogRecord> fwdIter = new LogRecordFwdIterator((LogRecordIterator) iter);
//			Iterator<LogRecord> fwdIter = new LogRecordFwdIterator();
			while (fwdIter.hasNext()) {
				LogRecord rec = fwdIter.next();
				System.out.println(rec);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
