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
		try {
			// analogous to the driver
			SimpleDB.initFileLogAndBufferMgr("simpleDBData");

			LogRecordFwdIterator iter = new LogRecordFwdIterator();
			while (iter.hasNext()) {
				LogRecord rec = iter.next();
				System.out.println(rec);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
