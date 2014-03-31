import java.nio.charset.Charset;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

/**
 * 
 * @author phil
 */
public class DumpPage {

	public static void main(String[] args) {
		try {
			// analogous to the driver
			SimpleDB.initFileLogAndBufferMgr("simpleDBDir");
		    float bytesPerChar = (int)Charset.defaultCharset().newEncoder().maxBytesPerChar();

			//Block blk = new Block("simpledb.log", 1);
			Block blk = new Block("tblcat.tbl", 0);
			Page p1 = new Page();
			p1.read(blk);
			for (int ii = 0; ii < 100; ii++) {
			   int val = p1.getInt(ii * 4);
			   System.out.printf("val: %d %d %d 0x%08X\n", ii, ii*4, val, val);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
