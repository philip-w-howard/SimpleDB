package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;
import simpledb.server.SimpleDB;
import java.util.Stack;
import java.util.Iterator;

/**
 * A class that provides the ability to move through the records of the log file
 * in forward order.
 * 
 * @author Edward Sciore, Philip Howard
 */
class LogFwdIterator implements Iterator<BasicLogRecord> {
	private Block blk;
	private Page pg = new Page();
	private int currentrec;
	private FileMgr fm = SimpleDB.fileMgr();
	private Stack<Integer> recordList = new Stack<Integer>();

	/**
	 * Creates an iterator for the records in the log file,
	 * positioned after the last log record.
	 * This constructor is called exclusively by
	 * {@link LogMgr#fwdIterator()}.
	 * @param blk The block to start the iterator at
	 */
	LogFwdIterator(Block blk) {
		this.blk = new Block(blk.fileName(), blk.number()-1);
		moveToNextBlock();
	}

	/**
	 * Determines if the current log record
	 * is the earliest record in the log file.
	 * @return true if there is an earlier record
	 */
	public boolean hasNext() {
		if (!recordList.empty()) return true;
		if (blk.number() < fm.size(blk.fileName())) return true;
		return false;
	}

	/**
	 * Moves to the next log record in reverse order.
	 * If the current log record is the earliest in its block,
	 * then the method moves to the next oldest block,
	 * and returns the log record from there.
	 * @return the next earliest log record
	 */
	public BasicLogRecord next() {
		if (recordList.empty()) moveToNextBlock();
		currentrec = recordList.pop(); 
		BasicLogRecord rec = new BasicLogRecord(pg, currentrec+INT_SIZE);
		return rec;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order,
	 * and positions it after the last record in that block.
	 */
	private void moveToNextBlock() {
		blk = new Block(blk.fileName(), blk.number()+1);
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
		currentrec = pg.getInt(currentrec);		// back up one
		recordList.clear();
		while (currentrec > 0)
		{
			recordList.push(currentrec);
			currentrec = pg.getInt(currentrec);
		}
		recordList.push(0);
		currentrec = 0;
	}
}
