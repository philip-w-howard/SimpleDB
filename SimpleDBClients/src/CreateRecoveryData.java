import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.file.Block;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.tx.recovery.CheckpointRecord;
import simpledb.tx.recovery.CommitRecord;
import simpledb.tx.recovery.RollbackRecord;
import simpledb.tx.recovery.SetIntRecord;
import simpledb.tx.recovery.SetStringRecord;
import simpledb.tx.recovery.StartRecord;


public class CreateRecoveryData {
	public static void main(String[] args) {
		System.out.println(System.getProperty("file.encoding"));
		String dbName = "simpleDBDir";
		if (args.length > 0)
			dbName = args[0];

		System.out.println("Connecting to " + dbName);
		
		try {
			// analogous to the driver
			SimpleDB.init(dbName);

			// analogous to the connection
			Transaction tx = new Transaction();

			// analogous to the statement
			String create = "create table RECOVERYDATA(RName varchar(10), RId int, Text varchar(20))";
			SimpleDB.planner().executeUpdate(create, tx);

			String delete = "delete from RECOVERYDATA";
			SimpleDB.planner().executeUpdate(delete, tx);
			
			String cmd = "insert into RECOVERYDATA(RName, RId, Text) values ";
			String[] recovervals = {
					"('record1', 1, 'This')", //
					"('record2', 2, 'flummoxed')", //		is
					"('record3', 3, 'a')", //
					"('record4', 45, 'mollusk')", //			4, test
					"('record5', 5, 'of')", //				55
					"('record6', 6, 'recovery.')", // 			12, your code
					"('record7', 7, 'If')", //
					"('record8', 8, 'pizza')", //			it
					"('record9', 9, 'works')", //			91, blows up
					"('record10', 10, 'you')", //
					"('record11', 11, 'wont')", //			should
					"('record12', 12, 'get')", //			give
					"('record13', 13, 'two')", //
					"('record14', 14, 'sentences.')" //		points
					//"('record15', 15, 'bogus stuff')", //
					//"('record16', 99, 'more stuff')"

			};
			for (int i=0; i<recovervals.length; i++) {
				System.out.println(cmd+recovervals[i]);
				SimpleDB.planner().executeUpdate(cmd+recovervals[i], tx);
			}
			tx.commit();
			System.out.println("RECOVERYDATA records inserted.");

			tx = new Transaction();
			String select = "select RName, RId, Text from RECOVERYDATA";
			Plan p = SimpleDB.planner().createQueryPlan(select, tx);
			// analogous to the result set
			Scan s = p.open();

			System.out.println("Name\tMajor");
			while (s.next()) {
				String rname = s.getString("rname"); // SimpleDB stores field names
				String text = s.getString("text"); // in lower case
				int id = s.getInt("rid");
				System.out.println(id + "\t" + rname + "\t" + text);
			}
			s.close();
			tx.commit();
			
			//******************************************
			// Artificially create log records
			//******************************************
			new CheckpointRecord().writeToLog();
			
			Block block_0 = new Block("recoverydata.tbl", 0); 
			Block block_1 = new Block("recoverydata.tbl", 1); 
			Block block_2 = new Block("recoverydata.tbl", 2); 
			Block block_3 = new Block("recoverydata.tbl", 3); 
			Block block_4 = new Block("recoverydata.tbl", 4); 
			Block block_5 = new Block("recoverydata.tbl", 5); 
			// <START 100>
			new StartRecord(100).writeToLog();
			// <SETSTRING 100 [file recoverydata.tbl, block 0] 110 ‘flummoxed’ ‘is’>
			new SetStringRecord(100, block_0, 110, "flummoxed", "is").writeToLog();
			// <SETINT 100 [file recoverydata.tbl, block 1] 68 45 4>
			new SetIntRecord(100, block_1, 68, 45, 5).writeToLog();
			
			// <START 101>
			new StartRecord(101).writeToLog();
			// <START 102>
			new StartRecord(102).writeToLog();
			// <SETINT 101 [file recoverydata.tbl, block 1] 174 5 55>
			new SetIntRecord(101, block_1, 174, 5, 55).writeToLog();
			// <SETSTRING 100 [file recoverydata.tbl, block 1] 4 ‘mollusk’ ‘test’>
			new SetStringRecord(100, block_1, 4, "mollusk", "test").writeToLog();
			// <COMMIT 100>
			new CommitRecord(100).writeToLog();
			// <SETSTRING 101 [file recoverydata.tbl, block 3] 216 ‘get’ ‘give’>
			new SetStringRecord(101, block_3, 216, "get", "give").writeToLog();
			// SKIP <ROLLBACK 101> SKIP
			
			// <SETINT 102 [file recoverydata.tbl, block 1] 280 6 12>
			new SetIntRecord(102, block_1, 280, 6, 12).writeToLog();
			// <SETSTRING 102 [file recoverydata.tbl, block 1] 216 ‘recovery.’ ‘your code’>
			new SetStringRecord(102, block_1, 216, "recovery.", "your code").writeToLog();
			// <SETINT 102 [file recoverydata.tbl, block 2] 280 9 91>
			new SetIntRecord(102, block_2, 280, 9, 91).writeToLog();
			// <SETSTRING 102 [file recoverydata.tbl, block 2] 216 ‘works’ ‘blows up’>
			new SetStringRecord(102, block_2, 216, "works", "blows up").writeToLog();
			
			// <START 103>
			new StartRecord(103).writeToLog();
			// <SETSTRING 103 [file recoverydata.tbl, block 4] 110 ‘sentences.’ ‘points’>
			new SetStringRecord(103, block_4, 110, "sentences.", "points").writeToLog();
			// <ROLLBACK 103>
			new RollbackRecord(103).writeToLog();
			
			// <START 104>
			new StartRecord(104).writeToLog();
			// <ROLLBACK 102>
			new RollbackRecord(102).writeToLog();
			// <SETSTRING 104 [file recoverydata.tbl, block 2] 110 ‘pizza’ ‘it’>
			new SetStringRecord(104, block_2, 110, "pizza", "it").writeToLog();
			// <COMMIT 104>
			new CommitRecord(104).writeToLog();
			
			// <START 105>
			new StartRecord(105).writeToLog();
			// <SETSTRING 105 [file recoverydata.tbl, block 3] 110 ‘wont’ ‘should’>
			new SetStringRecord(105, block_3, 110, "wont", "should").writeToLog();
			
			// <START 106>
			new StartRecord(106).writeToLog();
			// <SETINT 106 [file recoverydata.tbl, block 4] 212 0 1>
			new SetIntRecord(106, block_4, 212, 0, 1).writeToLog();
			// <SETSTRING 106 [file recoverydata.tbl, block 4] 284 ‘’ ‘record15’>
			new SetStringRecord(106, block_4, 284, "", "record15").writeToLog();
			// <SETINT 106 [file recoverydata.tbl, block 4] 280 0 15>
			new SetIntRecord(106, block_4, 280, 0, 15).writeToLog();
			// <SETSTRING 106 [file recoverydata.tbl, block 4] 216 ‘’ ‘bogus stuff’>
			new SetStringRecord(106, block_4, 216, "", "bogus stuff").writeToLog();
			// <SETINT 106 [file recoverydata.tbl, block 5] 0 0 1>
			new SetIntRecord(106, block_5, 0, 0, 1).writeToLog();
			// <SETSTRING 106 [file recoverydata.tbl, block 5] 72 ‘’ ‘record16’>
			new SetStringRecord(106, block_5, 72, "", "record16").writeToLog();
			// <COMMIT 105>
			new CommitRecord(105).writeToLog();
			// <SETINT 2 [file recoverydata.tbl, block 5] 68 0 99>
			new SetIntRecord(106, block_5, 68, 0, 99).writeToLog();
			// <SETSTRING 106 [file recoverydata.tbl, block 5] 4 ‘’ ‘more stuff’>
			new SetStringRecord(106, block_5, 4, "", "more stuff").writeToLog();
			// SKIP <ROLLBACK 106> SKIP
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
