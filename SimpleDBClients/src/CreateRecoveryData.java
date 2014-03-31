import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;


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
					"('record14', 14, 'sentences.')", //		points
					"('record15', 15, 'bogus stuff')", //
					"('record16', 99, 'more stuff')"
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    public static void oldMain(String[] args) {
		Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s = "create table RECOVERYDATA(RName varchar(10), RId int, Text varchar(20))";
			stmt.executeUpdate(s);
			System.out.println("Table RECOVERYDATA created.");

			s = "insert into RECOVERYDATA(RName, RId, Text) values ";
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
					"('record14', 14, 'sentences.')", //		points
					"('record15', 15, 'bogus stuff')", //
					"('record16', 99, 'more stuff')"
			};
			for (int i=0; i<recovervals.length; i++) {
				System.out.println(s+recovervals[i]);
				stmt.executeUpdate(s + recovervals[i]);
			}
			System.out.println("RECOVERYDATA records inserted.");
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
