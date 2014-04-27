import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import simpledb.remote.RemoteResultSet;
import simpledb.remote.SimpleDriver;


public class ShowRecovery {
    private static Connection conn = null;

    public static void main(String[] args) {
	   try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);

			//doQuery("select SName from STUDENT where SId=995");
			//doQuery("select SName,DName from STUDENT,DEPT where MajorId=DId and SId=995");
			//doQuery("select Title,Prof from SECTION,COURSE where CId=CourseId and Title='course_029' and Prof='prof_28'");
			//doQuery("select SName,Grade from ENROLL,STUDENT where StudentId=SId and SId=995");
			doQuery("select SName,Grade,YearOffered from STUDENT,ENROLL,SECTION where StudentId=SId and SectId=SectionId and SId=995");
			//doQuery("select SName,Grade,YearOffered,Title from STUDENT,ENROLL,SECTION,COURSE where StudentId=SId and SectId=SectionId and CId=CourseId and SId=995");
			//doQuery("select SName,DName,Title from STUDENT,DEPT,ENROLL,SECTION,COURSE " +
			//		"where MajorId=DId and SId=StudentId and CId=CourseId and SectId=SectionId and SId=995");
				    }
	    catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if (conn != null)
					conn.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void doQuery(String cmd) {
		try {
		    Statement stmt = conn.createStatement();
	        long startTime = System.currentTimeMillis();
		    ResultSet rs = stmt.executeQuery(cmd);
		    ResultSetMetaData md = rs.getMetaData();
		    int numcols = md.getColumnCount();
		    int totalwidth = 0;

		    // print header
		    for(int i=1; i<=numcols; i++) {
				int width = md.getColumnDisplaySize(i);
				totalwidth += width;
				String fmt = "%" + width + "s";
				System.out.format(fmt, md.getColumnName(i));
			}
			System.out.println();
			for(int i=0; i<totalwidth; i++)
			    System.out.print("-");
		    System.out.println();

		    // print records
		    while(rs.next()) {
				for (int i=1; i<=numcols; i++) {
					String fldname = md.getColumnName(i);
					int fldtype = md.getColumnType(i);
					String fmt = "%" + md.getColumnDisplaySize(i);
					if (fldtype == Types.INTEGER)
						System.out.format(fmt + "d", rs.getInt(fldname));
					else
						System.out.format(fmt + "s", rs.getString(fldname));
				}
				System.out.println();
			}
	        long endTime = System.currentTimeMillis();
	        System.out.println("Query took " + (endTime-startTime) + "ms");
			rs.close();
		}
		catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
