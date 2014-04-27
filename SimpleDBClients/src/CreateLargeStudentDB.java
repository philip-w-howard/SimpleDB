import java.sql.*;
import simpledb.remote.SimpleDriver;

public class CreateLargeStudentDB {
    public static void main(String[] args) {
		final int NUM_STUDENTS = 1000;
		final int NUM_DEPTS = 7;
		final int NUM_YEARS = 10;
		final int NUM_COURSES = 30;
		final int NUM_SECTIONS_PER_YEAR = 3;
		final int NUM_STUDENTS_PER_SECTION = 5;
		final int NUM_PROFS = 29;
		
    	Connection conn = null;
		try {
			Driver d = new SimpleDriver();
			conn = d.connect("jdbc:simpledb://localhost", null);
			Statement stmt = conn.createStatement();

			String s;
			
			// DEPT **********************************************
			s = "create table DEPT(DId int, DName varchar(8))";
			stmt.executeUpdate(s);
			System.out.println("Table DEPT created.");

			for (int ii=0; ii<NUM_DEPTS; ii++)
			{
				s = String.format("insert into DEPT(DId, DName) values " +
						"(%d, 'dept%02d')", ii+1, ii+1);
				stmt.executeUpdate(s);
			}
			System.out.println("DEPT records inserted.");

			// COURSE **********************************************
			s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
			stmt.executeUpdate(s);
			System.out.println("Table COURSE created.");

			for (int ii=0; ii<NUM_COURSES; ii++)
			{
				s = String.format("insert into COURSE(CId, Title, DeptId) values " +
						"(%d, 'course_%03d', %d)", ii+100, ii+1, (ii%NUM_DEPTS)+1);
				stmt.executeUpdate(s);
			}
			System.out.println("COURSE records inserted.");

			// SECTION **********************************************
			s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
			stmt.executeUpdate(s);
			System.out.println("Table SECTION created.");

			for (int ii=0; ii<NUM_YEARS*NUM_SECTIONS_PER_YEAR*NUM_COURSES; ii++)
			{
				s = String.format("insert into SECTION(SectId, CourseId, Prof, YearOffered) values " +
						"(%d, %d, 'prof_%d', %d)",
						ii+1, ii%NUM_COURSES+100, ii%NUM_PROFS, 2014-ii%NUM_YEARS);
				//System.out.println(s);
				stmt.executeUpdate(s);				
			}
			
			System.out.println("SECTION records inserted.");

			// STUDENT **********************************************
			s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
			stmt.executeUpdate(s);
			System.out.println("Table STUDENT created.");

			for (int ii=0; ii<NUM_STUDENTS; ii++)
			{
				s = String.format("insert into STUDENT(SId, SName, MajorId, GradYear) values " +
						"(%d, 'st_%05d', %d, %d)",
						ii+1, ii+1, ii%NUM_DEPTS+1, 2014-ii%NUM_YEARS);
				stmt.executeUpdate(s);
			}
			System.out.println("STUDENT records inserted.");

			// ENROLL **********************************************
			s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
			stmt.executeUpdate(s);
			System.out.println("Table ENROLL created.");

			String[] grade = {"A", "B", "C", "D", "F"};
			for (int ii=0; ii<NUM_YEARS*NUM_SECTIONS_PER_YEAR*NUM_COURSES*NUM_STUDENTS_PER_SECTION; ii++)
			{
				s = String.format("insert into ENROLL(EId, StudentId, SectionId, Grade) values " +
						"(%d, %d, %d, '%s')", ii+1000, ii%NUM_STUDENTS+1, ii%(NUM_YEARS*NUM_SECTIONS_PER_YEAR*NUM_COURSES)+1,
						grade[ii%grade.length]);
				stmt.executeUpdate(s);			
			}
			System.out.println("ENROLL records inserted.");
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
