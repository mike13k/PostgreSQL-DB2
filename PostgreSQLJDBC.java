
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class PostgreSQLJDBC {

	public static void main(String args[]) {
		
		//Test Connection to database
		/*
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/DB2Project2", "postgres", "010DB2");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");
		*/
		
		insertForStepTwo();
	}

	/**
	 * Write a program that populates the database with 2,680 matches and 58,960
	 * players. We are specifying an exact number of tuples in each table for
	 * consistency and usage in different parts in the assignment. You must insert
	 * players with name that contain the word “pele” 118 times.
	 */

	public static void insertForStepTwo() {
		
		int numOfPlayersNamedPele = 118;
		int numOfMatchesToInsert = 2680;
		int numOfPlayersToInsert = 58960;
		
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "manisha", "123");
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");

			stmt = c.createStatement();
			String sql = null;
			String sqlInsert_cup_matches = "INSERT INTO cup_matches (mid,round,year,num_ratings, rating) ";
			String sqlInsert_played_in = "INSERT INTO played_in (mid,name,year,position) ";
			String sqlValues_cup_matches =  "VALUES(?, ?, ?, ?, ?);";
			String sqlValues_played_in = "VALUES(?, ?, ?, ?);";
			String sqlValues = null;
			
			for(int matchesCreated = 0; matchesCreated < numOfMatchesToInsert; matchesCreated++ ) {
				sqlValues = "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
				sql = sqlInsert_cup_matches + sqlValues;
				stmt.executeUpdate(sql);

			}

			sqlValues = "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
			sql = sqlInsert_played_in + sqlValues;
			stmt.executeUpdate(sql);


			stmt.close();
			c.commit();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Records created successfully");
	}
	
	public static String[][] readCSV(String filePath) throws IOException {
		
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		
		
		String line = "";
		
		while((line = bf.readLine())  != null) {

			if (line != null) {
				String[] array = line.split(",+");

				for (String result : array) {
					System.out.println(result);
				}

			}
				
		
			
		}
		
		
		
		return null;
	}
	
}
