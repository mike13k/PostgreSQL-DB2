package src;

import java.sql.Statement;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLJDBC {

    public static void main(String args[]) {
        
        Connection c = null;
        Statement stmt = null;
        
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/DB2Project2", "postgres", "010DB2");
            
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");
            
            int mid, year, num_ratings;
            float rating;
            String round;

            for(int i = 1; i <= 100; i++) {    
                
                mid = i;
                year = generateYear();
                num_ratings = generateNumRatings();
                rating = generateRating();
                round = generateRound();
                
                stmt = c.createStatement();
                String sql = "INSERT INTO cup_matches (mid, round, year, num_ratings, rating) "
                        + String.format("VALUES (%d, '%s', %d, %d, %f );", mid,round,year,num_ratings,rating);
                stmt.executeUpdate(sql);
            
            }

            stmt.close();
            c.commit();
            c.close();
            
            
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        
    }

    private static String generateRound() {
        String round = "";
        int i = (int) (Math.random() * 5 );
        switch(i) {
        case 0: round = "32nd"; break;
        case 1: round = "16th"; break;
        case 2: round = "quarter_final"; break;
        case 3: round = "SemiFinal"; break;
        case 4: round = "Final"; break;
        }
        return round;
    }

    private static float generateRating() {
        float i = (float) (Math.random() * 11);
        return i;
    }

    private static int generateNumRatings() {
        int i = (int) (Math.random() * 100000001 );
        return i;
    }

    private static int generateYear() {
        boolean flag = false;
        int i = 0;
        while(!flag) {
            i = (int) ((Math.random() * 89 ) + 1930);
            if((i%4) == 0)
                flag = true;
        }
        return i;
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
			/*
			for(int matchesCreated = 0; matchesCreated < numOfMatchesToInsert; matchesCreated++ ) {
				sqlValues = "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
				sql = sqlInsert_cup_matches + sqlValues;
				stmt.executeUpdate(sql);

			}*/
			sqlValues = "VALUES (1, 5, 2000, 4, 5.00 );";
			sql = sqlInsert_cup_matches + sqlValues;
			stmt.executeUpdate(sql);

			sqlValues = "VALUES (1,'bas', 1996, 5 );";
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
	
	public static ArrayList<String> readCSV(String filePath) throws IOException {
		
		BufferedReader bf = new BufferedReader(new FileReader(filePath));
		
		String extractedRow = "";
		
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
