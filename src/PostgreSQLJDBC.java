package src;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLJDBC {

    public static void main(String args[]) {
    	
    	//For Step 2
    	insertIntoDatabase(2680, 58960, 118);
    	
    	//For last step (millions)
    	//insertIntoDatabase(5000000, 110000000, 250000);
        
    }
    
	public static void testConnection() {
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
	}


    /**
     * Step 2:
	 * Write a program that populates the database with 2,680 matches and 58,960
	 * players. We are specifying an exact number of tuples in each table for
	 * consistency and usage in different parts in the assignment. You must insert
	 * players with name that contain the word “pele” 118 times.
	 * 
	 * Step Last:
	 * 5 Mn matches, 110 Mn players
	 * Write a program that populates an empty database with the number of rows above.
	 * We are specifying an exact number of tuples in each table for consistency and usage in different
	 * parts in the assignment. You must insert players with name that contain the word “pele” 250,000
	 * times.
	 *
	 *	
	 *
	 * @params
	 * numOfMatches : total number of matches to be inserted.
	 * numOfPlayers : total number of players to be inserted.
	 * numOfPele 	: number of players named 'Pele' to be inserted.
	 * 
	 * 
	 */

	public static void insertIntoDatabase(int numOfMatches, int numOfPlayers, int numOfPele) {

		int numOfPlayersNamedPeleToBeInserted = numOfPele;//118;
		int numOfMatchesToInsert = numOfMatches;//2680;
		int numOfPlayersToInsert = numOfPlayers;//58960;
		int numOfPlayersPerMatch = numOfPlayersToInsert / numOfMatchesToInsert;

		int m_mid, m_year, m_num_ratings;
		float m_rating;
		String m_round;
		int p_year, p_position;
		String p_name;

		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/DB2Project2", "postgres", "010DB2");
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");

			stmt = c.createStatement();
			String sql = null;

			for (int matchesCreated = 1; matchesCreated <= numOfMatchesToInsert; matchesCreated++) {

				m_mid = matchesCreated;
				m_year = generateYear();
				m_num_ratings = generateNumRatings();
				m_rating = generateRating();
				m_round = generateRound();

				sql = "INSERT INTO cup_matches (mid, round, year, num_ratings, rating) " + String
						.format("VALUES (%d, '%s', %d, %d, %f );", m_mid, m_round, m_year, m_num_ratings, m_rating);
				stmt.executeUpdate(sql);
				System.out.println("Something added!");
				// Insert a batch of players
				for (int playersCreated = 1; playersCreated <= numOfPlayersPerMatch; playersCreated++) {
					if (numOfPlayersNamedPeleToBeInserted > 0 && (getRandomNumBetweenTenAndZero() <= 3
							|| (numOfPlayersToInsert - playersCreated - numOfPlayersNamedPeleToBeInserted) <= 0)) {
						// If Left number of players to be inserted just equal as the number needed for
						// "Pele" players,
						// Or If we are just in the middle, and it happens randomly we can insert a
						// "Pele" Player
						// Insert some "Pele"
						p_name = "pele" + playersCreated;
						numOfPlayersNamedPeleToBeInserted--;
					} else {
						p_name = generatePlayerName() + playersCreated;

					}

					p_year = generateYear();
					p_position = generatePlayerPosition();

					sql = "INSERT INTO played_in (mid, name, year, position)  "
							+ String.format("VALUES (%d, '%s', %d, %d);", m_mid, p_name, p_year, p_position);
					stmt.executeUpdate(sql);
				}
				numOfPlayersToInsert -= numOfPlayersPerMatch;
			}

			stmt.close();
			c.commit();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Records created successfully");
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
        float i = (float) (Math.random() * 10);
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
            i = (int) ((Math.random() * 73 ) + 1942);
            if((i%4) == 0)
                flag = true;
        }
        return i;
    }

	private static int generatePlayerPosition() {
		int i = (int) (Math.random() * 4) + 1;
        return i;
	}

	private static String generatePlayerName() {
		// TODO Auto-generated method stub
		return "a player";
	}

	private static int getRandomNumBetweenTenAndZero() {
		return ((int) Math.random() * 11);
	}

}
