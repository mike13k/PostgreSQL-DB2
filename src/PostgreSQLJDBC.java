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

		/*
		 * Connection c = null; Statement stmt = null;
		 * 
		 * try { Class.forName("org.postgresql.Driver"); c =
		 * DriverManager.getConnection("jdbc:postgresql://localhost:5432/DB2Project2",
		 * "postgres", "010DB2");
		 * 
		 * c.setAutoCommit(false); System.out.println("Opened database successfully");
		 * 
		 * int mid, year, num_ratings; float rating; String round;
		 * 
		 * for (int i = 1; i <= 1000000; i++) {
		 * 
		 * mid = i; year = generateYear(); num_ratings = generateNumRatings(); rating =
		 * generateRating(); round = generateRound();
		 * 
		 * stmt = c.createStatement(); String sql =
		 * "INSERT INTO cup_matches (mid, round, year, num_ratings, rating) " +
		 * String.format("VALUES (%d, '%s', %d, %d, %f );", mid, round, year,
		 * num_ratings, rating); stmt.executeUpdate(sql);
		 * 
		 * }
		 * 
		 * stmt.close(); c.commit(); c.close();
		 * 
		 * } catch (Exception e) { e.printStackTrace();
		 * System.err.println(e.getClass().getName() + ": " + e.getMessage());
		 * System.exit(0); }
		 */
		
		insertForStepTwo();

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
	 * Write a program that populates the database with 2,680 matches and 58,960
	 * players. We are specifying an exact number of tuples in each table for
	 * consistency and usage in different parts in the assignment. You must insert
	 * players with name that contain the word �pele� 118 times.
	 */

	public static void insertForStepTwo() {

		int numOfPlayersNamedPeleToBeInserted = 5;//118;
		int numOfMatchesToInsert = 30;//2680;
		int numOfPlayersToInsert = 1000;//58960;
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

			for (int matchesCreated = 0; matchesCreated < numOfMatchesToInsert; matchesCreated++) {

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
				for (int playersCreated = 0; playersCreated < numOfPlayersPerMatch; playersCreated++) {
					if (numOfPlayersNamedPeleToBeInserted > 0 && (getRandomNumBetweenTenAndZero() <= 3
							|| (numOfPlayersToInsert - playersCreated - numOfPlayersNamedPeleToBeInserted) <= 0)) {
						// If Left number of players to be inserted just equal as the number needed for
						// "Pele" players,
						// Or If we are just in the middle, and it happens randomly we can insert a
						// "Pele" Player
						// Insert some "Pele"
						p_name = "pele";
						numOfPlayersNamedPeleToBeInserted--;
					} else {
						p_name = generatePlayerName();

					}

					p_year = generateYear();
					p_position = generatePlayerPosition();

					sql = "INSERT INTO played_in (mid, name, year, position)  "
							+ String.format("VALUES (%d, '%s', %d, %d, %f );", m_mid, p_name, p_year, p_position);
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
		// TODO Auto-generated method stub
		return "Abc";
	}

	private static float generateRating() {
		// TODO Auto-generated method stub
		return 0;
	}

	private static int generateNumRatings() {
		// TODO Auto-generated method stub
		return 0;
	}

	private static int generateYear() {
		// TODO Auto-generated method stub
		return 0;
	}

	private static int generatePlayerPosition() {
		// TODO Auto-generated method stub
		return 0;
	}

	private static String generatePlayerName() {
		// TODO Auto-generated method stub
		return "a player";
	}

	private static int getRandomNumBetweenTenAndZero() {
		return ((int) Math.random() * 11);
	}

}
