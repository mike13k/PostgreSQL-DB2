import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQLJDBC {

    public static void main(String args[]) {
        
        Connection c = null;
        Statement stmt = null;
        
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/cup", "postgres", "12121213");
            
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");
            
            int mid, year, num_ratings;
            float rating;
            String round;

            for(int i = 1; i <= 1000000; i++) {    
                
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
        // TODO Auto-generated method stub
        return null;
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
    
}
