import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class Main  {
    private static int getRandomInt(int lower, int upper) {
        return lower + (new Random()).nextInt(upper - lower);
    }
    private static final String STACK_TRACE = new String(new char[1000]).replace("\0", "A");

    public static void main(String[] args) {
        Connection connection = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:example.db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);

            statement.executeUpdate("drop table if exists error");
            statement.executeUpdate("create table error (error_id string, stacktrace string, error_date datetime)");
            int readIssues = 0;
            int writeIssues = 0;

            for (int i=1; i<=1000; i++) {
                System.out.println(String.format("Iteration=%02d, readIssues=%d, writeIssues=%d", i, readIssues, writeIssues));

                for (int j=0; j<5; j++) {
                    String uuid = java.util.UUID.randomUUID().toString();
                    try {
                        statement.executeUpdate(String.format("insert into error values('%s', '%s', '%s')", uuid, STACK_TRACE, LocalDateTime.now().format(formatter)));
                    } catch (SQLException e) {
                        writeIssues++;
                        System.err.println(e.getMessage());
                    }
                }
                Thread.sleep(getRandomInt(75,125));

                if (i % 2 == 0) {
                    try {
                        ResultSet rsRecordsCount = statement.executeQuery("select count(*) from error");
                        rsRecordsCount.next();
                        int totalCount = rsRecordsCount.getInt(1);

                        ResultSet rs = statement.executeQuery("select * from error order by error_date desc limit 50");
                        int count = 0;
                        java.sql.Timestamp lastDate = null;

                        while(rs.next()) {
                            if (count == 0) {
                                lastDate = rs.getTimestamp("error_date");
                            }
                            count++;
                        }
                        if (count > 0) {
                            System.out.println(String.format("Total=%d, retrieved=%d, lastDate=%s", totalCount, count, lastDate));
                        }
                        System.out.println();
                    }
                    catch(SQLException e) {
                        readIssues++;
                        System.err.println(e.getMessage());
                    }
                }
            }

        }
        catch(SQLException e) {
            // if the error message is "out of memory",
            // it probably means no database file is found
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try  {
                if(connection != null) {
                    connection.close();
                }
            }
            catch(SQLException e) {
                // connection close failed.
                System.err.println(e.getMessage());
            }
        }
    }
}
