import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class Main  {
    private static final int QUERY_TIMEOUT = 30;
    private static final int TOTAL_ITERATIONS = 500;

    private static final int MESSAGE_CHAR_LENGTH = 1000;
    private static final int SELECT_EVERY_ITERATION = 2;
    private static final int SELECT_LIMIT = 100;

    private static final int CONSECUTIVE_INSERTS = 5;
    private static final int INSERTS_PER_MINUTE = 1000;

    private static final int SLEEP_VARIANCE = 25;
    private static final int SLEEP_AVERAGE = (int) (0.5 * ((60 * 1000) / (INSERTS_PER_MINUTE)) * CONSECUTIVE_INSERTS);

    private static final int SLEEP_LOWER_BOUND = SLEEP_AVERAGE - SLEEP_VARIANCE;
    private static final int SLEEP_UPPER_BOUND = SLEEP_AVERAGE + SLEEP_VARIANCE;



    private static final String STACK_TRACE = new String(new char[MESSAGE_CHAR_LENGTH]).replace("\0", "A");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static int getRandomInt(int lower, int upper) {
        return lower + (new Random()).nextInt(upper - lower);
    }

    private static void log(String message) {
        System.out.println(LocalDateTime.now().format(DATE_TIME_FORMATTER) + " - " + message);
    }

    private static int getTotalCount(Statement statement) throws SQLException {
        ResultSet rsRecordsCount = statement.executeQuery("select count(*) from error");
        rsRecordsCount.next();
        return rsRecordsCount.getInt(1);
    }

    public static void main(String[] args) {
        final String PROCESS_ID = java.util.UUID.randomUUID().toString();
        log(String.format("SLEEP_AVERAGE=%d", SLEEP_AVERAGE));
        final long t1 = System.currentTimeMillis();
        log(String.format("start process=%s", PROCESS_ID));

        Connection connection = null;

        int readIssues = 0;
        int writeIssues = 0;
        int totalCount = 0;
        int initialCount = 0;
        int diffCount = 0;
        int processCount = 0;

        try {
            connection = DriverManager.getConnection("jdbc:sqlite:example.db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(QUERY_TIMEOUT);

            statement.executeUpdate("create table if not exists error (process_id string, error_id string, stacktrace string, error_date datetime)");

            for (int i=1; i<=TOTAL_ITERATIONS; i++) {
                try {
                    totalCount = getTotalCount(statement);
                    if (initialCount == 0) {
                        initialCount = totalCount;
                    }
                    diffCount = totalCount - initialCount;
                }
                catch (SQLException e) {
                    readIssues++;
                    System.err.println(e.getMessage());
                }
                long tNow = System.currentTimeMillis();
                log(String.format("### iteration=%03d, elapsed=%d, totalCount=%d, processCount=%d, diffCount=%d, readIssues=%d, writeIssues=%d", i, (tNow - t1), totalCount, processCount, diffCount, readIssues, writeIssues));

                for (int j=0; j<CONSECUTIVE_INSERTS; j++) {
                    String uuid = java.util.UUID.randomUUID().toString();
                    try {
                        statement.executeUpdate(String.format("insert into error values('%s', '%s', '%s', '%s')", PROCESS_ID, uuid, STACK_TRACE, LocalDateTime.now().format(DATE_TIME_FORMATTER)));
                    } catch (SQLException e) {
                        writeIssues++;
                        System.err.println(e.getMessage());
                    }
                    processCount++;
                }
                int sleepMillis = getRandomInt(SLEEP_LOWER_BOUND, SLEEP_UPPER_BOUND);
                log(String.format("# sleep=%d", sleepMillis));
                Thread.sleep(sleepMillis);
                log("# done sleeping");

                if (i % SELECT_EVERY_ITERATION == 1) {
                    try {
                        ResultSet rs = statement.executeQuery("select * from error order by error_date desc limit " + SELECT_LIMIT);
                        int count = 0;
                        java.sql.Timestamp lastDate = null;

                        while(rs.next()) {
                            if (count == 0) {
                                lastDate = rs.getTimestamp("error_date");
                            }
                            count++;
                        }
                        if (count > 0) {
                            log(String.format(">>> retrieved=%d, lastDate=%s", count, lastDate));
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

        final long t2 = System.currentTimeMillis();
        final long tDiff = t2 - t1;
        log(String.format("end process=%s, elapsed=%d", PROCESS_ID, tDiff));

    }
}
