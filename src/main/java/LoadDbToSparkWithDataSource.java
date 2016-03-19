import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gangadharkadam on 3/17/16.
 * Project Name: Spark-JDBC}
 */

public class LoadDbToSparkWithDataSource {

    // Define the Logger
    private static final Logger LOGGER = Logger.getLogger(LoadDbToSparkWithDataSource.class);

    // Define MySQL connection parameters constants
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "hive";
    private static final String MYSQL_PWD = "hive";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

    // Define the spark context
    private static final JavaSparkContext sc =
            new JavaSparkContext(
                    new SparkConf()
                            .setMaster("local[*]")
                            .setAppName("LoadDbToSparkWithDataSource")

            );

    // Initialize the SQLContext from SparkContext
    private static final SQLContext sqlContext = new SQLContext(sc);


    public static void main(String[] args) {

        // JDBC data source options for MySQL database
        Map<String, String> options = new HashMap<>();
        options.put("drive", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",
                "(select emp_no, concat_ws('', first_name, last_name) " +
                        "as full_name from employees) as employee_name");
        options.put("partitionColumn","emp_no");
        options.put("lowerBound", "10001");
        options.put("upperBound", "499999");
        options.put("numPartitions", "10");

        // Load MySql query result as DataFrame
        DataFrame jdbcDF = sqlContext.load("jdbc",options);


        // Executing DataFrame by invoking an action
        List<Row> employeeFullNameRows = jdbcDF.collectAsList();
        for (Row employeeFullNameRow: employeeFullNameRows) {
            LOGGER.info(employeeFullNameRow);
        }

    }

}
