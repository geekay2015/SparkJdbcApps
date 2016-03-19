import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

/**
 * Created by gangadharkadam on 3/18/16.
 * Project Name: $(PROJECT_NAME}
 */

public class LoadSparkToDB implements Serializable{
    // Define the Logger
    private static final Logger LOGGER = Logger.getLogger(LoadSparkToDB.class);

    // Define MySQL connection parameters constants
    //private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
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

    public static void main (String[] args) {

        // Sample data-frame loaded from a JSON file
        // /Users/gangadharkadam/myapps/SparkJdbcApps/world_bank.json
        DataFrame worldBankDf = sqlContext.jsonFile("/Users/gangadharkadam/myapps/SparkJdbcApps/src/main/resources/users.json");
        worldBankDf.printSchema();

        //Save data-frame to MySQL
        worldBankDf.createJDBCTable(MYSQL_CONNECTION_URL,"users",true);

    }
}
