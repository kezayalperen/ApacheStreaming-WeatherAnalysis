import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class TimeOperator {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().appName("TimeOperator").master("local").getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "8000")
                .option("includeTimestamp", true)
                .load();

        // Bebek bezi 123123123123 -> 27.05.2020 12:31

        Dataset<Row> products = rawData.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("product", "timestamp");

        Dataset<Row> resultData = products.groupBy(functions.window(products.col("timestamp"), "1 minute"),
                products.col("product")).count().orderBy("window");

        StreamingQuery start = resultData.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        start.awaitTermination();


    }
}
