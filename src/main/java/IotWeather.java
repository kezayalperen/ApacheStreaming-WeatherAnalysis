import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class IotWeather {
    public static void main(String[] args) throws StreamingQueryException {

        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("SparkStreamingMessageListener").getOrCreate();

        StructType weatherType = new StructType().add("quarter","String")
                                                 .add("heatType","String")
                                                 .add("heat","integer")
                                                 .add("windType","String")
                                                 .add("wind","integer");

        Dataset<Row> rawData = sparkSession.readStream().schema(weatherType).option("sep", ",").csv("C:\\Users\\Kezay\\Desktop\\SparkStreaming\\*");

        Dataset<Row> heatData = rawData.select(rawData.col("*")).where(rawData.col("windType").equalTo("KD"));

        StreamingQuery start = heatData.writeStream().outputMode("append").format("console").start();

        start.awaitTermination();

    }
}
