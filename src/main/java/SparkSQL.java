import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.*;

/***
 * My idea is dump the log into Spark and partition by IP and time. Then use Hive-on-spark to run whatever query to get
 * whatever result
 */
public class SparkSQL {

  // this regex to divide the log entry, I also split client IP and port
  private static Pattern pattern =
      Pattern.compile("^(.{27}) ([^ ]*) ([^ ]{7,15}):([^ ]{1,5}) ([^ ]{1,20}) ([^ ]{1,20}) ([^ ]{1,20}) ([^ ]{1,20}) " +
          "(\\d{1,3}) (\\d{1,3}) (\\d{1,10}) (\\d{1,10}) (\".*HTTP.{4}\") (\"[^\"]*\") ([^ ]*) ([^ ]*)$");

  private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

  public static void main (String[] args) {

    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQL");
    JavaSparkContext sc = new JavaSparkContext(conf);

    HiveContext hiveContext = new HiveContext(sc);
    hiveContext.setConf("hive.server2.thrift.port", "10002");


    SparkSession sparkSession = SparkSession
        .builder()
        .master("local")
        .appName("Java Spark SQL basic example")
        .getOrCreate();

    StructType schema = new StructType(new StructField[]{
        new StructField("ts", LongType, false, Metadata.empty()),
        new StructField("elb", StringType, false, Metadata.empty()),
        new StructField("client_ip", StringType, false, Metadata.empty()),
        new StructField("client_port", StringType, false, Metadata.empty()),
        new StructField("backend_ip_and_port", StringType, false, Metadata.empty()),
        new StructField("request_processing_time", StringType, false, Metadata.empty()),
        new StructField("backend_processing_time", StringType, false, Metadata.empty()),
        new StructField("response_processing_time", StringType, false, Metadata.empty()),
        new StructField("elb_status_code", StringType, false, Metadata.empty()),
        new StructField("backend_status_code", StringType, false, Metadata.empty()),
        new StructField("received_bytes", StringType, false, Metadata.empty()),
        new StructField("sent_bytes", StringType, false, Metadata.empty()),
        new StructField("request", StringType, false, Metadata.empty()),
        new StructField("user_agent", StringType, false, Metadata.empty()),
        new StructField("ssl_cipher", StringType, false, Metadata.empty()),
        new StructField("ssl_protocol", StringType, false, Metadata.empty()),
        new StructField("quarter", StringType, true, Metadata.empty())
    });

    JavaRDD<String> textFile = sc.textFile(ClassLoader.getSystemClassLoader()
        .getResource("2015_07_22_mktplace_shop_web_log_sample.log.gz").getPath(), 1);


    JavaRDD<Row> rowRDD = textFile.map((String array) -> {
      Matcher matcher = pattern.matcher(array);
      Object[] parsedArray = new Object[matcher.groupCount() + 1];
      while (matcher.find()) {
        for (int i = 1; i <= matcher.groupCount(); i++) {
          if (i == 1) {
            String ts = matcher.group(i);
            parsedArray[0] = LocalDateTime.from(formatter.parse(ts)).toEpochSecond(ZoneOffset.UTC);
            int min = Integer.valueOf(ts.substring(14, 16));
            String quarter = null;

            // I tried a few times and 15 min seems legit, from bare eye-spotting.
            // Most clients are pretty much just did one time visit.
            // However this is an open implementation. Other factors could be user agent and request url relations
            // I would recommend to use a graph database like new4j to get the best session definition
            if (0 <= min && min < 15) {
              quarter = "1";
            } else if (15 <= min && min < 30) {
              quarter = "2";
            } else if (30 <= min && min < 45) {
              quarter = "3";
            } else if (45 <= min && min < 60) {
              quarter = "4";
            } else {
              System.out.println(" invalid min: " + min);
              System.exit(2);
            }
            parsedArray[matcher.groupCount()] = ts.substring(0, 13) + "Q" + quarter;
          } else {
            parsedArray[i - 1] = matcher.group(i);
          }
        }
      }
      return new GenericRowWithSchema(parsedArray, schema);
    });

    Dataset<Row> dataset = sparkSession.createDataFrame(rowRDD, schema);

//    // enable this if you want to writes session files into a specific path
//    dataset.write().partitionBy("client_ip", "quarter")
//        .csv(ClassLoader.getSystemClassLoader().getResource(".").getPath() + "output");

    // this create a local spark-warehouse with the session data partitioned
    dataset.write().partitionBy("client_ip", "quarter").saveAsTable("parsed_data");

    //average session_time
    System.out.println("average session_time");
    hiveContext.sql(
        "SELECT avg(session_time) as average_session_time from (" +
              "SELECT (MAX(ts) - MIN(ts)) as session_time, client_ip, quarter " +
              "FROM parsed_data GROUP BY client_ip, quarter)").show();

    //unique URL visits per session
    System.out.println("unique URL visits per session");
    hiveContext.sql("SELECT DISTINCT request as dr, client_ip, quarter " +
        "FROM parsed_data GROUP BY client_ip, quarter, dr").show();

    //most engaged users
    hiveContext.sql(
            "SELECT (MAX(ts) - MIN(ts)) as session_time, client_ip " +
            "FROM parsed_data GROUP BY client_ip ORDER BY session_time DESC").show();
  }
}
