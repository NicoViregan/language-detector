import org.apache.spark.sql.*;


public class ParquetReader {

    public static Dataset<Row> read(final String filePath, final SparkSession sparkSession) {
        return sparkSession.read().parquet(filePath);
    }
}
