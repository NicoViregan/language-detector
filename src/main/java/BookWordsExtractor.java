import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.List;


public class BookWordsExtractor {

    public Dataset<String> extract(final String book, final SparkSession sparkSession) {
        List<String> words = BookReader.read(book);

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        final JavaRDD<String> wordsRDD = jsc.parallelize(words);

        return sparkSession.sqlContext().createDataset(wordsRDD.rdd(), Encoders.STRING()).as("value");
    }
}