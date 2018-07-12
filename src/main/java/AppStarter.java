import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;


public class AppStarter {

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("LanguageDetector")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();

        List<String> booksList = new ArrayList<>();
        booksList.add("src/main/resources/bookEn.txt");
        booksList.add("src/main/resources/bookSp.txt");
        booksList.add("src/main/resources/bookIt.txt");
        booksList.add("src/main/resources/bookFr.txt");

        FrequentWordsFinder finder = new FrequentWordsFinder();
        final List<Dataset<Row>> freqWordsForAllLanguages = finder.find(booksList, sparkSession);
        ParquetWriter.write(freqWordsForAllLanguages);
    }
}