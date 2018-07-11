import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;


public class FrequentWordsFinder {

    private final WordsFrequency wordsFrequency = new WordsFrequency();

    public List<Dataset<Row>> find(final List<String> books, final SparkSession sparkSession) {
        List<Dataset<String>> wordsFromAllLanguages = wordsFrequency.extractBooksWords(books, sparkSession);
        return wordsFrequency.frequentWordsForAllLanguages(wordsFromAllLanguages);
    }
}
