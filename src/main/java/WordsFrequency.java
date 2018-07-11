import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.lit;


public class WordsFrequency {

    private final BookWordsExtractor extractor = new BookWordsExtractor();

    public List<Dataset<String>> extractBooksWords(final List<String> bookTitles, final SparkSession sparkSession) {
        String bookTitleEn = bookTitles.get(0);
        Dataset<String> wordsEn = extractor.extract(bookTitleEn, sparkSession);
        String bookTitleSp = bookTitles.get(1);
        Dataset<String> wordsSp = extractor.extract(bookTitleSp, sparkSession);
        String bookTitleIt = bookTitles.get(2);
        Dataset<String> wordsIt = extractor.extract(bookTitleIt, sparkSession);
        String bookTitleFr = bookTitles.get(3);
        Dataset<String> wordsFr = extractor.extract(bookTitleFr, sparkSession);

        List<Dataset<String>> languageDs = new ArrayList<>();
        languageDs.add(wordsEn);
        languageDs.add(wordsSp);
        languageDs.add(wordsIt);
        languageDs.add(wordsFr);

        return languageDs;
    }

    public List<Dataset<Row>> frequentWordsForAllLanguages(final List<Dataset<String>> allLanguagesWords) {
        int languagesNumber = 4;
        List<Dataset<Row>> mostCommonWords = new ArrayList<>();
        for (int i = 0; i < languagesNumber; ++i) {
            Dataset<String> currentLanguageDs = allLanguagesWords.get(i);
            mostCommonWords.add(frequentWordsInOneLanguage(currentLanguageDs));
        }
        return mostCommonWords;
    }

    public Dataset<Row> frequentWordsInOneLanguage(final Dataset<String> languageDs) {
        int percentage = 25;

        Dataset<Row> initializedWordsDS = languageDs.withColumn("frequency", lit(1));
        final Dataset<Row> countedWordsDs =
                initializedWordsDS.groupBy(initializedWordsDS.col("value")).sum("frequency");
        final long wordsNumber = countedWordsDs.count();

        int numberOfRowsChosen = percentage * (int) wordsNumber / 100;
        return initializedWordsDS.orderBy(initializedWordsDS.col("frequency").desc()).limit(numberOfRowsChosen);
    }
}