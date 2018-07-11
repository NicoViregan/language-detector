import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.lit;


public class WordsOccurence {

    public Dataset<String> wordsInLanguage(String bookTitle) {
        ReadBook r = new ReadBook();
        List<String> words = r.readBook(bookTitle);

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("LanguageDetector")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        final JavaRDD<String> wordsRDD = jsc.parallelize(words);

        Dataset<String> wordsLanguage =
                sparkSession.sqlContext().createDataset(wordsRDD.rdd(), Encoders.STRING()).as("value");

        return wordsLanguage;
    }

    public List<Dataset<String>> wordsAllLanguages(List<String> bookTitles) {

        String bookTitleEn = bookTitles.get(0);
        Dataset<String> wordsEn = wordsInLanguage(bookTitleEn);

        String bookTitleSp = bookTitles.get(1);
        Dataset<String> wordsSp = wordsInLanguage(bookTitleSp);

        String bookTitleIt = bookTitles.get(2);
        Dataset<String> wordsIt = wordsInLanguage(bookTitleIt);

        String bookTitleFr = bookTitles.get(3);
        Dataset<String> wordsFr = wordsInLanguage(bookTitleFr);

        List<Dataset<String>> languageDs = new ArrayList<>();

        languageDs.add(wordsEn);
        languageDs.add(wordsSp);
        languageDs.add(wordsIt);
        languageDs.add(wordsFr);

        return languageDs;
    }

    public Dataset<Row> mostCommonWordsInLanguage(Dataset<String> languageDs) {

        int procentage = 25;

        Dataset<Row> initializedWordsDS = languageDs.withColumn("frequency", lit(1));
        final Dataset<Row> countedWordsDs =
                initializedWordsDS.groupBy(initializedWordsDS.col("value")).sum("frequency");
        final long wordsNumber = countedWordsDs.count();

        int numberOfRowsChosen = procentage * (int) wordsNumber / 100;
        final Dataset<Row> result =
                initializedWordsDS.orderBy(initializedWordsDS.col("frequency").desc()).limit(numberOfRowsChosen);

        return result;
    }

    public List<Dataset<Row>> mostCommonWordsAllLanguages(List<Dataset<String>> languagesDs) {

        int languagesNumber = 4;
        List<Dataset<Row>> mostCommonWords = new ArrayList<>();
        for (int i = 0; i < languagesNumber; ++i) {

            Dataset<String> currentDs = languagesDs.get(i);
            mostCommonWords.add(mostCommonWordsInLanguage(currentDs));
        }

        return mostCommonWords;
    }

}

