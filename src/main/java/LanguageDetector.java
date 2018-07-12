import bean.LanguageDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;


public class LanguageDetector {

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("LanguageDetector")
                .config("spark.sql.parquet.binaryAsString", "true").getOrCreate();

        //read the freq words for each language
        final String freqWordENPath = args[0];
        final String freqWordSPPath = args[1];
        final String freqWordITPath = args[2];
        final String freqWordFRPath = args[3];
        final String bookTitlePath = args[4];

        final Dataset<Row> freqWordsEN = ParquetReader.read(freqWordENPath, sparkSession);
        final Dataset<Row> freqWordsFR = ParquetReader.read(freqWordFRPath, sparkSession);
        final Dataset<Row> freqWordsIT = ParquetReader.read(freqWordITPath, sparkSession);
        final Dataset<Row> freqWordsSP = ParquetReader.read(freqWordSPPath, sparkSession);
        final List<String> wordsTestedBook = BookReader.read(bookTitlePath);

        List<List<LanguageDescription>> freqForAllLanguages = new ArrayList<>();
        freqForAllLanguages.add(freqWordsEN.as(Encoders.bean(LanguageDescription.class)).collectAsList());
        freqForAllLanguages.add(freqWordsSP.as(Encoders.bean(LanguageDescription.class)).collectAsList());
        freqForAllLanguages.add(freqWordsIT.as(Encoders.bean(LanguageDescription.class)).collectAsList());
        freqForAllLanguages.add(freqWordsFR.as(Encoders.bean(LanguageDescription.class)).collectAsList());

        String languageOfBook = LanguageDeterminer.determine(wordsTestedBook, freqForAllLanguages);

        System.out.println("Language of the book: " + languageOfBook);
    }
}
