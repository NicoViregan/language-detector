import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;


public class CreateWordsBase {

    public void create(List<String> bookTitles){

        WordsOccurence w = new WordsOccurence();
        List<Dataset<String>> wordsFromAllLanguages =  w.wordsAllLanguages(bookTitles);

        List<Dataset<Row>> commonWordsAllLanguages = w.mostCommonWordsAllLanguages(wordsFromAllLanguages);

        WriteToParquet p = new WriteToParquet();
        p.writeParquet(commonWordsAllLanguages);
    }
}
