import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;


public class ParquetWriter {

    public static void write(final List<Dataset<Row>> freqWordsOfAllLanguages) {
        Dataset<Row> dsLanguage = freqWordsOfAllLanguages.get(0);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/" + "commonWordsEn.txt");

        dsLanguage = freqWordsOfAllLanguages.get(1);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/" + "commonWordsSp.txt");

        dsLanguage = freqWordsOfAllLanguages.get(2);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/" + "commonWordsIt.txt");

        dsLanguage = freqWordsOfAllLanguages.get(3);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/" + "commonWordsFr.txt");
    }
}
