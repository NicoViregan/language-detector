import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;


public class WriteToParquet {

    public void writeParquet(final List<Dataset<Row>> ds) {

        Dataset<Row> dsLanguage = ds.get(0);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/Documents" + "commonWordsEn.txt");

        dsLanguage = ds.get(1);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/Documents" + "commonWordsSp.txt");

        dsLanguage = ds.get(2);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/Documents" + "commonWordsIt.txt");

        dsLanguage = ds.get(3);
        dsLanguage.coalesce(1).write().parquet("/Users/nicoletav/Documents" + "commonWordsFr.txt");
    }

}
