import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.List;


public class AppStarter {

    public static void main(String[] args) {
        List<String> bookList = new ArrayList<>();
        bookList.add("bookEn.txt");
        bookList.add("bookSp.txt");
        bookList.add("bookIt.txt");
        bookList.add("bookFr.txt");

        CreateWordsBase c = new CreateWordsBase();
        c.create(bookList);
    }

}
