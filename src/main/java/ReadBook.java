import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class ReadBook {

    public List<String> readBook(final String bookTitle) {
        List<String> resultList = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(bookTitle));
            resultList = reader.lines().flatMap(x -> Stream.of(x.split(" ")))
                    .filter(x -> x.length() > 1).collect(Collectors.toList());



        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(resultList);
        return resultList;

    }

}
