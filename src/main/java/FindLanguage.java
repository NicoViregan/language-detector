import org.apache.spark.sql.Dataset;

import java.util.List;

import static org.apache.spark.sql.functions.sum;


public class FindLanguage {

    public void find(String testedBookTitle, List<String> dataBooks) {

        int numberLanguages = 4;

        BookReader r = new BookReader();
//        List<String> wordsTestedBook = r.readBook(testedBookTitle);

       WordsFrequency w = new WordsFrequency();
//        List<Dataset<String>> wordsSet = w.extractBooksWords(dataBooks);

        double probability[] = { 0.0, 0.0, 0.0, 0.0, 0.0 };
        int testesBookIndex = 0;

        while (probability[0] < 95 && probability[1] < 95 && probability[2] < 95 && probability[3] < 95) {

//            String currentWord = wordsTestedBook.get(testesBookIndex);
//            double[] currentProbability = wordProbability(currentWord, wordsSet);
//
//            for (int i = 0; i < numberLanguages; ++i) {
//                if (probability[i] != 0.0)
//                    probability[i] = (probability[i] + currentProbability[i]) / 2;
//                else
//                    probability[i] = currentProbability[i];
//            }
        }

    }

    public double[] wordProbability(String currentWord, List<Dataset<String>> wordsSet) {

        int numberLanguages = 4;
        double currentWordProbability[] = { 0.0, 0.0, 0.0, 0.0, 0.0 };

        for (int i = 0; i < numberLanguages; ++i) {

//            Dataset<String> wordsDs = wordsSet.get(i);
//            final Dataset<String> filteredDs = wordsDs.select(wordsDs.col("value"));
//            final Dataset<> resultFilter =  filteredDs.select(filteredDs.col("value")(currentWord);
//            long wordsAppearanceNumber =  resultFilter.filter(resultFilter.col("value")).agg(sum(resultFilter.col("value")));
//
//            long allWordsInLanguage = wordsDs.count();
//            currentWordProbability[i] = wordsAppearanceNumber * 100 / allWordsInLanguage ;
        }

        return currentWordProbability;
    }


}
