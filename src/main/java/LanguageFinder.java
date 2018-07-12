import bean.LanguageDescription;
import java.util.List;
import java.util.stream.Collectors;

public class LanguageFinder {

    public static double[] find(final List<String> wordsTestedBook, final List<List<LanguageDescription>> commonWordsForAllLanguages) {
        int numberLanguages = 4;
        double probability[] = { 0.0, 0.0, 0.0, 0.0, 0.0 };
        int testedBookIndex = 0;

        while (probability[0] < 60 && probability[1] < 60 && probability[2] < 60 && probability[3] < 60
                && testedBookIndex < wordsTestedBook.size()) {

            String currentWordFromBook = wordsTestedBook.get(testedBookIndex);
            double[] currentProbability = wordProbability(currentWordFromBook, commonWordsForAllLanguages);

            for (int i = 0; i < numberLanguages; ++i) {
                probability[i] = (probability[i] * testedBookIndex + currentProbability[i])/(testedBookIndex + 1);
            }
            ++testedBookIndex;
        }
        return probability;
    }

    public static double[] wordProbability(String currentWord, List<List<LanguageDescription>> commonWordsFromAllLanguages) {
        int numberLanguages = 4;
        double currentWordProbability[] = { 0.0, 0.0, 0.0, 0.0, 0.0 };

        for (int i = 0; i < numberLanguages; ++i) {
            List<LanguageDescription> commonWordsOneLanguage = commonWordsFromAllLanguages.get(i);
            List<LanguageDescription> foundWord = commonWordsOneLanguage.stream().filter(x -> x.getValue().equals(currentWord))
                    .collect(Collectors.toList());
            if (foundWord.size() == 1) {
                currentWordProbability[i] = foundWord.get(0).getFrequency();
            }

        }
        return currentWordProbability;
    }
}
