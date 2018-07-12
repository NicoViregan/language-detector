import bean.LanguageDescription;

import java.util.List;


public class LanguageDeterminer {

    public static String determine(final List<String> wordsTestedBook,
            final List<List<LanguageDescription>> commonWordsForAllLanguages) {
        double[] probabilityForAllLanguages = LanguageFinder.find(wordsTestedBook, commonWordsForAllLanguages);
        String testedBookLanguage = "unknown";
        double maxProbability = 0.0;

        if (maxProbability < probabilityForAllLanguages[0]) {
            testedBookLanguage = "English";
            maxProbability = probabilityForAllLanguages[0];
        }
        if (maxProbability < probabilityForAllLanguages[1]) {
            testedBookLanguage = "Spanish";
            maxProbability = probabilityForAllLanguages[1];
        }
        if (maxProbability < probabilityForAllLanguages[2]) {
            testedBookLanguage = "Italian";
            maxProbability = probabilityForAllLanguages[2];
        }
        if (maxProbability < probabilityForAllLanguages[3]) {
            testedBookLanguage = "French";
        }
        return testedBookLanguage;
    }
}
