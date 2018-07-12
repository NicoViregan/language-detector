package bean;

public class LanguageDescription {

    private String value;
    private double frequency;

    public LanguageDescription(String value, double frequency) {
        this.value = value;
        this.frequency = frequency;
    }

    public String getValue() {
        return value;
    }

    public double getFrequency() {
        return frequency;
    }
}
