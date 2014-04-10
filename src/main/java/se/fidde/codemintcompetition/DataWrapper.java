package se.fidde.codemintcompetition;

public class DataWrapper {

    private String year;
    private double temperature;

    public DataWrapper(String year, double temperature2) {
        super();
        this.setYear(year);
        this.setTemperature(temperature2);
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature2) {
        this.temperature = temperature2;
    }
}
