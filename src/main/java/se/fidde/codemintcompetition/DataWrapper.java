package se.fidde.codemintcompetition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DataWrapper {

    private String year;
    private Collection<Double> tempCollection;

    public DataWrapper(String year) {
        super();
        this.year = year;
        tempCollection = new ArrayList<Double>();
    }

    public double getAvgTemperature() {
        ToDoubleFunction<Double> mapper = temp -> {
            return temp.doubleValue();
        };

        Collector<Double, ?, Double> avgDouble = Collectors
                .averagingDouble(mapper);
        Double avg = tempCollection.parallelStream().collect(avgDouble);

        return avg.doubleValue();
    }

    public double getMinTemperature() {
        // TODO: implement

        return 0;
    }

    public double getMaxTemperature() {
        // TODO: implement

        return 0;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Collection<Double> getTemperatureCollection() {
        return tempCollection;
    }

    public void setTemperatureCollection(
            Collection<Double> temperatureCollection) {
        this.tempCollection = temperatureCollection;
    }

    public static DataWrapper getEmptyWrapper() {
        return new DataWrapper("0");
    }

    @Override
    public String toString() {
        return "DataWrapper [year=" + year + ", temperatureCollection="
                + tempCollection + "]";
    }

}
