package se.fidde.codemintcompetition;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

public class DataWrapper {

    private String year;
    private Collection<Double> tempCollection;

    public DataWrapper() {
        super();
        tempCollection = new ArrayList<Double>();
    }

    public String getAvgTemperature() {
        ToDoubleFunction<Double> mapper = temp -> {
            return temp.doubleValue();
        };

        OptionalDouble avg = tempCollection.parallelStream()
                .mapToDouble(mapper).average();

        DecimalFormat decimalFormat = new DecimalFormat("0.0");

        return decimalFormat.format(avg.getAsDouble() / 10);
    }

    public String getMinTemperature() {
        Comparator<Double> comparator = getDoubleComparator();
        Optional<Double> min = tempCollection.parallelStream().collect(
                Collectors.minBy(comparator));

        double result = min.get().doubleValue() / 10;
        return String.valueOf(result);
    }

    private Comparator<Double> getDoubleComparator() {
        Comparator<Double> comparator = (double1, double2) -> {
            double doubleValue = double1.doubleValue();
            double doubleValue2 = double2.doubleValue();

            if (doubleValue == doubleValue2)
                return 0;

            else if (doubleValue < doubleValue2)
                return -1;

            return 1;
        };
        return comparator;
    }

    public String getMaxTemperature() {
        Comparator<Double> comparator = getDoubleComparator();
        Optional<Double> max = tempCollection.parallelStream().collect(
                Collectors.maxBy(comparator));

        double result = max.get().doubleValue() / 10;
        return String.valueOf(result);
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
        return new DataWrapper();
    }

    @Override
    public String toString() {
        return "DataWrapper [year=" + year + ", temperatureCollection="
                + tempCollection + "]";
    }

}
