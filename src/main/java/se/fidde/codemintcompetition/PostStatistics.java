package se.fidde.codemintcompetition;

import java.util.IntSummaryStatistics;

/**
 * Pojo to hold data from each folder.
 * 
 * @author Fidde
 *
 */
public class PostStatistics {
    private String year;
    private IntSummaryStatistics statistics;

    public PostStatistics(String year, IntSummaryStatistics statistics) {
        this.setYear(year);
        this.statistics = statistics;
    }

    public IntSummaryStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(IntSummaryStatistics statistics) {
        this.statistics = statistics;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return "PostStatistics [year=" + year + ", statistics=" + statistics
                + "]";
    }

}
