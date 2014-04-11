package se.fidde.codemintcompetition;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public class ProcessedPost {

    private String fileNameString;
    private List<ISDRow> rows;

    public ProcessedPost(String fileNameString, List<ISDRow> rows) {
        super();
        this.fileNameString = fileNameString;
        this.rows = rows;
    }

    public ProcessedPost(String fileNameString) {
        super();
        this.fileNameString = fileNameString;
    }

    public int getMinTemp() {
        Comparator<ISDRow> comparator = getComparator();
        Optional<ISDRow> result = rows.parallelStream().collect(
                Collectors.minBy(comparator));

        return result.get().getTemp();
    }

    private Comparator<ISDRow> getComparator() {
        Comparator<ISDRow> comparator = (row, row2) -> {
            int intValue = row.getTemp();
            int intValue2 = row2.getTemp();

            if (intValue == intValue2)
                return 0;

            else if (intValue < intValue2)
                return -1;

            return 1;
        };
        return comparator;
    }

    public int getMaxTemp() {
        Comparator<ISDRow> comparator = getComparator();
        Optional<ISDRow> result = rows.parallelStream().collect(
                Collectors.maxBy(comparator));

        return result.get().getTemp();
    }

    public int getAvgTemp() {
        ToIntFunction<ISDRow> mapper = row -> {
            return row.getTemp();
        };
        Double result = rows.parallelStream().collect(
                Collectors.averagingInt(mapper));

        return result.intValue();
    }

    public String getFileNameString() {
        return fileNameString;
    }

    public void setFileNameString(String fileNameString) {
        this.fileNameString = fileNameString;
    }

    public List<ISDRow> getRows() {
        return rows;
    }

    public void setRows(List<ISDRow> rows) {
        this.rows = rows;
    }

}
