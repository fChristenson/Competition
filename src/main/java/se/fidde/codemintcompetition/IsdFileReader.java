package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class IsdFileReader {

    public static List<IntSummaryStatistics> getDataForEachGzipFile(File file) {
        File[] listFiles = file.listFiles();
        List<File> files = Arrays.asList(listFiles);
        Predicate<? super File> gzFilter = f -> Pattern.matches(".+\\.gz",
                f.getName());

        Function<File, IntSummaryStatistics> mapper = getDataFromFile();

        List<IntSummaryStatistics> dataFromFiles = files.parallelStream()
                .filter(gzFilter).map(mapper).collect(Collectors.toList());
        return dataFromFiles;
    }

    @SuppressWarnings("resource")
    private static Function<File, IntSummaryStatistics> getDataFromFile() {
        return file -> {
            try {
                FileInputStream fs = new FileInputStream(file);
                GZIPInputStream gzs = new GZIPInputStream(fs);
                InputStreamReader isr = new InputStreamReader(gzs);
                BufferedReader br = new BufferedReader(isr);

                ToIntFunction<? super String> mapper = getRowMapper();
                IntSummaryStatistics result = br.lines().parallel()
                        .mapToInt(mapper).summaryStatistics();

                br.close();
                return result;

            } catch (Exception e) {
                e.printStackTrace();
                return new IntSummaryStatistics();
            }

        };
    }

    private static ToIntFunction<? super String> getRowMapper() {
        return string -> {
            String tempString = string.substring(14, 20).trim();
            return Integer.valueOf(tempString);
        };
    }

}
