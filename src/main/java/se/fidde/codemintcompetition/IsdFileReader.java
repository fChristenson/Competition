package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class IsdFileReader {

    private static File errorOutputFile;
    private static boolean errorFlag;
    private static File lastFileLogged;

    public static void setErrorOutputFile(File file) {
        errorOutputFile = file;
    }

    public static List<IntSummaryStatistics> getDataForEachGzipFile(File folder) {
        File[] listFiles = folder.listFiles();
        List<File> files = Arrays.asList(listFiles);
        Predicate<? super File> gzFilter = f -> Pattern.matches(".+\\.gz",
                f.getName());

        Function<File, IntSummaryStatistics> mapper = getDataFromFile();

        List<IntSummaryStatistics> dataFromFiles = files.parallelStream()
                .filter(gzFilter).map(mapper)
                .filter(intSum -> intSum.getCount() > 0)
                .collect(Collectors.toList());

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

                IntSummaryStatistics result = new IntSummaryStatistics();
                br.lines().parallel().forEach(str -> {
                    try {
                        String yearString = str.substring(0, 5).trim();
                        int year = Integer.valueOf(yearString);

                        String tempString = str.substring(14, 19).trim();
                        int temp = Integer.valueOf(tempString);

                        isValidRow(year, temp);
                        result.accept(temp);

                    } catch (Exception e) {
                        if (errorOutputFile != null) {
                            writeErrorToFile(file);
                            lastFileLogged = file;
                        }
                        errorFlag = true;
                    }
                });

                br.close();
                if (errorFlag) {
                    errorFlag = false;
                    return new IntSummaryStatistics();
                }

                return result;

            } catch (Exception e) {
                e.printStackTrace();
                return new IntSummaryStatistics();
            }

        };
    }

    private static void isValidRow(int year, int temp) throws Exception {
        int errorInt = -9999;
        if (year == errorInt || temp == errorInt)
            throw new Exception(String.format("Year: %s, temp: %s", year, temp));

        else if (temp > 1000 || temp < -1000)
            throw new Exception("invalid temp: " + temp);

    }

    private static void writeErrorToFile(File file) {
        if (file == lastFileLogged)
            return;

        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {
            fileWriter = new FileWriter(errorOutputFile, true);
            bufferedWriter = new BufferedWriter(fileWriter);

            bufferedWriter.write(file.getName());
            bufferedWriter.newLine();

        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            try {
                if (bufferedWriter != null)
                    bufferedWriter.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

}
