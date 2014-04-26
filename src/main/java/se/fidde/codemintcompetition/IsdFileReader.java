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

/**
 * Static helper class for getting data from gzip files and writing out errors
 * to file.
 * 
 * @author Fidde
 *
 */
public class IsdFileReader {

    private static File errorOutputFile;
    private static boolean errorFlag;
    private static File lastFileLogged;

    public static void setErrorOutputFile(File file) {
        errorOutputFile = file;
    }

    /**
     * Takes a root folder containing ISD-lite formated gzip files and retrives
     * a list of {@link java.util.IntSummaryStatistics}. The data is retrieved
     * using parallel streams.
     * 
     * @param folder
     *            folder with ISD-lite gzip files
     * @return list of IntSummaryStatistics
     */
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

                            /*
                             * Could´nt find a way to stop iteration while using
                             * streams so i set a flag and if an error occurs i
                             * return an empty IntSummaryStatistics object. Once
                             * all the data is gathered i filter out the empty
                             * objects.
                             */
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
        // since iteration can´t stop i make sure only new file errors are
        // written to file
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
