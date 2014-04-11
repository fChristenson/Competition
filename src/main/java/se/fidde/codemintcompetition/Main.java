package se.fidde.codemintcompetition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        validateInputArgs(args);

        File folderToScan = new File(args[0]);
        validateFolderToScan(folderToScan);

        System.out.println("Processing...");

        List<File> folders = getSubFolders(folderToScan);
        ForkJoinPool pool = new ForkJoinPool();
        Collection<DataWrapper> result = new ArrayList<DataWrapper>();

        folders.forEach(file -> {
            File[] listFiles = file.listFiles();
            List<File> filterList = filterFiles(listFiles);

            GzipHandler gzipHandler = new GzipHandler(filterList);
            DataWrapper dataFromFolder = pool.invoke(gzipHandler);

            // TODO: use filters to get only the relevant entry
            result.add(dataFromFolder);
        });

        try {
            writeDataToFile(result, args[1]);

            System.out.println("Processing done.");
            System.out.println("Shutting down.");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private static List<File> getSubFolders(File folderToScan) {
        File[] listFiles = folderToScan.listFiles();
        List<File> folders = filterFolders(listFiles);

        return folders;
    }

    private static List<File> filterFolders(File[] listFiles) {
        List<File> asList = Arrays.asList(listFiles);
        Predicate<? super File> predicate = file -> {
            if (file.isDirectory())
                return true;

            return false;
        };
        List<File> result = asList.stream().filter(predicate)
                .collect(Collectors.toList());

        return result;
    }

    private static void writeDataToFile(Collection<DataWrapper> result,
            String fileName) throws IOException {
        File file = new File(fileName);
        FileWriter fileWriter = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        result.forEach(dataWrapper -> {
            try {
                writeYear(bufferedWriter, dataWrapper);
                writeMinTemperature(bufferedWriter, dataWrapper);
                writeAvgTemperature(bufferedWriter, dataWrapper);
                writeMaxTemperature(bufferedWriter, dataWrapper);

            } catch (Exception e) {
                e.printStackTrace();

            } finally {
                try {
                    bufferedWriter.close();
                    fileWriter.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void writeMaxTemperature(BufferedWriter bufferedWriter,
            DataWrapper dataWrapper) throws IOException {

        String maxTemperature = dataWrapper.getMaxTemperature();
        String format = String.format("Max temp: %s Celcius", maxTemperature);

        bufferedWriter.write(format);
        bufferedWriter.newLine();
    }

    private static void writeMinTemperature(BufferedWriter bufferedWriter,
            DataWrapper dataWrapper) throws IOException {

        String minTemperature = dataWrapper.getMinTemperature();
        String format = String.format("Min temp: %s Celcius", minTemperature);

        bufferedWriter.write(format);
        bufferedWriter.newLine();
    }

    private static void writeAvgTemperature(BufferedWriter bufferedWriter,
            DataWrapper dataWrapper) throws IOException {

        String avgTemperature = dataWrapper.getAvgTemperature();
        String format = String.format("Avg temp: %s Celcius", avgTemperature);

        bufferedWriter.write(format);
        bufferedWriter.newLine();
    }

    private static void writeYear(BufferedWriter bufferedWriter,
            DataWrapper dataWrapper) throws IOException {
        String year = dataWrapper.getYear();
        bufferedWriter.write(year);
        bufferedWriter.newLine();
    }

    private static void validateFolderToScan(File folderToScan) {
        if (!folderToScan.exists() || !folderToScan.isDirectory()
                || !folderToScan.canRead()) {
            System.out.println("Provided folder is not valid");
            System.out
                    .println("Please make sure that the path is correct and you have read/write access");
            System.exit(0);
        }
    }

    private static void validateInputArgs(String[] args) {
        if (args.length > 3 || args.length < 2) {
            System.out.println("Invalid arguments format");
            System.out
                    .println("Please use: <folder to scan> <outputfile> <optional:invalid posts outputfile>");
            System.exit(0);
        }
    }

    private static List<File> filterFiles(File[] listFiles) {
        List<File> asList = Arrays.asList(listFiles);
        Predicate<? super File> predicate = file -> {
            if (file.isDirectory()
                    || !file.getName().matches("\\d{6}-\\d{5}-\\d{4}.gz"))
                return false;

            return true;
        };
        List<File> collect = asList.stream().filter(predicate)
                .collect(Collectors.toList());

        return collect;
    }
}
