package se.fidde.codemintcompetition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        validateInputArgs(args);
        File folderToScan = getFolderToScan(args);

        System.out.println("Processing...");

        process(folderToScan, args[1]);

        System.out.println("Done.");
    }

    private static void process(File folderToScan, String outputFile) {
        List<File> folders = getSubFolders(folderToScan);
        ForkJoinPool pool = new ForkJoinPool();

        folders.forEach(file -> {
            File[] listFiles = file.listFiles();
            List<File> filterList = filterFiles(listFiles);

            GzipHandler gzipHandler = new GzipHandler(filterList);
            List<ProcessedPost> dataFromFolder = pool.invoke(gzipHandler);

            writeDataToFile(dataFromFolder, outputFile);
        });
    }

    private static void filterData(List<ProcessedPost> dataFromFolder) {
        Predicate<? super ProcessedPost> filter = post -> {
            return post.getRows() == null || post.getRows().size() == 0;
        };
        dataFromFolder.removeIf(filter);
    }

    private static File getFolderToScan(String[] args) {
        File folderToScan = new File(args[0]);
        validateFolderToScan(folderToScan);
        return folderToScan;
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

    private static void writeDataToFile(List<ProcessedPost> dataFromFolder,
            String fileName) {

        filterData(dataFromFolder);
        if (dataFromFolder.size() == 0)
            return;

        File file = new File(fileName);
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {
            fileWriter = new FileWriter(file, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            String valueString = getValues(dataFromFolder);

            bufferedWriter.write(valueString);
            bufferedWriter.newLine();

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            if (bufferedWriter != null)
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    private static String getValues(List<ProcessedPost> dataFromFolder) {
        String year = dataFromFolder.get(0).getRows().get(0).getYear();
        int max = dataFromFolder.parallelStream()
                .mapToInt(post -> post.getMaxTemp()).sum();

        int avg = dataFromFolder.parallelStream()
                .mapToInt(post -> post.getAvgTemp()).sum();

        int min = dataFromFolder.parallelStream()
                .mapToInt(post -> post.getMinTemp()).sum();

        int length = dataFromFolder.size();
        max = max / length;
        avg = avg / length;
        min = min / length;

        String format = String.format("%s;%s;%s;%s", year, max, avg, min);
        return format;
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
