package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class Main {

    private static File errorOutputFile;
    private static File outputFile;
    private static File rootFolder;
    private static FileVisitOption options;
    private static BiPredicate<Path, BasicFileAttributes> folder;
    private static Path start;

    public static void main(String[] args) throws IOException {
        validateInputArgs(args);
        instanciateParamFiles(args);

        System.out.println("Processing...");

        try {
            process();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println("Done.");
    }

    private static void instanciateParamFiles(String[] args) {
        rootFolder = new File(args[0]);
        outputFile = new File(args[1]);
        if (args.length == 3)
            errorOutputFile = new File(args[2]);
    }

    private static void process() throws IOException, URISyntaxException {

        validateFolderToScan();

        start = rootFolder.toPath();
        folder = getCorrectFolders();
        options = FileVisitOption.FOLLOW_LINKS;
        Function<Path, PostStatistics> dataPerFolder = getDataForEachFolder();

        List<PostStatistics> data = Files.find(start, 1, folder, options)
                .parallel().map(dataPerFolder).collect(Collectors.toList());

        // TODO: format data to strings and sort
        System.out.println(data);
    }

    private static BiPredicate<Path, BasicFileAttributes> getCorrectFolders() {
        return (path, attr) -> {
            return attr.isDirectory()
                    && Pattern.matches("\\d{4}", path.getFileName().toString());
        };
    }

    private static Function<Path, PostStatistics> getDataForEachFolder() {
        return path -> {
            File file = path.toFile();
            List<IntSummaryStatistics> summaryStatistics = getDataForEachGzipFile(file);
            IntSummaryStatistics statistics = getSumOfAllData(summaryStatistics);
            String year = path.getFileName().toString();

            return new PostStatistics(year, statistics);
        };
    }

    private static List<IntSummaryStatistics> getDataForEachGzipFile(File file) {
        File[] listFiles = file.listFiles();
        List<File> files = Arrays.asList(listFiles);
        Predicate<? super File> gzFilter = f -> Pattern.matches(".+\\.gz",
                f.getName());

        Function<File, IntSummaryStatistics> mapper = getDataFromFile();

        List<IntSummaryStatistics> dataFromFiles = files.parallelStream()
                .filter(gzFilter).map(mapper).collect(Collectors.toList());
        return dataFromFiles;
    }

    private static IntSummaryStatistics getSumOfAllData(
            List<IntSummaryStatistics> summaryStatistics) {

        IntSummaryStatistics result = new IntSummaryStatistics();
        summaryStatistics.parallelStream().forEach(sumary -> {
            result.combine(sumary);
        });

        return result;
    }

    private static Function<File, IntSummaryStatistics> getDataFromFile() {
        return file -> {
            try {
                FileInputStream fs = new FileInputStream(file);
                GZIPInputStream gzs = new GZIPInputStream(fs);
                InputStreamReader isr = new InputStreamReader(gzs);
                BufferedReader br = new BufferedReader(isr);

                ToIntFunction<? super String> mapper = string -> {
                    String tempString = string.substring(14, 20).trim();
                    return Integer.valueOf(tempString);
                };

                return br.lines().parallel().mapToInt(mapper)
                        .summaryStatistics();

            } catch (Exception e) {
                e.printStackTrace();
                return new IntSummaryStatistics();
            }

        };
    }

    private static Predicate<String> getRowFilter() {
        return string -> {
            String errorString = "-9999";
            boolean yearInValid = string.substring(0, 4).equals(errorString);

            String tempString = string.substring(14, 19);
            boolean tempInValid = tempString.equals(errorString);

            int temp = Integer.valueOf(tempString.trim());
            boolean tempInRange = temp > -100 && temp < 100;

            return yearInValid || tempInValid || !tempInRange;
        };
    }

    private static void validateFolderToScan() {
        if (!rootFolder.exists() || !rootFolder.isDirectory()
                || !rootFolder.canRead()) {

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
}
