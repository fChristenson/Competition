package se.fidde.codemintcompetition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

    private static File errorOutputFile;
    private static File outputFile;
    private static File rootFolder;
    private static FileWriter fWriter;
    private static BufferedWriter bWriter;
    private static FileVisitOption options;
    private static BiPredicate<Path, BasicFileAttributes> correctFolders;
    private static Path startFolderForSearch;

    public static void main(String[] args) throws IOException,
            URISyntaxException {

        instanciateParamFiles(args);

        System.out.println("Processing...");
        process();
        System.out.println("Done.");
    }

    private static void instanciateParamFiles(String[] args) {
        validateInputArgs(args);

        rootFolder = new File(args[0]);
        outputFile = new File(args[1]);
        if (args.length == 3)
            errorOutputFile = new File(args[2]);
    }

    private static void validateInputArgs(String[] args) {
        if (args.length > 3 || args.length < 2) {
            System.out.println("Invalid arguments format");
            System.out
                    .println("Please use: <folder to scan> <outputfile> <optional:invalid posts outputfile>");
            System.exit(0);
        }
    }

    private static void process() throws IOException, URISyntaxException {
        validateFolderToScan();

        startFolderForSearch = rootFolder.toPath();
        correctFolders = getCorrectFolders();
        options = FileVisitOption.FOLLOW_LINKS;

        Function<Path, PostStatistics> dataPerFolder = getDataForEachFolder();
        writeResultsToFile(dataPerFolder);
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

    private static BiPredicate<Path, BasicFileAttributes> getCorrectFolders() {
        return (path, attr) -> {
            return attr.isDirectory()
                    && Pattern.matches("\\d{4}", path.getFileName().toString());
        };
    }

    private static Function<Path, PostStatistics> getDataForEachFolder() {
        return path -> {
            File file = path.toFile();
            List<IntSummaryStatistics> summaryStatistics = IsdFileReader
                    .getDataForEachGzipFile(file);

            IntSummaryStatistics statistics = getSumOfAllData(summaryStatistics);
            String year = path.getFileName().toString();

            return new PostStatistics(year, statistics);
        };
    }

    private static void writeResultsToFile(
            Function<Path, PostStatistics> dataPerFolder) throws IOException {

        List<PostStatistics> data = Files
                .find(startFolderForSearch, 1, correctFolders, options)
                .parallel().map(dataPerFolder).collect(Collectors.toList());

        Comparator<? super PostStatistics> comparator = getComparator();
        Consumer<? super PostStatistics> writeToFile = writeToOutputFile();

        fWriter = new FileWriter(outputFile);
        bWriter = new BufferedWriter(fWriter);

        data.stream().sorted(comparator).forEach(writeToFile);
        bWriter.close();
    }

    private static Consumer<? super PostStatistics> writeToOutputFile()
            throws IOException {

        return ps -> {
            try {
                StringBuilder builder = getFormatedString(ps);
                bWriter.write(builder.toString());
                bWriter.newLine();

            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    private static StringBuilder getFormatedString(PostStatistics ps) {
        StringBuilder builder = new StringBuilder();
        builder.append(ps.getYear()).append(";");
        builder.append(ps.getStatistics().getMax()).append(";");

        double average = ps.getStatistics().getAverage();
        builder.append((int) Math.round(average)).append(";");

        builder.append(ps.getStatistics().getMin()).append(";");
        return builder;
    }

    private static Comparator<? super PostStatistics> getComparator() {
        return (ps1, ps2) -> {
            int year = Integer.valueOf(ps1.getYear());
            int year2 = Integer.valueOf(ps2.getYear());

            if (year < year2)
                return -1;

            else if (year > year2)
                return 1;

            return 0;
        };
    }

    private static IntSummaryStatistics getSumOfAllData(
            List<IntSummaryStatistics> summaryStatistics) {

        IntSummaryStatistics result = new IntSummaryStatistics();
        summaryStatistics.parallelStream().forEach(sumary -> {
            result.combine(sumary);
        });

        return result;
    }

    // TODO: add check for invalid data and if erroroutput is defined print
    // invalid posts
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
}
