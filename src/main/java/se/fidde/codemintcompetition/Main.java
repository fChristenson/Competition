package se.fidde.codemintcompetition;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Spliterator;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Main {

    private static String errorOutputFile;
    private static String outputFile;
    private static String folderToScan;
    private static FileVisitOption options;
    private static BiPredicate<Path, BasicFileAttributes> matcher;
    private static Path start;

    public static void main(String[] args) throws IOException {
        validateInputArgs(args);
        folderToScan = args[0];
        outputFile = args[1];
        if (args.length == 3)
            errorOutputFile = args[2];

        System.out.println("Processing...");

        try {
            process(folderToScan);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println("Done.");
    }

    private static void process(String folderToScan) throws IOException,
            URISyntaxException {
        File file = new File(folderToScan);

        validateFolderToScan(file);

        start = file.toPath();
        matcher = getMatcher();
        options = FileVisitOption.FOLLOW_LINKS;

        Stream<Path> find = Files.find(start, 2, matcher, options).parallel();
        Spliterator<Path> spliterator = find.spliterator().trySplit();

        // TODO: get streams to files and get data

    }

    private static BiPredicate<Path, BasicFileAttributes> getMatcher() {
        return (path, basicFileAttr) -> {

            return Pattern.matches(".+\\.gz", path.toString());
        };
    }

    private static void validateFolderToScan(File file) {
        if (!file.exists() || !file.isDirectory() || !file.canRead()) {
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
