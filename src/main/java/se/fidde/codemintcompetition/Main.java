package se.fidde.codemintcompetition;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {
        validateInputArgs(args);

        File folderToScan = new File(args[0]);
        validateFolderToScan(folderToScan);

        List<File> folders = getFolders(folderToScan);

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        FolderHandler gzipHandler = new FolderHandler(folders);

        System.out.println("Processing...");
        ForkJoinTask<Collection<String>> submit = forkJoinPool
                .submit(gzipHandler);

        try {
            Collection<String> join = submit.join();
            writeDataToFile(join);

            System.out.println("Processing done.");
            System.out.println("Shutting down.");

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private static List<File> getFolders(File folderToScan) {
        File[] listFiles = folderToScan.listFiles();
        List<File> folders = filter(listFiles);

        return folders;
    }

    private static List<File> filter(File[] listFiles) {
        List<File> asList = Arrays.asList(listFiles);
        Predicate<? super File> predicate = file -> {
            if (file.isDirectory())
                return true;

            return false;
        };
        List<File> collect = asList.stream().filter(predicate)
                .collect(Collectors.toList());

        return collect;
    }

    private static void writeDataToFile(Collection<String> join) {
        // TODO Auto-generated method stub

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
}
