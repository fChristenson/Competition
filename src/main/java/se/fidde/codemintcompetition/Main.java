package se.fidde.codemintcompetition;

import java.io.File;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Main {

    static final Logger log = LogManager.getLogger(Main.class.getName());
    private static File invalidPostsOutputFile;

    public static void main(String[] args) {
        log.debug("starting app");

        validateInputArgs(args);

        File folderToScan = new File(args[0]);
        validateFolderToScan(folderToScan);

        instanciateInvalidPostsOutputFile(args);

    }

    private static void instanciateInvalidPostsOutputFile(String[] args) {
        if (args.length == 2) {
            invalidPostsOutputFile = new File(args[1]);
        }
    }

    private static void validateFolderToScan(File folderToScan) {
        log.debug("validating folder to scan");

        if (!folderToScan.exists() || !folderToScan.isDirectory()
                || !folderToScan.canRead()) {
            System.out.println("Provided folder is not valid");
            System.out
                    .println("Please make sure that the path is correct and you have read/write access");
            System.exit(0);
        }
        log.debug("folder to scan is valid");
    }

    private static void validateInputArgs(String[] args) {
        log.debug("validating args");
        if (args.length > 2 || args.length < 1) {
            System.out.println("Invalid arguments format");
            System.out
                    .println("Please use: <folder to scan> <optional:invalid posts outputfile>");
            System.exit(0);
        }
        log.debug("args length valid");
    }
}
