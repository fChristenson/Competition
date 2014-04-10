package se.fidde.codemintcompetition;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FolderHandler extends RecursiveTask<Collection<String>> {

    private static final long serialVersionUID = 1L;
    private List<File> foldersWithGzipFiles;

    public FolderHandler(List<File> folders) {
        super();
        this.foldersWithGzipFiles = folders;
    }

    @Override
    protected Collection<String> compute() {
        if (foldersWithGzipFiles.size() == 1) {
            try {
                return getData(foldersWithGzipFiles);

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        int mid = foldersWithGzipFiles.size() / 2;
        int end = foldersWithGzipFiles.size();

        List<File> copy1 = foldersWithGzipFiles.subList(0, mid);
        FolderHandler folderHandler = new FolderHandler(copy1);

        List<File> copy2 = foldersWithGzipFiles.subList(mid, end);
        FolderHandler folderHandler2 = new FolderHandler(copy2);

        ForkJoinTask<Collection<String>> fork = folderHandler.fork();
        ForkJoinTask<Collection<String>> fork2 = folderHandler2.fork();

        try {
            Collection<String> collection = fork.get();
            Collection<String> collection2 = fork2.get();
            collection.addAll(collection2);

            return collection;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }

    }

    private Collection<String> getData(List<File> asList)
            throws InterruptedException, ExecutionException {

        File file = asList.get(0);
        File[] listFiles = file.listFiles();

        List<File> gzipFiles = filterList(listFiles);
        GzipHandler gzipHandler = new GzipHandler(gzipFiles);

        ForkJoinPool forkJoinPool = new ForkJoinPool();
        ForkJoinTask<Collection<DataWrapper>> submit = forkJoinPool
                .submit(gzipHandler);

        Collection<DataWrapper> collection = submit.get();
        return filterData(collection);
    }

    private Collection<String> filterData(Collection<DataWrapper> collection) {
        Predicate<? super DataWrapper> predicate = data -> {
            String year = data.getYear();
            double temperature = data.getTemperature();

            if (year.equals("-9999"))
                return false;

            else if (temperature < -100.0 || temperature > 100.0
                    || temperature == -9999)

                return false;

            return true;
        };

        collection.stream().filter(predicate).collect(Collectors.toList());
        return null;
    }

    private List<File> filterList(File[] listFiles) {
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
