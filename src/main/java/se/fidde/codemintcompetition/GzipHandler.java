package se.fidde.codemintcompetition;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class GzipHandler extends RecursiveTask<Collection<String>> {

    private List<File> files;

    public GzipHandler(List<File> gzipFiles) {
        super();
        this.files = gzipFiles;
    }

    @Override
    protected Collection<String> compute() {
        if (files.size() == 1)
            return getData(files);

        int mid = files.size() / 2;
        int end = files.size();

        List<File> gzipFiles = files.subList(0, mid);
        GzipHandler gzipHandler = new GzipHandler(gzipFiles);

        List<File> gzipFiles2 = files.subList(mid, end);
        GzipHandler gzipHandler2 = new GzipHandler(gzipFiles2);

        ForkJoinTask<Collection<String>> fork = gzipHandler.fork();
        ForkJoinTask<Collection<String>> fork2 = gzipHandler2.fork();

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

    private Collection<String> getData(List<File> files) {

        return null;
    }

}
