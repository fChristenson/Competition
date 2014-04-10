package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.zip.GZIPInputStream;

public class GzipHandler extends RecursiveTask<Collection<DataWrapper>> {

    private static final long serialVersionUID = 1L;
    private List<File> files;

    public GzipHandler(List<File> gzipFiles) {
        super();
        this.files = gzipFiles;
    }

    @Override
    protected Collection<DataWrapper> compute() {
        if (files.size() <= 1)
            try {
                return getData(files);

            } catch (IOException e1) {
                e1.printStackTrace();
                return Collections.emptyList();
            }

        int mid = files.size() / 2;
        int end = files.size();

        List<File> gzipFiles = files.subList(0, mid);
        GzipHandler gzipHandler = new GzipHandler(gzipFiles);

        List<File> gzipFiles2 = files.subList(mid, end);
        GzipHandler gzipHandler2 = new GzipHandler(gzipFiles2);

        ForkJoinTask<Collection<DataWrapper>> fork = gzipHandler.fork();
        ForkJoinTask<Collection<DataWrapper>> fork2 = gzipHandler2.fork();

        try {
            Collection<DataWrapper> collection = fork.get();
            Collection<DataWrapper> collection2 = fork2.get();
            collection.addAll(collection2);

            return collection;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    private Collection<DataWrapper> getData(List<File> files)
            throws IOException {

        File file = files.get(0);
        FileInputStream fileInputStream = new FileInputStream(file);
        GZIPInputStream gzipIs = new GZIPInputStream(fileInputStream);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipIs);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        Collection<DataWrapper> result = new ArrayList<>();
        String line = bufferedReader.readLine();

        while (line != null) {
            String year = line.substring(0, 4);
            String temperatureString = line.substring(13, 19);

            temperatureString = temperatureString.trim();
            double temperature = Double.valueOf(temperatureString) / 10;

            DataWrapper dataWrapper = new DataWrapper(year, temperature);

            result.add(dataWrapper);
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        return result;
    }
}
