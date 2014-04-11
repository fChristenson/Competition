package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.zip.GZIPInputStream;

public class GzipHandler extends RecursiveTask<List<ProcessedPost>> {

    private static final long serialVersionUID = 1L;
    private List<File> files;

    public GzipHandler(List<File> gzipFiles) {
        super();
        this.files = gzipFiles;
    }

    @Override
    protected List<ProcessedPost> compute() {
        if (files.size() <= 1)
            try {
                ProcessedPost postData = getPostData(files);
                List<ProcessedPost> result = new ArrayList<>();
                result.add(postData);
                return result;

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

        ForkJoinTask<List<ProcessedPost>> fork = gzipHandler.fork();
        ForkJoinTask<List<ProcessedPost>> fork2 = gzipHandler2.fork();

        try {
            List<ProcessedPost> list = fork.get();
            List<ProcessedPost> list2 = fork2.get();
            list.addAll(list2);

            return list;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    private ProcessedPost getPostData(List<File> files) throws IOException {

        File file = files.get(0);
        FileInputStream fileInputStream = new FileInputStream(file);
        GZIPInputStream gzipIs = new GZIPInputStream(fileInputStream);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipIs);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line = bufferedReader.readLine();

        List<ISDRow> rows = new ArrayList<ISDRow>();

        while (line != null) {
            String year = line.substring(0, 4);

            String temperatureString = line.substring(13, 19);
            temperatureString = temperatureString.trim();
            int temp = Integer.valueOf(temperatureString);

            if (!isValidData(year, temperatureString)) {
                bufferedReader.close();
                return new ProcessedPost(file.getName());
            }

            ISDRow row = new ISDRow(year, temp);
            rows.add(row);

            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        return new ProcessedPost(file.getName(), rows);
    }

    private boolean isValidData(String year, String temperatureString) {
        String invalidData = "-9999";
        boolean isInvalidYear = year.equals(invalidData);
        boolean isInvalidTempFormat = temperatureString.equals(invalidData);
        int temp;
        try {
            temp = Integer.valueOf(temperatureString);
            temp = temp / 10;

        } catch (NumberFormatException e) {
            return false;
        }
        boolean isValidTemp = (temp > -100 && temp < 100) ? true : false;

        if (!isInvalidYear && !isInvalidTempFormat && isValidTemp)
            return true;

        return false;
    }
}
