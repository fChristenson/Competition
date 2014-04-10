package se.fidde.codemintcompetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.zip.GZIPInputStream;

public class GzipHandler extends RecursiveTask<DataWrapper> {

    private static final long serialVersionUID = 1L;
    private List<File> files;

    public GzipHandler(List<File> gzipFiles) {
        super();
        this.files = gzipFiles;
    }

    @Override
    protected DataWrapper compute() {
        if (files.size() <= 1)
            try {
                return getData(files);

            } catch (IOException e1) {
                e1.printStackTrace();
                return DataWrapper.getEmptyWrapper();

            }

        int mid = files.size() / 2;
        int end = files.size();

        List<File> gzipFiles = files.subList(0, mid);
        GzipHandler gzipHandler = new GzipHandler(gzipFiles);

        List<File> gzipFiles2 = files.subList(mid, end);
        GzipHandler gzipHandler2 = new GzipHandler(gzipFiles2);

        ForkJoinTask<DataWrapper> fork = gzipHandler.fork();
        ForkJoinTask<DataWrapper> fork2 = gzipHandler2.fork();

        try {
            DataWrapper wrapper = fork.get();
            DataWrapper wrapper2 = fork2.get();
            wrapper.getTemperatureCollection().addAll(
                    wrapper2.getTemperatureCollection());

            return wrapper;

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return DataWrapper.getEmptyWrapper();
        }
    }

    private DataWrapper getData(List<File> files) throws IOException {

        File file = files.get(0);
        FileInputStream fileInputStream = new FileInputStream(file);
        GZIPInputStream gzipIs = new GZIPInputStream(fileInputStream);
        InputStreamReader inputStreamReader = new InputStreamReader(gzipIs);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line = bufferedReader.readLine();

        String year = getYear(line);
        DataWrapper dataWrapper = new DataWrapper(year);

        while (line != null) {
            String temperatureString = line.substring(13, 19);

            temperatureString = temperatureString.trim();
            double temperature = Double.valueOf(temperatureString) / 10;

            dataWrapper.getTemperatureCollection().add(temperature);
            line = bufferedReader.readLine();
        }
        bufferedReader.close();
        return dataWrapper;
    }

    private String getYear(String line) {
        if (line != null) {
            String year = line.substring(0, 4);
            return year;
        }
        return null;
    }
}
