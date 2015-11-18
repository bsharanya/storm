package backtype.storm.scheduler.advancedstela;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StelaHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StelaHelper.class);

    public static void writeToFile(File file, String data) {
        try {
            FileWriter fileWritter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
            bufferWritter.append(data);
            bufferWritter.close();
            fileWritter.close();
        } catch (IOException ex) {
            LOG.error("Error writing to file {}.", ex);
        }
    }
}
