package backtype.storm.multinenantstela;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StelaHelpers {
    private static final Logger LOG = LoggerFactory.getLogger(StelaHelpers.class);

    public static void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
        } catch (IOException ex) {
            LOG.error("Error writing to file {}", ex);
        }
    }
}
