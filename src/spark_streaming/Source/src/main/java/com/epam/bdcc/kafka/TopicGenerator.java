package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();

            Path path = Paths.get(sampleFile);
            List<String> fileLines = null;


            try {
                fileLines = Files.readAllLines(path);
            } catch (IOException e) {
                System.out.println("Error during file reading... " + e.getMessage());
            }

            if (fileLines.size() > 0 && skipHeader) {
                fileLines.remove(0);
            }

            for (String fileLine: fileLines) {
                MonitoringRecord monitoringRecord = new MonitoringRecord(fileLine.split(FILE_TOKEN_SEPARATOR));

                ProducerRecord<String, MonitoringRecord> producerRecord =
                        new ProducerRecord(
                                topicName,
                                KafkaHelper.getKey(monitoringRecord),
                                monitoringRecord);

                producer.send(producerRecord);
                TimeUnit.SECONDS.sleep(batchSleep);
            }
        }
    }

}
