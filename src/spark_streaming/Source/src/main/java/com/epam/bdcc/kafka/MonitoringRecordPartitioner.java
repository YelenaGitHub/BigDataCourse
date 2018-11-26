package com.epam.bdcc.kafka;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }

    public void close() {
        super.close();
    }

}