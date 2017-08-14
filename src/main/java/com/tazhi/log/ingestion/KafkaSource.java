/**
 * 
 */
package com.tazhi.log.ingestion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tazhi.log.ingestion.util.Assert;

/**
 * Kafka消息源。kafka的配置项在配置文件中以kafka.开头。其中必须的配置项有：
 * <ul>
 * <li>kafka.bootstrap.servers</li>
 * <li>kafka.group.id</li>
 * <li>log.topic.mappings</li>
 * </ul>
 * 
 * @author evan.wu
 *
 */
public class KafkaSource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int DEFAULT_BATCH_DURATION = 1000;
    
    private HashMap<TopicPartition, OffsetAndMetadata> tpAndOffsetMetadata;
    private AtomicBoolean rebalanceFlag;
    private Properties kafkaProps;

    private int batchUpperLimit;
    private int maxBatchDurationMillis;
    private KafkaConsumer<String, String> consumer;
    private List<String> topics = new ArrayList<>();
    private Iterator<ConsumerRecord<String, String>> it;
    private volatile boolean running = false;
    private Ingestion ingestion;
    
    public KafkaSource(Properties props) {
        String mappings = props.getProperty("log.topic.mappings");
        Assert.notEmpty(mappings, "Please configure log.topic.mappings");
        String[] array = mappings.split(";");
        if (array.length > 0) {
            for (String pair : array) {
                String[] topicAndTable = pair.split("=");
                topics.add(topicAndTable[0].trim());
            }
        }
        
        Assert.notEmpty(props.getProperty("kafka.bootstrap.servers"), "Please configure kafka.bootstrap.servers");
        Assert.notEmpty(props.getProperty("kafka.group.id"), "Please configure kafka.group.id");
        
        kafkaProps = new Properties();
        // some default config // Note: the consumer group id, different group consumers the same messages
        kafkaProps.put("enable.auto.commit", "false"); // auto commit
        kafkaProps.put("session.timeout.ms", "60000");
        kafkaProps.put("auto.offset.reset", "earliest"); // consumer all records
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("max.poll.records", 30000); // too many records increase processing time, and may cause session timeout
        
        collectKafkaConcumerProps(kafkaProps, props);
        
        String batchSize = props.getProperty("kafka.batchSize");
        String batchDuration = props.getProperty("kafka.batchDuration");
        batchUpperLimit = batchSize == null? DEFAULT_BATCH_SIZE : Integer.parseInt(batchSize);
        maxBatchDurationMillis = batchDuration == null? DEFAULT_BATCH_DURATION : Integer.parseInt(batchDuration);
        
        tpAndOffsetMetadata = new HashMap<TopicPartition, OffsetAndMetadata>();
        rebalanceFlag = new AtomicBoolean(false);
    }
    
    public void start() {
        LOG.info("Starting kafka source...");
        
        consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.subscribe(topics, new SourceRebalanceListener(rebalanceFlag));
        //尝试获取消息，首次可能需要较长时间...或者kafka连接不上
        it = consumer.poll(1000).iterator();
        
        running  = true;
        
        new Thread( () -> {
            while (running) {
                try {
                    // prepare time variables for new batch
                    final long batchStartTime = System.currentTimeMillis();
                    final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
                    
                    int batchMessageSize = 0;
                    final Map<String, List<String>> eventBatch = new HashMap<>();
                    
                    while (batchMessageSize < batchUpperLimit && System.currentTimeMillis() < maxBatchEndTime) {
                        if (it == null || !it.hasNext()) {
                            // Obtaining new records
                            // Poll time is remainder time for current batch.
                            ConsumerRecords<String, String> records = consumer
                                    .poll(Math.max(0, maxBatchEndTime - System.currentTimeMillis()));
                            it = records.iterator();
    
                            // this flag is set to true in a callback when some partitions are revoked.
                            // If there are any records we commit them.
                            if (rebalanceFlag.get()) {
                                rebalanceFlag.set(false);
                                break;
                            }
                            // check records after poll
                            if (!it.hasNext()) {
                                // batch time exceeded
                                break;
                            }
                        }
                        
                        // get next message
                        ConsumerRecord<String, String> message = it.next();
                        List<String> topicEvents = eventBatch.get(message.topic());
                        if (topicEvents == null) {
                            topicEvents = new ArrayList<>();
                            topicEvents.add(message.value());
                            eventBatch.put(message.topic(), topicEvents);
                        } else {
                            topicEvents.add(message.value());
                        }
                        batchMessageSize ++;
                        
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Waited: {} ", System.currentTimeMillis() - batchStartTime);
                            LOG.debug("Event #: {}", batchMessageSize);
                        }
                        
                        // For each partition store next offset that is going to be read.
                        tpAndOffsetMetadata.put(new TopicPartition(message.topic(), message.partition()),
                                new OffsetAndMetadata(message.offset() + 1, ""));
                    }
                    
                    if (eventBatch.size() > 0) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("batching time in millis: " + (System.currentTimeMillis() - batchStartTime));
                        
                        getIngestion().processBatch(eventBatch);
                        
                        eventBatch.clear();
                        batchMessageSize = 0;
                        
                        if (!tpAndOffsetMetadata.isEmpty()) {
                            long commitStartTime = System.currentTimeMillis();
                            consumer.commitSync(tpAndOffsetMetadata);
                            long commitEndTime = System.currentTimeMillis();
                            if (LOG.isDebugEnabled())
                                LOG.debug("batch commit time in millis: " + (commitEndTime - commitStartTime));
                            tpAndOffsetMetadata.clear();
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Kafka consumer exception", e);
                }
            }
        }).start();
        LOG.info("Kafka source started.");
    }
    
    public Ingestion getIngestion() {
        return ingestion;
    }

    public void setIngestion(Ingestion ingestion) {
        this.ingestion = ingestion;
    }
    
    private void collectKafkaConcumerProps(Properties kafkaProps, Properties props) {
        for (Object key : props.keySet()) {
            if (key.toString().startsWith("kafka."))
                kafkaProps.put(key.toString().substring(6), props.getProperty(key.toString()));
        }
    }
    
    public void stop() {
        running = false;
        if (consumer != null)
            consumer.close();
    }
}

class SourceRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(SourceRebalanceListener.class);
    private AtomicBoolean rebalanceFlag;

    public SourceRebalanceListener(AtomicBoolean rebalanceFlag) {
        this.rebalanceFlag = rebalanceFlag;
    }

    // Set a flag that a rebalance has occurred. Then commit already read events to
    // kafka.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            LOG.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
            rebalanceFlag.set(true);
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            LOG.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
        }
    }
}