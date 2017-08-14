/**
 * 
 */
package com.tazhi.log.ingestion;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tazhi.log.ingestion.util.Assert;

/**
 * 从Kafka获取消息并插入到指定的存储。
 * 
 * @author evan.wu
 *
 */
public class IngestionMain {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionMain.class);
    
    public static void main(String[] args) {
        if (args.length <= 0) {
            LOG.info("USAGE: java [options] " + IngestionMain.class.getName() + " config.properties");
            return;
        }
        
        try {
            Properties props = new Properties();
            InputStream in = new FileInputStream(args[0]);
            props.load(in);
            
            Assert.notEmpty(props.getProperty("log.ingestion.class"), "Please configure log.ingestion.class");
            Class<?> ingestionClass = Class.forName(props.getProperty("log.ingestion.class"));
            if (!Ingestion.class.isAssignableFrom(ingestionClass)) {
                throw new IllegalArgumentException(ingestionClass + " is not instance of " + Ingestion.class);
            }
            
            Ingestion ingestion = (Ingestion)ingestionClass.newInstance();
            ingestion.configure(props);
            if (!ingestion.start()) {
                LOG.error("Failed to start ingestion.");
                System.exit(-1);
            }
            
            KafkaSource source = new KafkaSource(props);
            source.setIngestion(ingestion);
            source.start();
    
            LOG.info("Kafka message ingestion programm started.");
            
            Runtime.getRuntime().addShutdownHook(new Thread(()->{
                source.stop();
                ingestion.stop();
            }));
        } catch (Exception e) {
            LOG.error("Ingestion program exception", e);
        }
    }

}
