/**
 * 
 */
package com.tazhi.log.ingestion;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 保存事件的接口。
 * 
 * @author evan.wu
 *
 */
public interface Ingestion {
    void configure(Properties props);
    
    boolean start();
    
    void stop();
    
    /**
     * 批量处理消息，必须在处理完所有消息后才返回。
     * 
     * @param eventBatch key: topic, value: 该topic下批量的消息
     */
    void processBatch(Map<String, List<String>> eventBatch);
}
