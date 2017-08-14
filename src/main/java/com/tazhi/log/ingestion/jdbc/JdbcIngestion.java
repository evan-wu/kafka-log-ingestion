/**
 * 
 */
package com.tazhi.log.ingestion.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.commons.beanutils.ConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tazhi.log.ingestion.Ingestion;
import com.tazhi.log.ingestion.IngestionMain;
import com.tazhi.log.ingestion.util.Assert;
import com.tazhi.log.ingestion.util.NamedThreadFactory;

/**
 * 把Kafka消息插入到数据库表里。
 * 
 * @author evan.wu
 *
 */
public class JdbcIngestion implements Ingestion {
    private static final Logger LOG = LoggerFactory.getLogger(IngestionMain.class);
    
    private DataSource dataSource;
    private Map<String, String> topicTableNameMapping = new HashMap<>();
    private Map<String, Map<String, Class<?>>> tableColumnTypes = new HashMap<>();
    private ThreadPoolExecutor executor;
    
    public void configure(Properties props) {
        String mappings = props.getProperty("log.topic.mappings");
        Assert.notEmpty(mappings, "Please configure log.topic.mappings");
        String[] array = mappings.split(";");
        if (array.length > 0) {
            for (String pair : array) {
                String[] topicAndTable = pair.split("=");
                topicTableNameMapping.put(topicAndTable[0].trim(), topicAndTable[1].trim());
            }
        }
        dataSource = createDataSource(props);
        int insertThreads = Integer.parseInt(props.getProperty("log.jdbc.insertThreads", "30"));
        this.executor = new ThreadPoolExecutor(insertThreads, insertThreads, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(insertThreads), new NamedThreadFactory("jdbc-insert"), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public boolean start() {
        try {
            scanTables();
        } catch (Exception e) {
            return false;
        }
        LOG.info("jdbc ingestion started.");
        return true;
    }

    
    @Override
    public void stop() {
        executor.shutdown();
    }

    @Override
    public void processBatch(Map<String, List<String>> eventBatch) {
        LOG.info("Processing events from topics:" + eventBatch.keySet());
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(eventBatch.size());
        AtomicInteger rows = new AtomicInteger();
        
        for (final String topic : eventBatch.keySet()) {
            final List<String> events = eventBatch.get(topic);
            executor.submit(() -> {
                Connection conn = null;
                try {
                    conn = dataSource.getConnection();
                    conn.setAutoCommit(false);
                    
                    for (String evt : events) {
                        JSONObject obj = JSON.parseObject(evt);
                        
                        List<Object> params = new ArrayList<Object>();
                        String sql = generateSql(obj, topicTableNameMapping.get(topic), tableColumnTypes.get(topicTableNameMapping.get(topic)), params);

                        PreparedStatement stmt = conn.prepareStatement(sql);
                        fillStatementParams(stmt, params);
                        stmt.executeUpdate();
                    }
                    conn.commit();
                    rows.incrementAndGet();
                    LOG.info(events.size() + " rows inserted into " + topicTableNameMapping.get(topic) + " from topic " + topic);
                    latch.countDown();
                } catch (SQLException e) {
                    LOG.error("Failed to insert", e);
                    throw new RuntimeException("Failed to insert", e);
                } finally {
                    if (conn != null) try { conn.close(); } catch (SQLException e) {}
                }
            });
        }
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("batch processing interrupted", e);
            throw new RuntimeException("batch processing interrupted", e);
        }
        LOG.info("Batch processing time in millis: " + (System.currentTimeMillis() - startTime) + ", total messages: " + rows.get());
    }

    protected String generateSql(JSONObject event, String tableName, Map<String, Class<?>> columnTypes, List<Object> params) {
            StringBuilder sql = new StringBuilder();
            sql.append("insert into ").append(tableName).append("(");
            StringBuilder temp = new StringBuilder(") values(");
            
            Set<String> attrs = event.keySet();
            
            for (String col: attrs) {
                if (columnTypes.get(col) == null) {
                    // column not exist in table
                    LOG.warn("column '" + col + "' does not exist in table '" + tableName + "', it will be ignored!");
                    continue;
                }
                
                if (params.size() > 0) {
                    sql.append(", ");
                    temp.append(", ");
                }
                sql.append(col);
                temp.append("?");
                params.add(ConvertUtils.convert(event.get(col), columnTypes.get(col)));
            }
        sql.append(temp.toString()).append(")");
        return sql.toString();
    }
    
    protected void fillStatementParams(PreparedStatement pst, List<Object> params) throws SQLException {
        for (int i=0, size=params.size(); i<size; i++) {
            pst.setObject(i + 1, params.get(i));
        }
    }
    
    protected DataSource createDataSource(Properties props) {
        DruidDataSource ds = new DruidDataSource();
        
        ds.setUrl(props.getProperty("jdbc.url"));
        ds.setUsername(props.getProperty("jdbc.user"));
        ds.setPassword(props.getProperty("jdbc.password"));
        ds.setDriverClassName(props.getProperty("jdbc.driverClass"));
        ds.setInitialSize(initialSize);
        ds.setMinIdle(minIdle);
        ds.setMaxActive(maxActive);
        ds.setMaxWait(maxWait);
        ds.setTimeBetweenConnectErrorMillis(timeBetweenConnectErrorMillis);
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        
        ds.setValidationQuery(validationQuery);
        ds.setTestWhileIdle(testWhileIdle);
        ds.setTestOnBorrow(testOnBorrow);
        ds.setTestOnReturn(testOnReturn);
        
        ds.setRemoveAbandoned(removeAbandoned);
        ds.setRemoveAbandonedTimeoutMillis(removeAbandonedTimeoutMillis);
        ds.setLogAbandoned(logAbandoned);
        
        ds.setMaxPoolPreparedStatementPerConnectionSize(maxPoolPreparedStatementPerConnectionSize);
        return ds;
    }
    
    protected void scanTables() {
        Connection conn = null;
        JavaType javaType = new JavaType();
        try {
            conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            for (String tableName : topicTableNameMapping.values()) {
                Map<String, Class<?>> columnTypes = new HashMap<>();
                tableColumnTypes.put(tableName, columnTypes);
                
                try {
                    ResultSet rs = stmt.executeQuery("select * from " + tableName + " where 1=2");
                    ResultSetMetaData rsmd = rs.getMetaData();

                    for (int i=1; i<=rsmd.getColumnCount(); i++) {
                        String colName = rsmd.getColumnName(i);
                        String colClassName = rsmd.getColumnClassName(i);

                        Class<?> clazz = javaType.getType(colClassName);
                        
                        if (clazz != null) {
                            columnTypes.put(colName, clazz);
                        } else {
                            int type = rsmd.getColumnType(i);
                            if (type == Types.BINARY || type == Types.VARBINARY || type == Types.BLOB) {
                                columnTypes.put(colName, byte[].class);
                            }
                            else if (type == Types.CLOB || type == Types.NCLOB) {
                                columnTypes.put(colName, String.class);
                            }
                            else {
                                columnTypes.put(colName, String.class);
                            }
                        }
                    }
                } catch (SQLException e) {
                    LOG.error("Table " + tableName + " does not exist!", e);
                    throw new IllegalArgumentException("Table " + tableName + " does not exist!", e);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to check table", e);
            throw new RuntimeException("Failed to check table", e);
        } finally {
            if (conn != null)
                try { conn.close(); } catch (SQLException e) {}
        }
    }
    
    /** default druid connection pool config **/
    
    // 初始连接池大小、最小空闲连接数、最大活跃连接数
    private int initialSize = 10;
    private int minIdle = 10;
    private int maxActive = 100;
    
    // 配置获取连接等待超时的时间
    private long maxWait = DruidDataSource.DEFAULT_MAX_WAIT;
    
    // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
    private long timeBetweenEvictionRunsMillis = DruidDataSource.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    // 配置连接在池中最小生存的时间
    private long minEvictableIdleTimeMillis = DruidDataSource.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    // 配置发生错误时多久重连
    private long timeBetweenConnectErrorMillis = DruidDataSource.DEFAULT_TIME_BETWEEN_CONNECT_ERROR_MILLIS;
    
    /**
     * hsqldb - "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS"
     * Oracle - "select 1 from dual"
     * DB2 - "select 1 from sysibm.sysdummy1"
     * mysql - "select 1"
     */
    private String validationQuery = "select 1";
    private boolean testWhileIdle = true;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;
    
    // 是否打开连接泄露自动检测
    private boolean removeAbandoned = false;
    // 连接长时间没有使用，被认为发生泄露时长
    private long removeAbandonedTimeoutMillis = 300 * 1000;
    // 发生泄露时是否需要输出 log，建议在开启连接泄露检测时开启，方便排错
    private boolean logAbandoned = false;
    
    // 是否缓存preparedStatement，即PSCache，对支持游标的数据库性能提升巨大，如 oracle、mysql 5.5 及以上版本
    // private boolean poolPreparedStatements = false;  // oracle、mysql 5.5 及以上版本建议为 true;
    
    // 只要maxPoolPreparedStatementPerConnectionSize>0,poolPreparedStatements就会被自动设定为true，使用oracle时可以设定此值。
    private int maxPoolPreparedStatementPerConnectionSize = -1;
}
