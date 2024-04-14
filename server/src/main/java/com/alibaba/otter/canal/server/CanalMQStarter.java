package com.alibaba.otter.canal.server;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import com.alibaba.otter.canal.connector.core.producer.MQDestination;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.util.Callback;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalMQConfig;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author debugcode
 */
public class CanalMQStarter {
    private static final Logger          logger         = LoggerFactory.getLogger(CanalMQStarter.class);
    private volatile boolean             running        = false;
    private ExecutorService              executorService;
    /**
     * canalMQ公共生产者对象
     * */
    private CanalMQProducer              canalMQProducer;
    private MQProperties                 mqProperties;
    private CanalServerWithEmbedded      canalServer;
    private Map<String, CanalMQRunnable> canalMQWorks   = new ConcurrentHashMap<>();
    private static Thread                shutdownThread = null;

    public CanalMQStarter(CanalMQProducer canalMQProducer){
        this.canalMQProducer = canalMQProducer;
    }

    public synchronized void start(String destinations) {
        try {
            if (running) {
                return;
            }
            mqProperties = canalMQProducer.getMqProperties();
            // set filterTransactionEntry
            if (mqProperties.isFilterTransactionEntry()) {
                System.setProperty("canal.instance.filter.transaction.entry", "true");
            }

            // 1. 创建CanalServerWithEmbedded实例，
            canalServer = CanalServerWithEmbedded.instance();
            // 对应每个instance启动一个worker线程
            executorService = Executors.newCachedThreadPool();

            // 2. 按逗号分隔出要启动的canal实例信息
            String[] dsts = StringUtils.split(destinations, ",");
            for (String destination : dsts) {
                destination = destination.trim();
                // 2.1 为当前实例创建CanalMQRunnable
                CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
                // 2.2 缓存当前实例名称和canalMQRunnable
                canalMQWorks.put(destination, canalMQRunnable);
                // 2.3 启动当前实例对应的CanalMQRunnable
                executorService.execute(canalMQRunnable);
            }

            running = true;
            logger.info("## the MQ workers is running now ......");
            shutdownThread = new Thread(() -> {
                try {
                    logger.info("## stop the MQ workers");
                    running = false;
                    executorService.shutdown();
                    canalMQProducer.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping MQ workers:", e);
                } finally {
                    logger.info("## canal MQ is down.");
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownThread);
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal MQ workers:", e);
        }
    }

    public synchronized void destroy() {
        running = false;
        if (executorService != null) {
            executorService.shutdown();
        }
        if (canalMQProducer != null) {
            canalMQProducer.stop();
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
    }

    /**
     * 启动指定实例对应的CanalMQRunnable，这是在canal-admin平台增量新建实例后会调用该方法进行处理
     * */
    public synchronized void startDestination(String destination) {
        CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
        if (canalInstance != null) {
            stopDestination(destination);
            CanalMQRunnable canalMQRunnable = new CanalMQRunnable(destination);
            canalMQWorks.put(canalInstance.getDestination(), canalMQRunnable);
            executorService.execute(canalMQRunnable);
            logger.info("## Start the MQ work of destination:" + destination);
        }
    }

    public synchronized void stopDestination(String destination) {
        CanalMQRunnable canalMQRunable = canalMQWorks.get(destination);
        if (canalMQRunable != null) {
            canalMQRunable.stop();
            canalMQWorks.remove(destination);
            logger.info("## Stop the MQ work of destination:" + destination);
        }
    }

    private class CanalMQRunnable implements Runnable {

        private String destination;

        CanalMQRunnable(String destination){
            this.destination = destination;
        }

        private AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            worker(destination, running);
        }

        public void stop() {
            running.set(false);
        }
    }

    /**
     * 一个CanalMQRunnable对应一个Worker，且只能启动一次, 目标是从EventStore中获取解析后的Entry数据封装进Message中，并发送到消息中心
     * */
    private void worker(String destination, AtomicBoolean destinationRunning) {
        while (!running || !destinationRunning.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        logger.info("## start the MQ producer: {}.", destination);
        MDC.put("destination", destination);
        final ClientIdentity clientIdentity = new ClientIdentity(destination, (short) 1001, "");
        while (running && destinationRunning.get()) {
            try {
                // 获取destinatin指定的canalInstance
                CanalInstance canalInstance = canalServer.getCanalInstances().get(destination);
                if (Objects.isNull(canalInstance)) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }

                MQDestination canalDestination = new MQDestination();
                canalDestination.setCanalDestination(destination);
                // 读取当前canalInstance对应的【instance.properties】配置文件中的MQ配置信息
                CanalMQConfig mqConfig = canalInstance.getMqConfig();
                canalDestination.setTopic(mqConfig.getTopic());
                canalDestination.setPartition(mqConfig.getPartition());
                canalDestination.setDynamicTopic(mqConfig.getDynamicTopic());
                canalDestination.setPartitionsNum(mqConfig.getPartitionsNum());
                canalDestination.setPartitionHash(mqConfig.getPartitionHash());
                canalDestination.setDynamicTopicPartitionNum(mqConfig.getDynamicTopicPartitionNum());
                canalDestination.setEnableDynamicQueuePartition(mqConfig.getEnableDynamicQueuePartition());
                // 执行下订阅，canalServer = CanalServerWithEmbedded，告知当前CanalMQRunnable需要从destination对应的到EventStore
                // 中获取消息数据
                canalServer.subscribe(clientIdentity);
                logger.info("## the MQ producer: {} is running now ......", destination);

                // 获取拉取超时时间
                Integer getTimeout = mqProperties.getFetchTimeout();
                // 获取批量大小
                Integer getBatchSize = mqProperties.getBatchSize();
                while (running && destinationRunning.get()) {
                    Message message;
                    if (getTimeout != null && getTimeout > 0) {
                        // 超时时间不为空，根据batchSize大小和超时时间配置获取ClientIdentity订阅关系中的消息数据
                        message = canalServer.getWithoutAck(clientIdentity,
                            getBatchSize,
                            getTimeout.longValue(),
                            TimeUnit.MILLISECONDS);
                    } else {
                        message = canalServer.getWithoutAck(clientIdentity, getBatchSize);
                    }

                    final long batchId = message.getId();
                    try {
                        int size = message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
                        if (batchId != -1 && size != 0) {
                            canalMQProducer.send(canalDestination, message, new Callback() {

                                @Override
                                public void commit() {
                                    canalServer.ack(clientIdentity, batchId); // 提交确认
                                }

                                @Override
                                public void rollback() {
                                    canalServer.rollback(clientIdentity, batchId);
                                }
                            }); // 发送message到topic
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }

                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            }
        }
    }
}
