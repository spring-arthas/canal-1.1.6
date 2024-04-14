package com.alibaba.otter.canal.deployer;

import java.util.Objects;
import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.server.CanalMQStarter;

/**
 * Canal server 启动类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.2
 */
public class CanalStarter {
    private static final Logger logger                    = LoggerFactory.getLogger(CanalStarter.class);
    private static final String CONNECTOR_SPI_DIR         = "/plugin";
    private static final String CONNECTOR_STANDBY_SPI_DIR = "/canal/plugin";
    /**
     * canal控制器
     * */
    private CanalController     controller                = null;
    /**
     * canal MQ代理生产者
     * */
    private CanalMQProducer     canalMQProducer           = null;
    private Thread              shutdownThread            = null;
    /**
     * canal MQ启动器
     * */
    private CanalMQStarter      canalMQStarter            = null;
    /**
     * canal全局配置：合并和本地和canal-admin平台的公共配置
     * */
    private volatile Properties properties;
    private volatile boolean    running                   = false;
    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties){
        this.properties = properties;
    }
    public boolean isRunning() {
        return running;
    }
    public Properties getProperties() {
        return properties;
    }
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    public CanalController getController() {
        return controller;
    }

    /**
     * 启动方法
     * @throws Throwable
     */
    public synchronized void start() throws Throwable {
        // 1. 读取【canal.server.mode】配置参数，用于后续实例解析出的binlog数据的后置处理，是通过tcp方式消费还是通过MQ方式处理
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
        if (!"tcp".equalsIgnoreCase(serverMode)) {
            // SPI机制处理CanalMQProducer
            ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
            canalMQProducer = loader.getExtension(serverMode.toLowerCase(), CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR);
            if (Objects.nonNull(canalMQProducer)) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(canalMQProducer.getClass().getClassLoader());
                // 配置参数设置进canalMQProducer
                canalMQProducer.init(properties);
                Thread.currentThread().setContextClassLoader(cl);
            }
        }

        if (Objects.nonNull(canalMQProducer)) {
            MQProperties mqProperties = canalMQProducer.getMqProperties();
            // disable netty 禁用netty方式消费
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            if (mqProperties.isFlatMessage()) {
                // 设置为raw避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
            }
        }

        // 2. 构建canalController调度控制器并启动，该控制器用于对canal instances的处理，并新建thread用于JVM退出时执行控制器关闭操作
        controller = new CanalController(properties);
        controller.start();
        shutdownThread = new Thread(() -> {
            try {
                logger.info("## stop the canal server");
                controller.stop();
                CanalLauncher.runningLatch.countDown();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal Server:", e);
            } finally {
                logger.info("## canal server is down.");
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // 3. 构建CanalMQStarter启动canal instances个数对应的CanalMQRunnable，启动CanalMQRunnable用于不同实例对应的binlog解析数据
        // 通过canalMQProducer发送到对应的MQ队列
        if (Objects.nonNull(canalMQProducer)) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
            canalMQStarter.start(destinations);
            // 将CanalMQStarter托管给CanalController调度控制器
            controller.setCanalMQStarter(canalMQStarter);
        }

        // 4. start canalAdmin todo 后续分析CanalAdminController
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);
            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);
            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}",
                port,
                user,
                passwd,
                ip);
            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.parseInt(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }

        running = true;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }
}
