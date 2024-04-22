package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanal;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;

/**
 * canal独立版本启动的入口类  canal-deployer启动类
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */
public class CanalLauncher {
    private static final String             CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger             logger               = LoggerFactory.getLogger(CanalLauncher.class);
    public static final CountDownLatch      runningLatch         = new CountDownLatch(1);
    private static ScheduledExecutorService executor             = Executors.newScheduledThreadPool(1,
                                                                     new NamedThreadFactory("canal-server-scan"));
    public static void main(String[] args) {
        try {
            logger.info("=>【deployer】开始启动......");
            setGlobalUncaughtExceptionHandler();

            // 支持rocketmq client 配置日志路径
            System.setProperty("rocketmq.client.logUseSlf4j","true");

            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            // 构建CanalStarter启动器类，在配置文件加载且合并完成后，通过该对象进行实例的启动
            final CanalStarter canalStater = new CanalStarter(properties);

            // 处理【canal.admin.manager】配置参数逻辑，即canal-deployer服务需要连接canal-admin平台读取各类配置
            String managerAddress = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
            if (StringUtils.isNotEmpty(managerAddress)) {
                beginCanalAdminPlatformManage(canalStater, properties, managerAddress);
            } else {
                canalStater.setProperties(properties);
            }

            // 启动canal服务
            canalStater.start();
            runningLatch.await();
            executor.shutdownNow();
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:", e);
        }
    }

    /**
     * 开启针对配置了canal-admin平台配置信息的读取和定时读取
     * @param canalStarter
     * @param properties
     * @param managerAddress
     * */
    private static void beginCanalAdminPlatformManage(CanalStarter canalStarter, Properties properties, String managerAddress) {
        // 配置了canal-admin管理端地址后，读取用户名
        String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
        // 配置了canal-admin管理端地址后，读取密码
        String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
        // 配置了canal-admin管理端地址后，读取端口号
        String adminPort = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110");
        // 配置了canal-admin管理端地址后，读取自动注册配置
        boolean autoRegister = BooleanUtils.toBoolean(CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_REGISTER));
        // 配置了canal-admin管理端地址后，
        String autoCluster = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_AUTO_CLUSTER);
        // 配置了canal-admin管理端地址后，配置需要通过admin平台读取的集群名称，该集群名称下的实例信息需要加载，名字如果配置指定，则使用host
        // ip地址作为名字
        String adminRegisterName = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_REGISTER_NAME);
        if (StringUtils.isEmpty(adminRegisterName)) {
            adminRegisterName = AddressUtils.getHostName();
        }
        // 配置了canal-admin管理端地址后，读取配置好的ip地址
        String registerIp = CanalController.getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
        if (StringUtils.isEmpty(registerIp)) {
            registerIp = AddressUtils.getHostIp();
        }

        // 构建【PlainCanalConfigClient】客户端类，向canal-admin平台【集群管理】菜单下请求集群名称为【canal.admin.register.name】配置
        // 的名称对应canal远程主配置，注意是主配置不是该集群下的实例配置
        final PlainCanalConfigClient configClient = new PlainCanalConfigClient(managerAddress,
            user,
            passwd,
            registerIp,
            Integer.parseInt(adminPort),
            autoRegister,
            autoCluster,
            adminRegisterName);
        PlainCanal canalConfig = configClient.findServer(null);
        if (Objects.isNull(canalConfig)) {
            throw new IllegalArgumentException("managerAddress:" + managerAddress + " can't not found config for [" + registerIp + ":" + adminPort + "]");
        }
        // 获取从canal-admin平台读取到主配置
        Properties managerProperties = canalConfig.getProperties();
        // 合并本地【canal.properties】的主配置，不过会以本地的主配置为主
        managerProperties.putAll(properties);
        // 如果配置的【canal.auto.scan.interval】配置自动扫描间隔参数不为空，则开启异步线程自动进行配置变更，默认为5s
        int scanIntervalInSecond = Integer.valueOf(CanalController.getProperty(managerProperties, CanalConstants.CANAL_AUTO_SCAN_INTERVAL, "5"));
        // 开启自动间隔定时任务实时从canal-admin平台读取最新的配置信息，并替换本地的配置
        executor.scheduleWithFixedDelay(new Runnable() {

            private PlainCanal lastCanalConfig;

            @Override
            public void run() {
                try {
                    if (Objects.isNull(lastCanalConfig)) {
                        // 上次未向 canal-admin 发生拉取配置请求，则尝试拉取一次，作为最新的一次拉取配置
                        lastCanalConfig = configClient.findServer(null);
                    } else {
                        // 远程拉取最新的主配置，传递上次拉取请求后的md5参数，如果主配置没有任何变化，那么查询的结果为null，说明主配置没有
                        // 发生任何变化
                        PlainCanal newCanalConfig = configClient.findServer(lastCanalConfig.getMd5());
                        if (Objects.nonNull(newCanalConfig)) {
                            canalStarter.stop();
                            Properties managerProperties = newCanalConfig.getProperties();
                            managerProperties.putAll(properties);
                            canalStarter.setProperties(managerProperties);
                            // 配置变化重启所有实例信息
                            canalStarter.start();
                            lastCanalConfig = newCanalConfig;
                        }
                    }
                } catch (Throwable e) {
                    logger.error("scan failed", e);
                }
            }

        }, 0, scanIntervalInSecond, TimeUnit.SECONDS);
        canalStarter.setProperties(managerProperties);
    }

    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));
    }

}
