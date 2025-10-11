package com.alibaba.otter.canal.example;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Lists;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 集群模式的测试例子
 * 
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class ClusterCanalClientTest extends AbstractCanalClientTest {
    // 当前事务上下文（单线程安全）
    private static TransactionContext currentTx = null;

    public ClusterCanalClientTest(String destination){
        super(destination);
    }

    public static void main(String args[]) {

        // 连接canal-deployer服务
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("172.21.32.104", 11111),
                "veneno-instance03", // instance name
                "",        // username
                ""         // password
        );

        try {
            connector.connect();
            //connector.subscribe(".*\\..*"); // 订阅所有库表
            connector.subscribe("jxgy_saas_test\\.edu_family_member"); // 订阅所有库表
            connector.rollback(); // 回滚到未确认位置

            while (true) {
                // 3. 拉取消息（batchSize=100, timeout=1秒）
                Message message = connector.getWithoutAck(100, 1000L, TimeUnit.MILLISECONDS);
                long batchId = message.getId();

                if (batchId == -1 || message.getEntries().isEmpty()) {
                    continue;
                }

                // 4. 遍历每个 Entry（即每个 binlog 事件）
                for (CanalEntry.Entry entry : message.getEntries()) {
                    processEntry(entry);
                }

                // 5. 确认消费（避免重复）
                connector.ack(batchId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }








        /*String destination = "example";

        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector("127.0.0.1:2181", destination, "canal", "canal");

        final ClusterCanalClientTest clientTest = new ClusterCanalClientTest(destination);
        clientTest.setConnector(connector);
        clientTest.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                clientTest.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));*/
    }

    private static void processEntry(CanalEntry.Entry entry) {
        CanalEntry.EntryType type = entry.getEntryType();
        // 忽略非 ROWDATA 和 QUERY 类型
        if (type == CanalEntry.EntryType.TRANSACTIONBEGIN) {
            // 开启新事务
            currentTx = new TransactionContext();
        } else if (type == CanalEntry.EntryType.TRANSACTIONEND) {
            // 事务结束
            if (currentTx != null) {
                // ✅ 获取 XID（1.1.6 支持！）
                //long xid = entry.getHeader().getLogfileOffset(); // ❌ 错！
                // 正确方式：
                try {
                    CanalEntry.TransactionEnd txEnd = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    String xid = txEnd.getTransactionId();
                    currentTx.setXid(xid);
                } catch (Exception e) {
                    // 兜底：用时间戳或 offset 作为伪事务ID（不推荐）
                    currentTx.setXid(String.valueOf(entry.getHeader().getExecuteTime()));
                }

                currentTx.printAuditLog();
                currentTx = null; // 清空上下文
            }
        } else if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
            try {
                if(currentTx != null) {
                    currentTx.addEntry(entry);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void printRow(String prefix, List<CanalEntry.Column> columns) {
        System.out.print(prefix + " [");
        for (int i = 0; i < columns.size(); i++) {
            CanalEntry.Column col = columns.get(i);
            if (i > 0) System.out.print(", ");
            System.out.print(col.getName() + "=" + col.getValue());
        }
        System.out.println("]");
    }

    static class TransactionContext {
        String xid = "-1";
        List<CanalEntry.Entry> entries = Lists.newArrayList();

        public void addEntry(CanalEntry.Entry entry) {
            entries.add(entry);
        }

        public void setXid(String xid) {
            this.xid = xid;
        }

        public void printAuditLog() {
            if(CollectionUtils.isEmpty(entries)) {
                return;
            }
            System.out.println("\n=== 订阅到事务开始 [当前事务Id：XID=" + xid + "] 本次事务变更信息如下 ===");
            System.out.println();

            // 输出变更
            int i = 0;
            for (CanalEntry.Entry entry : entries) {
                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                    try {
                        String dbName = entry.getHeader().getSchemaName();
                        String tableName = entry.getHeader().getTableName();
                        long timestamp = entry.getHeader().getExecuteTime(); // 毫秒
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        System.out.println("=== 事件序号 "+(++i)+" ===");
                        Date date = new Date(timestamp);
                        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
                        System.out.println("执行时间: " + ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        System.out.println("数据库: " + dbName);
                        System.out.println("操作表: " + tableName);
                        System.out.println("事件类型: " + eventType);

// 如果开启了 binlog_rows_query_log_events，这里可以拿到原始 SQL
                        if (rowChange.hasSql() && !rowChange.getSql().isEmpty()) {
                            System.out.println("本次dml对应SQL: " + rowChange.getSql());
                        }

// 遍历每行变更（支持批量 DML）
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            if (eventType == CanalEntry.EventType.DELETE) {
                                printRow("删除前镜像", rowData.getBeforeColumnsList());
                            } else if (eventType == CanalEntry.EventType.INSERT) {
                                printRow("新增后镜像", rowData.getAfterColumnsList());
                            } else if (eventType == CanalEntry.EventType.UPDATE) {
                                printRow("编辑前镜像：", rowData.getBeforeColumnsList());
                                printRow("编辑后镜像：", rowData.getAfterColumnsList());
                            }
                        }
                        System.out.println();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("\n=== 订阅到事务结束 [当前事务Id：XID=" + xid + "] ===");
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
        }
    }
}
