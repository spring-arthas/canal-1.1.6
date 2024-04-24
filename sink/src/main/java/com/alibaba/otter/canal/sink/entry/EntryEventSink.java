package com.alibaba.otter.canal.sink.entry;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.memory.MemoryEventStoreWithBuffer;
import com.alibaba.otter.canal.store.model.Event;

/**
 * mysql binlog数据对象输出
 * 
 * @author jianghang 2012-7-4 下午03:23:16
 * @version 1.0.0
 */
public class EntryEventSink extends AbstractCanalEventSink<List<CanalEntry.Entry>> implements CanalEventSink<List<CanalEntry.Entry>> {
    private static final Logger    logger                        = LoggerFactory.getLogger(EntryEventSink.class);
    private static final int       maxFullTimes                  = 10;
    private CanalEventStore<Event> eventStore;
    protected boolean              filterTransactionEntry        = false;                                        // 是否需要尽可能过滤事务头/尾
    protected boolean              filterEmtryTransactionEntry   = true;                                         // 是否需要过滤空的事务头/尾
    protected long                 emptyTransactionInterval      = 5 * 1000;                                     // 空的事务输出的频率
    protected long                 emptyTransctionThresold       = 8192;                                         // 超过8192个事务头，输出一个

    protected volatile long        lastTransactionTimestamp      = 0L;
    protected AtomicLong           lastTransactionCount          = new AtomicLong(0L);
    protected volatile long        lastEmptyTransactionTimestamp = 0L;
    protected AtomicLong           lastEmptyTransactionCount     = new AtomicLong(0L);
    protected AtomicLong           eventsSinkBlockingTime        = new AtomicLong(0L);
    protected boolean              raw;

    public EntryEventSink(){
        addHandler(new HeartBeatEntryEventHandler());
    }

    @Override
    public void start() {
        super.start();
        Assert.notNull(eventStore);
        if (eventStore instanceof MemoryEventStoreWithBuffer) {
            this.raw = ((MemoryEventStoreWithBuffer) eventStore).isRaw();
        }
        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (!handler.isStart()) {
                handler.start();
            }
        }
    }

    @Override
    public void stop() {
        super.stop();

        for (CanalEventDownStreamHandler handler : getHandlers()) {
            if (handler.isStart()) {
                handler.stop();
            }
        }
    }

    public boolean filter(List<Entry> event, InetSocketAddress remoteAddress, String destination) {
        return false;
    }

    @Override
    public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                           throws CanalSinkException,
                                                                                                           InterruptedException {
        return sinkData(entrys, remoteAddress);
    }

    private boolean sinkData(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress) throws InterruptedException {
        boolean hasRowData = false;
        boolean hasHeartBeat = false;
        List<Event> events = new ArrayList<>();
        for (CanalEntry.Entry entry : entrys) {
            if (!doFilter(entry)) {
                continue;
            }

            if (filterTransactionEntry && (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND)) {
                long currentTimestamp = entry.getHeader().getExecuteTime();
                // 基于一定的策略控制，放过空的事务头和尾，便于及时更新数据库位点，表明工作正常
                if (lastTransactionCount.incrementAndGet() <= emptyTransctionThresold
                    && Math.abs(currentTimestamp - lastTransactionTimestamp) <= emptyTransactionInterval) {
                    continue;
                } else {
                    // fixed issue https://github.com/alibaba/canal/issues/2616
                    // 主要原因在于空事务只发送了begin，没有同步发送commit信息，这里修改为只对commit事件做计数更新，确保begin/commit成对出现
                    if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        lastTransactionCount.set(0L);
                        lastTransactionTimestamp = currentTimestamp;
                    }
                }
            }

            hasRowData |= (entry.getEntryType() == EntryType.ROWDATA);
            hasHeartBeat |= (entry.getEntryType() == EntryType.HEARTBEAT);
            // entry数据封装进Event，并添加至events数组
            Event event = new Event(new LogIdentity(remoteAddress, -1L), entry, raw);
            events.add(event);
        }

        if (hasRowData || hasHeartBeat) {
            // 存在row记录 或者 存在heartbeat记录，直接跳给后续处理
            return doSink(events);
        } else {
            // 需要过滤的数据
            if (filterEmtryTransactionEntry && !CollectionUtils.isEmpty(events)) {
                long currentTimestamp = events.get(0).getExecuteTime();
                // 基于一定的策略控制，放过空的事务头和尾，便于及时更新数据库位点，表明工作正常
                if (Math.abs(currentTimestamp - lastEmptyTransactionTimestamp) > emptyTransactionInterval
                    || lastEmptyTransactionCount.incrementAndGet() > emptyTransctionThresold) {
                    lastEmptyTransactionCount.set(0L);
                    lastEmptyTransactionTimestamp = currentTimestamp;
                    return doSink(events);
                }
            }

            // 直接返回true，忽略空的事务头和尾
            return true;
        }
    }

    protected boolean doSink(List<Event> events) {
        // 1. sink前的 前置处理
        for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
            events = handler.before(events);
        }
        long blockingStart = 0L;
        int fullTimes = 0;
        do {
            // 2. events数据存入eventStore，此处尝试放入，有可能存在eventStores中的数组已满导致无法放入本次events数据，那么就进行当前
            // 线程阻塞等待，详见else逻辑代码
            if (eventStore.tryPut(events)) {
                if (fullTimes > 0) {
                    eventsSinkBlockingTime.addAndGet(System.nanoTime() - blockingStart);
                }
                for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                    events = handler.after(events);
                }
                return true;
            } else {
                if (fullTimes == 0) {
                    blockingStart = System.nanoTime();
                }
                applyWait(++fullTimes);
                if (fullTimes % 100 == 0) {
                    long nextStart = System.nanoTime();
                    eventsSinkBlockingTime.addAndGet(nextStart - blockingStart);
                    blockingStart = nextStart;
                }
            }

            for (CanalEventDownStreamHandler<List<Event>> handler : getHandlers()) {
                events = handler.retry(events);
            }

        } while (running && !Thread.interrupted());
        return false;
    }

    protected boolean doFilter(CanalEntry.Entry entry) {
        if (filter != null && entry.getEntryType() == EntryType.ROWDATA) {
            String name = getSchemaNameAndTableName(entry);
            boolean need = filter.filter(name);
            if (!need) {
                logger.debug("filter name[{}] entry : {}:{}",
                    name,
                    entry.getHeader().getLogfileName(),
                    entry.getHeader().getLogfileOffset());
            }

            return need;
        } else {
            return true;
        }
    }

    // 处理无数据的情况，避免空循环挂死
    private void applyWait(int fullTimes) {
        int newFullTimes = fullTimes > maxFullTimes ? maxFullTimes : fullTimes;
        if (fullTimes <= 3) { // 3次以内
            Thread.yield();
        } else { // 超过3次，最多只sleep 10ms
            LockSupport.parkNanos(1000 * 1000L * newFullTimes);
        }

    }

    private String getSchemaNameAndTableName(CanalEntry.Entry entry) {
        return entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setFilterTransactionEntry(boolean filterTransactionEntry) {
        this.filterTransactionEntry = filterTransactionEntry;
    }

    public void setFilterEmtryTransactionEntry(boolean filterEmtryTransactionEntry) {
        this.filterEmtryTransactionEntry = filterEmtryTransactionEntry;
    }

    public void setEmptyTransactionInterval(long emptyTransactionInterval) {
        this.emptyTransactionInterval = emptyTransactionInterval;
    }

    public void setEmptyTransctionThresold(long emptyTransctionThresold) {
        this.emptyTransctionThresold = emptyTransctionThresold;
    }

    public AtomicLong getEventsSinkBlockingTime() {
        return eventsSinkBlockingTime;
    }

}
