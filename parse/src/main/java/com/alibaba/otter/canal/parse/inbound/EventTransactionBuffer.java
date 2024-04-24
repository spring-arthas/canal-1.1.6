package com.alibaba.otter.canal.parse.inbound;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.store.CanalStoreException;

/**
 * 缓冲event队列，提供按事务刷新数据的机制
 * 
 * @author jianghang 2012-12-6 上午11:05:12
 * @version 1.0.0
 */
public class EventTransactionBuffer extends AbstractCanalLifeCycle {

    private static final long        INIT_SQEUENCE = -1;
    private int                      bufferSize    = 1024;
    private int                      indexMask;
    private CanalEntry.Entry[]       entries;
    /**
     * 关于putSequence和flushSequence，由于数组是循环使用，且大小为1024，可以得出，putSequence理应大于flushSequence
     * */
    private AtomicLong               putSequence   = new AtomicLong(INIT_SQEUENCE); // 代表当前put操作最后一次写操作发生的位置
    private AtomicLong               flushSequence = new AtomicLong(INIT_SQEUENCE); // 代表满足flush条件后最后一次数据flush的时间
    private TransactionFlushCallback flushCallback;

    public EventTransactionBuffer(){

    }

    public EventTransactionBuffer(TransactionFlushCallback flushCallback){
        this.flushCallback = flushCallback;
    }

    @Override
    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        Assert.notNull(flushCallback, "flush callback is null!");
        indexMask = bufferSize - 1;
        entries = new CanalEntry.Entry[bufferSize];
    }

    @Override
    public void stop() throws CanalStoreException {
        putSequence.set(INIT_SQEUENCE);
        flushSequence.set(INIT_SQEUENCE);

        entries = null;
        super.stop();
    }

    public void add(List<CanalEntry.Entry> entrys) throws InterruptedException {
        for (CanalEntry.Entry entry : entrys) {
            add(entry);
        }
    }

    public void add(CanalEntry.Entry entry) throws InterruptedException {
        switch (entry.getEntryType()) {
            case TRANSACTIONBEGIN:
                flush();// 刷新上一次的数据
                put(entry);
                break;
            case TRANSACTIONEND:
                put(entry);
                flush();
                break;
            case ROWDATA:
                put(entry);
                // 针对非DML的数据，直接输出，不进行buffer控制
                EventType eventType = entry.getHeader().getEventType();
                if (eventType != null && !isDml(eventType)) {
                    flush();
                }
                break;
            case HEARTBEAT:
                // master过来的heartbeat，说明binlog已经读完了，是idle状态
                put(entry);
                flush();
                break;
            default:
                break;
        }
    }

    private void put(CanalEntry.Entry data) throws InterruptedException {
        // 1. 首先获取一个数组索引位置，putSequence.get()为上次已经放入数据的索引位置，本次放入时，先获取要放入位置是否允许放入，即调用
        // checkFreeSlotAt()方法进行判断
        if (checkFreeSlotAt(putSequence.get() + 1)) {
            long current = putSequence.get();
            long next = current + 1;

            // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringbuffer中的老的Entry值
            entries[getIndex(next)] = data;
            // putSequence放入的索引未知会一直保持自增，且最终会超过bufferSize = 1024，其初始值为-1
            putSequence.set(next);
        } else {
            flush();// buffer区满了，刷新一下
            put(data);// 继续加一下新数据
        }
    }

    /**
     * 查询是否有空位
     * @param sequence 当前所要校验的位置索引序号
     */
    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;
        if (wrapPoint > flushSequence.get()) { // 刚好追上一轮
            return false;
        } else {
            return true;
        }
    }

    private void flush() throws InterruptedException {
        // flushSequence 和 putSequence 均从-1开始计算，所以刷新数据时，flushSequence得到的待刷新的索引位置是必须要小于等于putSequence
        // 索引未知，即是一种我要先put放数据，才能flush数据
        long start = this.flushSequence.get() + 1;
        long end = this.putSequence.get();

        // 待刷新的位置 小于等于 已经放入元素的索引未知，即可执行刷新索引位置数据操作
        if (start <= end) {
            // 收集 start 到 end 之间的数据，并放入canalEntrys中，即该数据最终是用于mq消费的
            List<CanalEntry.Entry> canalEntrys = new ArrayList<>();
            for (long next = start; next <= end; next++) {
                // getIndex(next)方法通过next与数组长度取模保证索引位置用于介于0～1024之间，保证能够获取到正确位置的元素
                canalEntrys.add(this.entries[getIndex(next)]);
            }

            // 通过flushCallback开始消费数据，数据来自【MysqlMultiStageCoprocessor】最后一阶段【SinkStoreStage】中的transactionBuffer.add(event.getEntry())
            // flushCallback定义在AbstractEventParser的构造函数中, 通过该回调函数flush()方法将数据推入EntryEventSink中
            flushCallback.flush(canalEntrys);
            flushSequence.set(end);// flush成功后，更新flush位置为最新已经刷新可被消费的位置
        }
    }

    public void reset() {
        putSequence.set(INIT_SQEUENCE);
        flushSequence.set(INIT_SQEUENCE);
    }

    private int getIndex(long sequcnce) {
        return (int) sequcnce & indexMask;
    }

    private boolean isDml(EventType eventType) {
        return eventType == EventType.INSERT || eventType == EventType.UPDATE || eventType == EventType.DELETE;
    }

    // ================ setter / getter ==================

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setFlushCallback(TransactionFlushCallback flushCallback) {
        this.flushCallback = flushCallback;
    }

    /**
     * 事务刷新机制
     * 
     * @author jianghang 2012-12-6 上午11:57:38
     * @version 1.0.0
     */
    public static interface TransactionFlushCallback {

        public void flush(List<CanalEntry.Entry> transaction) throws InterruptedException;
    }

}
