package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.parse.driver.mysql.socket.SocketChannel;
import com.taobao.tddl.dbsync.binlog.LogFetcher;

/**
 * 基于socket的logEvent实现，该类主要负责的是解析master推送过来的二进制binlog流数据，按照mysql协议进行字节数据拆解
 * 
 * @author jianghang 2013-1-14 下午07:39:30
 * @version 1.0.0
 */
public class DirectLogFetcher extends LogFetcher {

    protected static final Logger logger                          = LoggerFactory.getLogger(DirectLogFetcher.class);

    // Master heartbeat interval
    public static final int       MASTER_HEARTBEAT_PERIOD_SECONDS = 15;
    // +10s 确保 timeout > heartbeat interval
    private static final int      READ_TIMEOUT_MILLISECONDS       = (MASTER_HEARTBEAT_PERIOD_SECONDS + 10) * 1000;

    /** Command to dump binlog */
    public static final byte      COM_BINLOG_DUMP                 = 18;

    /** Packet header sizes */
    public static final int       NET_HEADER_SIZE                 = 4;
    public static final int       SQLSTATE_LENGTH                 = 5;

    /** Packet offsets */
    public static final int       PACKET_LEN_OFFSET               = 0;
    public static final int       PACKET_SEQ_OFFSET               = 3;

    /** Maximum packet length mysql binlog协议中规定的一个大包的最大字节大小，即16MB - 1 */
    public static final int       MAX_PACKET_LENGTH               = (256 * 256 * 256 - 1);

    private SocketChannel         channel;

    private boolean               issemi                          = false;

    // private BufferedInputStream input;

    public DirectLogFetcher(){
        super(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity){
        super(initialCapacity, DEFAULT_GROWTH_FACTOR);
    }

    public DirectLogFetcher(final int initialCapacity, final float growthFactor){
        super(initialCapacity, growthFactor);
    }

    public void start(SocketChannel channel) throws IOException {
        this.channel = channel;
        String dbsemi = System.getProperty("db.semi");
        if ("1".equals(dbsemi)) {
            issemi = true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#fetch()
     */
    public boolean fetch() throws IOException {
        try {
            // Fetching packet header from input.
            if (!fetch0(0, NET_HEADER_SIZE)) {
                logger.warn("Reached end of input stream while fetching header");
                return false;
            }

            // Fetching the first packet(may a multi-packet).
            int netlen = getUint24(PACKET_LEN_OFFSET); // 读取报文头4个字节的前3个字节数据（即偏移量为0-2，0,1,2），即网络报文长度
            int netnum = getUint8(PACKET_SEQ_OFFSET); // 读取报文头4个字节的第4个字节数据（即偏移量为3），表示的是序列号，该序列号用于保证数据包传输的顺序性和应答
            if (!fetch0(NET_HEADER_SIZE, netlen)) { // 从偏移量为4开始，即第5个字节开始，读取netlen长度的字节流数据，即一个完整 MySQL 协议包的数据部分，读取失败返回false
                logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                return false;
            }

            // Detecting error code. 解析第4个字节的数据，共计8为二进制数，即-127～127数据范围
            final int mark = getUint8(NET_HEADER_SIZE);
            // mark == 0 表示这是一个正常的数据包（OK Packet），包含了有效的 binlog 事件数据或其他信息。fetch() 方法会调整缓冲区指针，
            // 为后续解析做好准备，并返回 true 告知调用者已成功获取到一个有效的数据包
            if (mark != 0) {
                if (mark == 255) // error from master, 表示这是一个错误包（Error Packet）。这时，fetch() 方法会解析出错误码（errno）、
                    // SQL 状态（sqlstate）和错误信息（errmsg），然后抛出 IOException。例如，常见的错误 errno = 1236 (SQLSTATE: HY000)
                    // 表示无法在 binlog index 文件中找到指定的日志文件，这通常发生在指定的 binlog 文件已被清除或位置不正确时
                {
                    // Indicates an error, for example trying to fetch from
                    // wrong
                    // binlog position.
                    position = NET_HEADER_SIZE + 1;
                    final int errno = getInt16();
                    String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
                    String errmsg = getFixString(limit - position);
                    throw new IOException("Received error packet:" + " errno = " + errno + ", sqlstate = " + sqlstate
                                          + " errmsg = " + errmsg);
                } else if (mark == 254) { // 表示这是一个 EOF 包（End Of File Packet）。通常用于标识数据流的结束，或者在某些情况下
                        // 主库主动断开连接。fetch() 方法会记录警告 "Received EOF packet from server, apparent master disconnected"
                        // 并返回 false。这可能意味着主库连接正常结束，或者由于 duplicate slave ID 等问题导致主库拒绝了连接
                    // Indicates end of stream. It's not clear when this would
                    // be sent.
                    logger.warn("Received EOF packet from server, apparent"
                                + " master disconnected. It's may be duplicate slaveId , check instance config");
                    return false;
                } else {
                    // Should not happen.
                    throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
                                          + ", len = " + netlen);
                }
            }

            // if mysql is in semi mode
            if (issemi) {
                // parse semi mark
                int semimark = getUint8(NET_HEADER_SIZE + 1);
                int semival = getUint8(NET_HEADER_SIZE + 2);
                this.semival = semival;
            }

            // The first packet is a multi-packet, concatenate the packets. 如果第一个包就是个大包，那么就会进入while循环读取，注意
            // while中的代码会从头又开始完成一次重新读取，可以说明上面的预读取都会被while中的读取重新覆盖，交由while来完整读取，因为是一个大包
            while (netlen == MAX_PACKET_LENGTH) {
                if (!fetch0(0, NET_HEADER_SIZE)) {
                    logger.warn("Reached end of input stream while fetching header");
                    return false;
                }

                netlen = getUint24(PACKET_LEN_OFFSET);
                netnum = getUint8(PACKET_SEQ_OFFSET);
                if (!fetch0(limit, netlen)) {
                    logger.warn("Reached end of input stream: packet #" + netnum + ", len = " + netlen);
                    return false;
                }
            }

            // Preparing buffer variables to decoding.
            if (issemi) {
                origin = NET_HEADER_SIZE + 3;
            } else {
                // 关于：为什么是 NET_HEADER_SIZE + 1？
                /**
                 * 结合以上分析，当代码执行到 origin = NET_HEADER_SIZE + 1; 时，意味着同时满足了以下三个条件：
                 *     成功读取到了一个完整的 MySQL 网络包。
                 *     该包是 OK Packet (mark == 0)。
                 *     不处于半同步模式 (issemi == false)。
                 *     该包不是一个大包 (netlen < MAX_PACKET_LENGTH)，因此没有进入 while 循环。
                 *  在这种情况下，origin 被设置为 NET_HEADER_SIZE + 1 的原因非常明确：
                 *      NET_HEADER_SIZE (值为4)： 跳过标准的字节 MySQL 网络协议包头。包头包含了包长度和序列号信息，对解析 Binlog 事件
                 *      本身无用。+ 1： 跳过 OK Packet 包体的第一个字节。这个字节就是之前检查的 mark。在 OK Packet 中，mark=0 这个字
                 *      节是一个包头标识符，它本身并不是 Binlog 事件数据的一部分。因此，NET_HEADER_SIZE + 1 = 4 + 1 = 5。这个计算就是
                 *      为了将 LogBuffer 的起始指针 origin 精确地定位到 ​OK Packet 中所承载的实际 Binlog 事件数据的第一个字节上。
                 * */
                origin = NET_HEADER_SIZE + 1;
            }
            position = origin;
            limit -= origin;
            return true;
        } catch (SocketTimeoutException e) {
            close(); /* Do cleanup */
            logger.error("Socket timeout expired, closing connection", e);
            throw e;
        } catch (InterruptedIOException | ClosedByInterruptException e) {
            close(); /* Do cleanup */
            logger.info("I/O interrupted while reading from client socket", e);
            throw e;
        } catch (IOException e) {
            close(); /* Do cleanup */
            logger.error("I/O error while reading from client socket", e);
            throw e;
        }
    }

    private final boolean fetch0(final int off, final int len) throws IOException {
        ensureCapacity(off + len);

        // byte[] read = channel.read(len, READ_TIMEOUT_MILLISECONDS);
        // System.arraycopy(read, 0, this.buffer, off, len);
        // 从off位置开始写入，一共写len长度个字节到LogBuffer中的buffer
        channel.read(buffer, off, len, READ_TIMEOUT_MILLISECONDS);
        if (limit < off + len) {
            limit = off + len;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.taobao.tddl.dbsync.binlog.LogFetcher#close()
     */
    public void close() throws IOException {
        // do nothing
    }

}
