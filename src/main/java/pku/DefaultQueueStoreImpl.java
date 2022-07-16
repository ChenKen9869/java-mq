package pku;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();
    private Map<String, QueueBuffer> queueMaps = new ConcurrentHashMap<>();

    public DefaultQueueStoreImpl(){
        this.queueMaps = new ConcurrentHashMap<String, QueueBuffer>();
    }

    @Override
    public void put(String queueName, byte[] message) {
        QueueBuffer BZqueue = queueMaps.get(queueName);
        if (BZqueue == null) {
            synchronized (this) {
                // 双重检测
                BZqueue = queueMaps.get(queueName);
                if (BZqueue == null) {
                    BZqueue = new QueueBuffer();
                    queueMaps.put(queueName, BZqueue);
                }
            }
        }

        BZqueue.put(message);

    }

    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        QueueBuffer BZqueue = queueMaps.get(queueName);
        if (BZqueue == null) {
            return EMPTY;
        }
        return BZqueue.get(offset, num);
    }
}
