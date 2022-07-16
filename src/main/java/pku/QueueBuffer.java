package pku;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class QueueBuffer {

    public QueueBuffer() {

    }
    private static final int BufferSize = 2048;
    private static final int channelNum = 60;

    private static AtomicLong positionInput = new AtomicLong();

    private volatile boolean firstGet = true;

    private ByteBuffer buffer = ByteBuffer.allocateDirect(BufferSize);
    private BufferControl bufferControl = new BufferControl(buffer);
    private int queueLength = 0;
    private static ChannelManager channelManager;

    static {
        FileChannel[] channels = new FileChannel[channelNum];
        for (int i = 0; i < channelNum; i++) {
            try {
                File file=new File("data");
                if(!file.exists()){
                    file.mkdir();
                }
                RandomAccessFile randomAccessFile = new RandomAccessFile("data/queue_data", "rw");
                channels[i] = randomAccessFile.getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        channelManager = new ChannelManager(channels);
    }

    private BufferTableManager bufferTable = new BufferTableManager();

    public void put(byte[] message) {
        while (message.length + 2 > buffer.remaining()) {
            flush();
        }
        buffer.putShort((short) message.length);
        buffer.put(message);
        this.queueLength++;
    }

    private void flush() {
        long inputPos = positionInput.getAndIncrement();
        bufferTable.add(inputPos * BufferSize, queueLength - 1);
        buffer.clear();
        try {
            FileChannel channel = channelManager.getChannel();
            channel.write(buffer, inputPos * BufferSize);
            channelManager.freeChannel(channel);
        } catch (IOException e) {
            e.printStackTrace();
        }

        buffer.clear();
    }

    public synchronized Collection<byte[]> get(long offset, long num) {
        if (firstGet) {
            flush();
            firstGet = false;
        }

        List<byte[]> result = new ArrayList<>();

        long totalRead = 0;
        long current = offset;

        for (int i = 0; i < num; i++) {
            TableItemInfo itemWrapper = bufferTable.offsetInFile(current);
            if (itemWrapper == null) {
                return result;
            }
            buffer.clear();
            //读盘
            try {
                FileChannel channel = channelManager.getChannel();
                channel.read(buffer, itemWrapper.offset);
                channelManager.freeChannel(channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.clear();

            if (i == 0) {
                bufferControl.skip(itemWrapper.offsetInBlock);
            }

            for (; current <= itemWrapper.lastMessageCount; current++) {
                if (totalRead == num) {
                    return result;
                }
                byte[] message = bufferControl.nextMessage();

                result.add(message);
                totalRead++;
            }
            buffer.clear();
        }
        return result;
    }


    private static class BufferTableManager {
        private List<TableItem> tableItems = new ArrayList<>();

        public synchronized void add(long offset, int lastMessageCount) {
            tableItems.add(new TableItem(offset, lastMessageCount));
        }


        public TableItemInfo offsetInFile(long offset) {
            long startInBlock = 0;
            for (int i = 0; i < tableItems.size(); i++) {
                TableItem tableItem = tableItems.get(i);
                if (tableItem.lastMessageCount < offset) {
                    startInBlock = tableItem.lastMessageCount + 1;
                } else {
                    return new TableItemInfo((int) (offset - startInBlock), tableItem.offset, tableItem.lastMessageCount);
                }
            }

            return null;
        }
    }

    public static class TableItemInfo {
        public int offsetInBlock;
        public long offset;
        public long lastMessageCount;

        public TableItemInfo(int offsetInBlock, long offset, long lastMessageCount) {
            this.offsetInBlock = offsetInBlock;
            this.offset = offset;
            this.lastMessageCount = lastMessageCount;
        }
    }


    private static class TableItem {

        public long offset;
        public int lastMessageCount;

        public TableItem(long offset, int lastMessageCount) {
            this.offset = offset;
            this.lastMessageCount = lastMessageCount;
        }
    }


    private static class BufferControl {
        ByteBuffer buffer;

        public BufferControl(ByteBuffer buffer) {
            this.buffer = buffer;
        }


        public void skip(int n) {
            for (int i = 0; i < n; i++) {
                short messageLength = buffer.getShort();
                int oldPos = buffer.position();
                buffer.position(oldPos + messageLength);
            }
        }

        public byte[] nextMessage() {
            short messageLength = buffer.getShort();
            byte[] message = new byte[messageLength];
            for(int i = 0;i<messageLength;++i){
                message[i] = buffer.get();
            }
            return message;
        }
    }


    public static class ChannelManager {
        private Semaphore semaphore;
        private HashMap<FileChannel, Boolean> channelState = new HashMap<>();

        public ChannelManager(FileChannel[] channels) {
            semaphore = new Semaphore(channels.length);
            for (FileChannel channel : channels) {
                channelState.put(channel, false);
            }
        }

        public FileChannel getChannel() {
            FileChannel target = null;
            try {
                semaphore.acquire();
                target = findFree();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return target;
        }

        public synchronized void freeChannel(FileChannel channel) {
            channelState.put(channel, false);
            semaphore.release();
        }

        private synchronized FileChannel findFree() {
            for (FileChannel channel : channelState.keySet()) {
                if (!channelState.get(channel)) {
                    channelState.put(channel, true);
                    return channel;
                }
            }
            return null;
        }
    }
}
