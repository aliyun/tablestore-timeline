package examples.v2;

import java.util.concurrent.atomic.AtomicLong;

public class MessageIdGenerator {

    public MessageIdGenerator() {
    }

    /**
     *
     * 0(1bit) + timestamp(ms, 43bit) + seqId(20bit)
     *
     * @param lastId
     * @return
     */
    public static long next(long lastId) {
        long newId = System.currentTimeMillis() << 20;
        if (newId <= lastId) {
            return lastId + 1;
        } else {
            return newId;
        }
    }

    public static void main(String[] args) {
        long lastId = 0;
        for (int i = 0; i < 10000; i++) {
            lastId = MessageIdGenerator.next(lastId);
            System.out.println(lastId);
        }
    }
}
