package com.hazelnut.utils;

import java.nio.ByteBuffer;

/**
 * Utility class to convert other data types to and from byte array
 * byte array is required format to send data over network to ZooKeeper
 */
public class DataMapper {

    private DataMapper() {
    }

    private static final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    /**
     * Convert long to byte array
     *
     * @param value
     * @return
     */
    public static byte[] longToBytes(long value) {
        buffer.putLong(0, value);
        return buffer.array();
    }

    /**
     * Convert byte array to long
     *
     * @param bytes
     * @return
     */
    public static long bytesToLong(byte[] bytes) {
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }

    /**
     * Convert boolean to byte array
     *
     * @param bool
     * @return
     */
    public static byte[] booleanToBytes(boolean bool) {
        return new byte[]{(byte) (bool ? 1 : 0)};
    }

    /**
     * convert byte array to boolean
     *
     * @param bytes
     * @return
     */
    public static boolean bytesToBoolean(byte[] bytes) {
        return bytes.length == 1 && (byte) 1 == bytes[0];
    }


}
