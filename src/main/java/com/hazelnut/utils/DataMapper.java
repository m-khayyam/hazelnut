package com.hazelnut.utils;

import java.nio.ByteBuffer;

public class DataMapper {

    private DataMapper() {
    }

    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    public static byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] booleanToBytes(boolean bool) {
        return new byte[]{(byte) (bool ? 1 : 0)};
    }

    public static boolean bytesToBoolean(byte[] bytes) {
        return bytes.length == 1 && (byte) 1 == bytes[0];
    }


}
