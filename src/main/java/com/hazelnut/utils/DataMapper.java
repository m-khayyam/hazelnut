package com.hazelnut.utils;

/**
 * Utility class to convert other data types to and from byte array
 * byte array is required format to send data over network to ZooKeeper
 */
public class DataMapper {

    private DataMapper() {
    }

    /**
     * Convert boolean to byte array
     *
     * @param bool to be converted
     * @return the converted as bytes
     */
    public static byte[] booleanToBytes(boolean bool) {
        return new byte[]{(byte) (bool ? 1 : 0)};
    }

    /**
     * convert byte array to boolean
     *
     * @param bytes to be converted
     * @return converted boolean
     */
    public static boolean bytesToBoolean(byte[] bytes) {
        return bytes.length == 1 && (byte) 1 == bytes[0];
    }

}
