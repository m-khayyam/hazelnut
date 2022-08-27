package com.hazelnut.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(MockitoJUnitRunner.class)
class TestDataMapper {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBooleanToByteAndViceVerse(boolean bool) {
        byte[] boolConvertedBytes = DataMapper.booleanToBytes(bool);
        assertArrayEquals(boolConvertedBytes, new byte[]{(byte) (bool ? 1 : 0)});
        assertEquals(DataMapper.bytesToBoolean(boolConvertedBytes), bool);
    }

    @Test
        //Cluster starting first time and this node gets lock to bootstrap
    void testLongToByteAndViceVerse() {
        Long value = 105687L;
        byte[] longConvertedBytes = DataMapper.longToBytes(value);
        assertArrayEquals(longConvertedBytes, new byte[]{0, 0, 0, 0, 0, 1, -100, -41});
        assertEquals(DataMapper.bytesToLong(longConvertedBytes), value);
    }

}
