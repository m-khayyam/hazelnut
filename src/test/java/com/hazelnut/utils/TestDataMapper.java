package com.hazelnut.utils;

import com.hazelnut.cluster.DistributedLock;
import com.hazelnut.cluster.ZkDataStore;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class TestDataMapper {

    protected @MockBean
    ZkConnectionManager zkConnectionManager;

    protected @MockBean DistributedLock distributedLock;

    protected @MockBean
    ZkDataStore clusterData;

    protected @MockBean
    NodeLivenessReporter reporter;

    @Autowired
    protected NodeStartup service;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBooleanToByteAndViceVerse(boolean bool) throws Exception {
        byte[] boolConvertedBytes = DataMapper.booleanToBytes(bool);

        assertArrayEquals(boolConvertedBytes, new byte[]{(byte) (bool ? 1 : 0)});

        assertEquals(DataMapper.bytesToBoolean(boolConvertedBytes), bool);
    }

    @Test
        //Cluster starting first time and this node gets lock to bootstrap
    void testLongToByteAndViceVerse() throws Exception {

        Long value = 105687l;
        byte[] longConvertedBytes = DataMapper.longToBytes(value);
        assertArrayEquals(longConvertedBytes, new byte[]{0, 0, 0, 0, 0, 1, -100, -41});

        assertEquals(DataMapper.bytesToLong(longConvertedBytes), value);


    }
}
