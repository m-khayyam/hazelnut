package com.hazelnut.node.preps;

import com.hazelnut.cluster.ZkDataStore;
import com.hazelnut.node.NodeLivenessReporter;
import com.hazelnut.node.NodeStartup;
import com.hazelnut.utils.ZkClientSupplier;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;

public class NodeLivenessReporterTestUtils extends CommonTestUtils {

    @MockBean
    protected ZkClientSupplier zkClientSupplier;

    @MockBean
    protected ZkDataStore zkDataStore;

    @MockBean
    protected NodeStartup service;

    @Autowired
    protected NodeLivenessReporter reporter;


    protected void verifyHeartBeatCallIsMade() {
        Mockito.verify(zkDataStore, times(EXACTLY_ONCE)).setNodeHeartBeatTime(anyString(), anyLong(), anyLong());
    }

}
