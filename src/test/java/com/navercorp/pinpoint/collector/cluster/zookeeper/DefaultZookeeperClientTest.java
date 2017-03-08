package com.navercorp.pinpoint.collector.cluster.zookeeper;

import com.navercorp.pinpoint.collector.cluster.zookeeper.exception.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 17-1-14.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DefaultZookeeperClient.class)
public class DefaultZookeeperClientTest {
    ZookeeperEventWatcher watcher = PowerMockito.mock(ZookeeperEventWatcher.class);
    ZooKeeper zooKeeper = PowerMockito.mock(ZooKeeper.class);
    DefaultZookeeperClient zookeeperClient = new DefaultZookeeperClient("0:0", 1000, watcher);

    @Before
    public void setUp() throws Exception {
        PowerMockito.whenNew(ZooKeeper.class).withArguments(Mockito.anyString(), Mockito.anyInt(), Mockito.any(Watcher.class)).thenReturn(zooKeeper);
//        zookeeperClient.connect();
    }

    @Test
    public void should_invoke_fail_create_zk_instance_when_reconnectWhenSessionExpired() throws Exception {
        //given
        zookeeperClient.connect();

        //when
        PowerMockito.when(zooKeeper.getState()).thenReturn(ZooKeeper.States.NOT_CONNECTED);
        PowerMockito.whenNew(ZooKeeper.class).withArguments(Mockito.anyString(), Mockito.anyInt(), Mockito.any(Watcher.class)).thenReturn(null);
        zookeeperClient.reconnectWhenSessionExpired();

        //then
        PowerMockito.verifyNew(ZooKeeper.class);
        zookeeperClient.close();
    }

    @Test
    public void should_throw_InterruptedException_when_invoke_reconnect() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(zooKeeper.getState()).thenReturn(ZooKeeper.States.NOT_CONNECTED);
        PowerMockito.doThrow(new InterruptException("")).when(zooKeeper).close();

        try {
            zookeeperClient.reconnectWhenSessionExpired();
        } catch (Exception e) {
            assertTrue(e instanceof InterruptException);
        }
    }

    @Test
    public void should_throw_ConnectionException_when_createPath() throws Exception {
        zookeeperClient.connect();

        try {
            zookeeperClient.createPath("/test");
        } catch (Exception e) {
            assertTrue(e instanceof ConnectionException);
        }
    }

    @Test
    public void should_do_create_when_createPath() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);

        zookeeperClient.createPath("/test/test/test");
    }

    @Test
    public void should_throw_Exception_when_createPath() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.ConnectionLossException()).when(zooKeeper)
                .create(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyList(), Mockito.any(CreateMode.class));
        try {
            zookeeperClient.createPath("/test/test/test");
        } catch (Exception e) {
            assertTrue(e instanceof ConnectionException);
        }
    }

    @Test
    public void should_return_expect_ZNodePaht_when_given_zNodePath() throws Exception {
        zookeeperClient.connect();
        String zNodePath = "/test";

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.when(zooKeeper.exists(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new Stat());

        String expectRes = zookeeperClient.createNode(zNodePath, new byte[1]);

        assertEquals(expectRes, zNodePath);
    }

    @Test
    public void should_throw_AuthException_when_given_zNodePath() throws Exception {
        zookeeperClient.connect();
        String zNodePath = "/test";

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.AuthFailedException()).when(zooKeeper)
                .create(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyList(), Mockito.any(CreateMode.class));

        try {
            zookeeperClient.createNode(zNodePath, new byte[1]);
        } catch (Exception e) {
            assertTrue(e instanceof AuthException);
        }
    }

    @Test
    public void should_return_expect_res_when_input_given_path() throws Exception {
        zookeeperClient.connect();
        byte[] expect = new byte[1];
        expect[0] = 6;

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.when(zooKeeper.getData(Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(Stat.class)))
                .thenReturn(expect);

        byte[] res = zookeeperClient.getData("/path");

        assertEquals(res, expect);
    }

    @Test
    public void should_throw_BadOperationException_when_input_given_path() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.NodeExistsException()).when(zooKeeper)
                .getData(Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(Stat.class));

        try {
            zookeeperClient.getData("/path");
        } catch (Exception e) {
            assertTrue(e instanceof BadOperationException);
        }
    }

    @Test
    public void should_do_not_setData() throws Exception {
        zookeeperClient.connect();
        byte[] expect = new byte[1];
        expect[0] = 6;

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        zookeeperClient.setData("/test", expect);

        Mockito.verify(zooKeeper, Mockito.times(0)).setData(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyInt());
    }

    @Test
    public void should_throw_expetion_when_set_given_data() throws Exception {
        zookeeperClient.connect();
        byte[] expect = new byte[1];
        expect[0] = 6;

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.when(zooKeeper.exists(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new Stat());
        PowerMockito.doThrow(new KeeperException.NoNodeException()).when(zooKeeper)
                .setData(Mockito.anyString(), Mockito.any(byte[].class), Mockito.anyInt());

        try {
            zookeeperClient.setData("/test", expect);
        } catch (Exception e) {
            assertTrue(e instanceof NoNodeException);
        }
    }

    @Test
    public void should_throw_TimeoutException_when_delete_given_path() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.OperationTimeoutException()).when(zooKeeper)
                .delete(Mockito.anyString(), Mockito.anyInt());
        try {
            zookeeperClient.delete("/test");
        } catch (Exception e) {
            assertTrue(e instanceof TimeoutException);
        }
    }

    @Test
    public void should_return_false_when_check_given_path_exists() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);

        boolean exists = zookeeperClient.exists("test");

        assertFalse(exists);
    }

    @Test
    public void should_return_true_when_check_given_path_exists() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.when(zooKeeper.exists(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(new Stat());

        boolean exists = zookeeperClient.exists("test");

        assertTrue(exists);
    }

    @Test
    public void should_throw_UnknownException_when_check_given_path_exists() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.MarshallingErrorException()).when(zooKeeper).exists(Mockito.anyString(), Mockito.anyBoolean());

        try {
            zookeeperClient.exists("/test");
        } catch (Exception e) {
            assertTrue(e instanceof UnknownException);
        }
    }

    @Test
    public void should_return_expect_list_when_getChildrenNode() throws Exception {
        zookeeperClient.connect();
        List<String> excepts = newArrayList("/test", "path");

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.when(zooKeeper.getChildren(Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(Stat.class)))
                .thenReturn(excepts);

        List<String> childrenNode = zookeeperClient.getChildrenNode("/test", false);

        assertEquals(2, childrenNode.size());
        assertEquals(excepts, childrenNode);
    }

    @Test
    public void should_throw_BadOperationException_expect_list_when_getChildrenNode() throws Exception {
        zookeeperClient.connect();

        PowerMockito.when(watcher.isConnected()).thenReturn(true);
        PowerMockito.doThrow(new KeeperException.NotEmptyException()).when(zooKeeper)
                .getChildren(Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(Stat.class));

        try {
            zookeeperClient.getChildrenNode("/test", true);
        } catch (Exception e) {
            assertTrue(e instanceof BadOperationException);
        }
    }
}
