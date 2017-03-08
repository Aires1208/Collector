package com.navercorp.pinpoint.collector.cluster.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by root on 17-1-12.
 */
public class ZookeeperUtilsTest {
    @Test
    public void should_return_true_when_given_mock_event() throws Exception {
        //given
        WatchedEvent watchedEvent = mock(WatchedEvent.class);

        when(watchedEvent.getState()).thenReturn(Watcher.Event.KeeperState.SyncConnected);
        when(watchedEvent.getType()).thenReturn(Watcher.Event.EventType.None);
        boolean isConnectedEvent = ZookeeperUtils.isConnectedEvent(watchedEvent);

        assertTrue(isConnectedEvent);
    }

    @Test
    public void should_return_false_when_given_mock_event() throws Exception {
        //given
        WatchedEvent watchedEvent = mock(WatchedEvent.class);

        //when
        when(watchedEvent.getState()).thenReturn(Watcher.Event.KeeperState.SyncConnected);
        when(watchedEvent.getType()).thenReturn(Watcher.Event.EventType.NodeCreated);
        boolean isConnectedEvent = ZookeeperUtils.isConnectedEvent(watchedEvent);

        //then
        assertFalse(isConnectedEvent);
    }

    @Test
    public void should_return_disconnect_true_when_given_mock_event() throws Exception {
        //given
        WatchedEvent watchedEvent = mock(WatchedEvent.class);

        //when
        when(watchedEvent.getState()).thenReturn(Watcher.Event.KeeperState.Disconnected);
        when(watchedEvent.getType()).thenReturn(Watcher.Event.EventType.None);
        boolean isDisconnected = ZookeeperUtils.isDisconnectedEvent(watchedEvent);

        //then
        assertTrue(isDisconnected);
    }

    @Test
    public void should_return_disconnect_false_when_given_mock_event() throws Exception {
        //given
        WatchedEvent watchedEvent = mock(WatchedEvent.class);

        //when
        when(watchedEvent.getState()).thenReturn(Watcher.Event.KeeperState.SyncConnected);
        when(watchedEvent.getType()).thenReturn(Watcher.Event.EventType.None);
        boolean isDisconnected = ZookeeperUtils.isDisconnectedEvent(watchedEvent);

        //then
        assertFalse(isDisconnected);
    }

}