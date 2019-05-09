    /*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.async.ConnectionType;
import org.apache.cassandra.net.async.FutureCombiner;
import org.apache.cassandra.net.async.InboundConnectionSettings;
import org.apache.cassandra.net.async.InboundSockets;
import org.apache.cassandra.net.async.InboundMessageHandlers;
import org.apache.cassandra.net.async.OutboundConnectionSettings;
import org.apache.cassandra.net.async.OutboundConnections;
import org.apache.cassandra.net.async.SocketFactory;
import org.apache.cassandra.net.async.ResourceLimits;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.MUTATION;
import static org.apache.cassandra.utils.Throwables.maybeFail;

public final class MessagingService extends MessagingServiceMBeanImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MessagingService.class);

    // 8 bits version, so don't waste versions
    public static final int VERSION_30 = 10;
    public static final int VERSION_3014 = 11;
    public static final int VERSION_40 = 12;
    public static final int minimum_version = VERSION_30;
    public static final int current_version = VERSION_40;
    public static AcceptVersions accept_messaging = new AcceptVersions(minimum_version, current_version);
    public static AcceptVersions accept_streaming = new AcceptVersions(current_version, current_version);

    public static final byte[] ONE_BYTE = new byte[1];

    private static class MSHandle
    {
        public static final MessagingService instance = new MessagingService(false);
    }
    public static MessagingService instance()
    {
        return MSHandle.instance;
    }

    public final SocketFactory socketFactory = new SocketFactory();
    public final LatencySubscribers latencySubscribers = new LatencySubscribers();
    public final RemoteCallbacks callbacks = new RemoteCallbacks(this);

    // a public hook for filtering messages intended for delivery to this node
    public final InboundSink inboundSink = new InboundSink(this);

    // the inbound global reserve limits and associated wait queue
    private final InboundMessageHandlers.GlobalResourceLimits inboundGlobalReserveLimits = new InboundMessageHandlers.GlobalResourceLimits(
        new ResourceLimits.Concurrent(DatabaseDescriptor.getInternodeApplicationReserveReceiveQueueGlobalCapacityInBytes()));

    // the socket bindings we accept incoming connections on
    private final InboundSockets inboundSockets = new InboundSockets(new InboundConnectionSettings()
                                                                     .withHandlers(this::getInbound)
                                                                     .withSocketFactory(socketFactory));

    // a public hook for filtering messages intended for delivery to another node
    public final OutboundSink outboundSink = new OutboundSink(this::doSendOneWay);

    public final ResourceLimits.Limit outboundGlobalReserveLimit =
        new ResourceLimits.Concurrent(DatabaseDescriptor.getInternodeApplicationReserveSendQueueGlobalCapacityInBytes());

    // back-pressure implementation
    private final BackPressureStrategy backPressure = DatabaseDescriptor.getBackPressureStrategy();

    private volatile boolean isShuttingDown;

    @VisibleForTesting
    MessagingService(boolean testOnly)
    {
        super(testOnly);
        OutboundConnections.scheduleUnusedConnectionMonitoring(this, ScheduledExecutors.scheduledTasks, 1L, TimeUnit.HOURS);
    }

    /**
     * Updates the back-pressure state on sending to the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param callback The message callback.
     * @param message The actual message.
     */
    public void updateBackPressureOnSend(InetAddressAndPort host, IAsyncCallback callback, Message<?> message)
    {
        if (DatabaseDescriptor.backPressureEnabled() && callback.supportsBackPressure())
        {
            BackPressureState backPressureState = getBackPressureState(host);
            if (backPressureState != null)
                backPressureState.onMessageSent(message);
        }
    }

    /**
     * Updates the back-pressure state on reception from the given host if enabled and the given message callback supports it.
     *
     * @param host The replica host the back-pressure state refers to.
     * @param callback The message callback.
     * @param timeout True if updated following a timeout, false otherwise.
     */
    public void updateBackPressureOnReceive(InetAddressAndPort host, IAsyncCallback callback, boolean timeout)
    {
        if (DatabaseDescriptor.backPressureEnabled() && callback.supportsBackPressure())
        {
            BackPressureState backPressureState = getBackPressureState(host);
            if (backPressureState == null)
                return;
            if (!timeout)
                backPressureState.onResponseReceived();
            else
                backPressureState.onResponseTimeout();
        }
    }

    /**
     * Applies back-pressure for the given hosts, according to the configured strategy.
     *
     * If the local host is present, it is removed from the pool, as back-pressure is only applied
     * to remote hosts.
     *
     * @param hosts The hosts to apply back-pressure to.
     * @param timeoutInNanos The max back-pressure timeout.
     */
    public void applyBackPressure(Iterable<InetAddressAndPort> hosts, long timeoutInNanos)
    {
        if (DatabaseDescriptor.backPressureEnabled())
        {
            Set<BackPressureState> states = new HashSet<>();
            for (InetAddressAndPort host : hosts)
            {
                if (host.equals(FBUtilities.getBroadcastAddressAndPort()))
                    continue;
                states.add(getOutbound(host).getBackPressureState());
            }
            backPressure.apply(states, timeoutInNanos, NANOSECONDS);
        }
    }

    BackPressureState getBackPressureState(InetAddressAndPort host)
    {
        return getOutbound(host).getBackPressureState();
    }

    void markExpiredCallback(InetAddressAndPort addr)
    {
        OutboundConnections conn = channelManagers.get(addr);
        if (conn != null)
            conn.incrementExpiredCallbackCount();
    }

    /**
     * Only to be invoked once we believe the endpoint will never be contacted again.
     *
     * We close the connection after a five minute delay, to give asynchronous operations a chance to terminate
     */
    public void closeOutbound(InetAddressAndPort to)
    {
        OutboundConnections pool = channelManagers.get(to);
        if (pool != null)
            pool.scheduleClose(5L, MINUTES, true)
                .addListener(future -> channelManagers.remove(to, pool));
    }

    /**
     * Only to be invoked once we believe the connections will never be used again.
     */
    public void closeOutboundNow(OutboundConnections connections)
    {
        connections.close(true).addListener(
            future -> channelManagers.remove(connections.template().to, connections)
        );
    }

    /**
     * Only to be invoked once we believe the connections will never be used again.
     */
    public void removeInbound(InetAddressAndPort from)
    {
        InboundMessageHandlers handlers = messageHandlers.remove(from);
        if (null != handlers)
            handlers.releaseMetrics();
    }

    /**
     * Closes any current open channel/connection to the endpoint, but does not cause any message loss, and we will
     * try to re-establish connections immediately
     */
    public void interruptOutbound(InetAddressAndPort to)
    {
        OutboundConnections pool = channelManagers.get(to);
        if (pool != null)
            pool.interrupt();
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param address IP Address to identify the peer
     * @param preferredAddress IP Address to use (and prefer) going forward for connecting to the peer
     */
    public Future<Void> maybeReconnectWithNewIp(InetAddressAndPort address, InetAddressAndPort preferredAddress)
    {
        if (!SystemKeyspace.updatePreferredIP(address, preferredAddress))
            return null;

        OutboundConnections messagingPool = channelManagers.get(address);
        if (messagingPool != null)
            return messagingPool.reconnectWithNewIp(preferredAddress);

        return null;
    }

    public long sendRRWithFailure(Message message, InetAddressAndPort to, IAsyncCallbackWithFailure cb)
    {
        return sendRR(message, to, cb);
    }

    /**
     * Send a non-mutation message to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param cb      callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public long sendRR(Message message, InetAddressAndPort to, IAsyncCallback cb)
    {
        return sendRR(message, to, cb, null);
    }

    public long sendRR(Message message, InetAddressAndPort to, IAsyncCallback cb, ConnectionType specifyConnection)
    {
        long id = callbacks.addWithExpiration(cb, message, to);
        updateBackPressureOnSend(to, cb, message);
        sendOneWay(cb instanceof IAsyncCallbackWithFailure<?> ? message.withIdAndFlag(id, MessageFlag.CALL_BACK_ON_FAILURE)
                                                              : message.withId(id), to, specifyConnection);
        return id;
    }

    /**
     * Send a mutation message or a Paxos Commit to a given endpoint. This method specifies a callback
     * which is invoked with the actual response.
     * Also holds the message (only mutation messages) to determine if it
     * needs to trigger a hint (uses StorageProxy for that).
     *
     * @param message message to be sent.
     * @param to      endpoint to which the message needs to be sent
     * @param handler callback interface which is used to pass the responses or
     *                suggest that a timeout occurred to the invoker of the send().
     * @return an reference to message id used to match with the result
     */
    public long sendWriteRR(Message<?> message,
                           Replica to,
                           AbstractWriteResponseHandler<?> handler,
                           boolean allowHints)
    {
        return sendWriteRR(message, to, handler, allowHints, null);
    }

    public long sendWriteRR(Message<?> message,
                           Replica to,
                           AbstractWriteResponseHandler<?> handler,
                           boolean allowHints,
                           ConnectionType specifyConnection)
    {
        long id = callbacks.addWithExpiration(handler, message, to, handler.consistencyLevel(), allowHints);
        updateBackPressureOnSend(to.endpoint(), handler, message);
        sendOneWay(message.withIdAndFlag(id, MessageFlag.CALL_BACK_ON_FAILURE), to.endpoint(), specifyConnection);
        return id;
    }

    public void sendResponse(Message message, InetAddressAndPort to)
    {
        sendResponse(message, to, null);
    }

    public void sendResponse(Message message, InetAddressAndPort to, ConnectionType specifyConnection)
    {
        sendOneWay(message, to, specifyConnection);
    }

    /**
     * Send a message to a given endpoint. This method adheres to the fire and forget
     * style messaging.
     *
     * @param message messages to be sent.
     * @param to      endpoint to which the message needs to be sent
     */
    public void sendOneWay(Message message, InetAddressAndPort to)
    {
        sendOneWay(message, to, null);
    }
    public void sendOneWay(Message message, InetAddressAndPort to, ConnectionType specifyConnection)
    {
        if (logger.isTraceEnabled())
            logger.trace("{} sending {} to {}@{}", FBUtilities.getBroadcastAddressAndPort(), message.verb(), message.id(), to);

        if (to.equals(FBUtilities.getBroadcastAddressAndPort()))
            logger.trace("Message-to-self {} going over MessagingService", message);

        outboundSink.accept(message, to, specifyConnection);
    }

    private void doSendOneWay(Message message, InetAddressAndPort to, ConnectionType specifyConnection)
    {
        // expire the callback if the message failed to enqueue (failed to establish a connection or exceeded queue capacity)
        while (true)
        {
            OutboundConnections connections = getOutbound(to);
            try
            {
                connections.enqueue(message, specifyConnection);
                return;
            }
            catch (ClosedChannelException e)
            {
                if (isShuttingDown)
                    return; // just drop the message, and let others clean up

                // remove the connection and try again
                channelManagers.remove(to, connections);
            }
        }
    }

    public <T> AsyncOneResponse<T> sendRR(Message message, InetAddressAndPort to)
    {
        AsyncOneResponse<T> iar = new AsyncOneResponse<>();
        sendRR(message, to, iar);
        return iar;
    }

    /**
     * Wait for callbacks and don't allow any more to be created (since they could require writing hints)
     */
    public void shutdown()
    {
        shutdown(1L, MINUTES, true, true);
    }

    public void shutdown(long timeout, TimeUnit units, boolean shutdownGracefully, boolean shutdownExecutors)
    {
        isShuttingDown = true;
        logger.info("Waiting for messaging service to quiesce");
        // We may need to schedule hints on the mutation stage, so it's erroneous to shut down the mutation stage first
        assert !StageManager.getStage(MUTATION).isShutdown();

        if (shutdownGracefully)
        {
            callbacks.shutdownGracefully();
            List<Future<Void>> closing = new ArrayList<>();
            for (OutboundConnections pool : channelManagers.values())
                closing.add(pool.close(true));

            long deadline = System.nanoTime() + units.toNanos(timeout);
            maybeFail(() -> new FutureCombiner(closing).get(timeout, units),
                      () -> inboundSockets.close().get(),
                      () -> {
                          if (shutdownExecutors)
                              shutdownExecutors(deadline);
                      },
                      () -> callbacks.awaitTerminationUntil(deadline),
                      inboundSink::clear,
                      outboundSink::clear);
        }
        else
        {
            callbacks.shutdownNow(false);
            List<Future<Void>> closing = new ArrayList<>();
            closing.add(inboundSockets.close());
            for (OutboundConnections pool : channelManagers.values())
                closing.add(pool.close(false));

            long deadline = System.nanoTime() + units.toNanos(timeout);
            maybeFail(() -> new FutureCombiner(closing).get(timeout, units),
                      () -> {
                          if (shutdownExecutors)
                              shutdownExecutors(deadline);
                      },
                      () -> callbacks.awaitTerminationUntil(deadline),
                      inboundSink::clear,
                      outboundSink::clear);
        }
    }

    private void shutdownExecutors(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        socketFactory.shutdownNow();
        socketFactory.awaitTerminationUntil(deadlineNanos);
    }

    private OutboundConnections getOutbound(InetAddressAndPort to)
    {
        OutboundConnections connections = channelManagers.get(to);
        if (connections == null)
            connections = OutboundConnections.tryRegister(channelManagers, to, new OutboundConnectionSettings(to), backPressure.newState(to));
        return connections;
    }

    public InboundMessageHandlers getInbound(InetAddressAndPort from)
    {
        InboundMessageHandlers handlers = messageHandlers.get(from);
        if (null != handlers)
            return handlers;

        return messageHandlers.computeIfAbsent(from, addr ->
            new InboundMessageHandlers(FBUtilities.getLocalAddressAndPort(),
                                       addr,
                                       DatabaseDescriptor.getInternodeApplicationReceiveQueueCapacityInBytes(),
                                       DatabaseDescriptor.getInternodeApplicationReserveReceiveQueueEndpointCapacityInBytes(),
                                       inboundGlobalReserveLimits, metrics, inboundSink)
        );
    }

    @VisibleForTesting
    boolean isConnected(InetAddressAndPort address, Message<?> messageOut)
    {
        OutboundConnections pool = channelManagers.get(address);
        if (pool == null)
            return false;
        return pool.connectionFor(messageOut).isConnected();
    }

    public void listen()
    {
        inboundSockets.open();
    }

    public void waitUntilListening() throws InterruptedException
    {
        inboundSockets.open().await();
    }
}
