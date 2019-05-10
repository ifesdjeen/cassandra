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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.proxy.ProxyServer;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.async.ConnectionTest.SETTINGS;

//public class InJVMProxyConnectionsTest
//{
//    private static final SocketFactory factory = new SocketFactory();
//
//    @BeforeClass
//    public static void startup()
//    {
//        DatabaseDescriptor.daemonInitialization();
//    }
//
//    @After
//    public void resetVerbs() throws Throwable
//    {
//        for (Map.Entry<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> e : serializers.entrySet())
//            e.getKey().unsafeSetSerializer(e.getValue());
//        serializers.clear();
//        for (Map.Entry<Verb, Supplier<? extends IVerbHandler<?>>> e : handlers.entrySet())
//            e.getKey().unsafeSetHandler(e.getValue());
//        handlers.clear();
//        for (Map.Entry<Verb, ToLongFunction<TimeUnit>> e : timeouts.entrySet())
//            e.getKey().unsafeSetExpiration(e.getValue());
//        timeouts.clear();
//    }
//
//    // TODO: factor out
//    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
//    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();
//    private final Map<Verb, ToLongFunction<TimeUnit>> timeouts = new HashMap<>();
//
//    private void unsafeSetSerializer(Verb verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> supplier) throws Throwable
//    {
//        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
//    }
//
//    protected void unsafeSetHandler(Verb verb, Supplier<? extends IVerbHandler<?>> supplier) throws Throwable
//    {
//        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
//    }
//
//    private void unsafeSetExpiration(Verb verb, ToLongFunction<TimeUnit> expiration) throws Throwable
//    {
//        timeouts.putIfAbsent(verb, verb.unsafeSetExpiration(expiration));
//    }
//
//    @Test
//    public void testExpireInbound() throws Throwable
//    {
//        DatabaseDescriptor.forceCrossNodeTimeout();
//        testOneManual((settings, proxy, inbound, outbound, endpoint) -> {
//            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
//
//            CountDownLatch connectionLatch = new CountDownLatch(1);
//            unsafeSetHandler(Verb._TEST_1, () -> v -> {
//                connectionLatch.countDown();
//            });
//            outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//            connectionLatch.await(10, SECONDS);
//            Assert.assertEquals(0, connectionLatch.getCount());
//
//            // Slow things down
//            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(50, MILLISECONDS));
//            proxy.latency(endpoint, 100, MILLISECONDS);
//
//            unsafeSetHandler(Verb._TEST_1, () -> v -> {
//                throw new RuntimeException("Should have not been triggered " + v);
//            });
//            int expireMessages = 10;
//            for (int i = 0; i < expireMessages; i++)
//                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//
//            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
//            waitForCondition(() -> handlers.expiredCount() == expireMessages);
//            System.out.println("handlers.expiredCount() = " + handlers.expiredCount());
//            Assert.assertEquals(expireMessages, handlers.expiredCount());
//        });
//    }
//
//    @Test
//    public void testExpireSome() throws Throwable
//    {
//        DatabaseDescriptor.forceCrossNodeTimeout();
//        testOneManual((settings, proxy, inbound, outbound, endpoint) -> {
//            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
//            connect(outbound);
//
//            AtomicInteger counter = new AtomicInteger();
//            unsafeSetHandler(Verb._TEST_1, () -> v -> {
//                counter.incrementAndGet();
//            });
//
//            int expireMessages = 10;
//            for (int i = 0; i < expireMessages; i++)
//                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//            waitForCondition(() -> counter.get() == 10);
//
//            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(50, MILLISECONDS));
//            proxy.latency(endpoint, 100, MILLISECONDS);
//
//            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
//            for (int i = 0; i < expireMessages; i++)
//                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//            waitForCondition(() -> handlers.expiredCount() == 10);
//
//            proxy.latency(endpoint, 2, MILLISECONDS);
//
//            for (int i = 0; i < expireMessages; i++)
//                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//            waitForCondition(() -> counter.get() == 20);
//        });
//    }
//
//    @Test
//    public void testExpireSomeFromBatch() throws Throwable
//    {
//        DatabaseDescriptor.forceCrossNodeTimeout();
//        testManual((settings, proxy, inbound, outbound, endpoint) -> {
//            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
//            connect(outbound);
//
//            Message msg = Message.out(Verb._TEST_1, 1L);
//            int messageSize = msg.serializedSize(MessagingService.current_version);
//            DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(messageSize * 40);
//
//            AtomicInteger counter = new AtomicInteger();
//            unsafeSetHandler(Verb._TEST_1, () -> v -> {
//                counter.incrementAndGet();
//            });
//
//            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(200, MILLISECONDS));
//            proxy.latency(endpoint, 60, MILLISECONDS);
//
//            int expireMessages = 20;
//            long nanoTime = ApproximateTime.nanoTime();
//            CountDownLatch enqueueDone = new CountDownLatch(1);
//            outbound.unsafeRunOnDelivery(() -> Uninterruptibles.awaitUninterruptibly(enqueueDone, 10, SECONDS));
//            for (int i = 0; i < expireMessages; i++)
//            {
//                boolean expire = i % 2 == 0;
//                outbound.enqueue(Message.builder(Verb._TEST_1, 1L)
//                                        .withCreatedAt(nanoTime)
//                                        .withExpiresAt(nanoTime + (expire ? MILLISECONDS.toNanos(50) : MILLISECONDS.toNanos(100)))
//                                        .build());
//            }
//            enqueueDone.countDown();
//
//            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
//            waitForCondition(() -> handlers.expiredCount() == 10 && counter.get() == 10);
//        });
//    }
//
//    @Test
//    public void suddenDisconnect() throws Throwable
//    {
//        DatabaseDescriptor.forceCrossNodeTimeout();
//        testManual((settings, proxy, inbound, outbound, endpoint) -> {
//            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
//            connect(outbound);
//
//            CountDownLatch closeLatch = new CountDownLatch(1);
//            proxy.closeAfterRead(endpoint, closeLatch::countDown);
//            AtomicInteger counter = new AtomicInteger();
//            unsafeSetHandler(Verb._TEST_1, () -> v -> counter.incrementAndGet());
//
//            outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//            closeLatch.await(10, SECONDS);
//            proxy.forward(endpoint);
//            connect(outbound);
//            Assert.assertTrue(outbound.isConnected());
//        });
//    }
//
//    private static void waitForCondition(Supplier<Boolean> cond) throws Throwable
//    {
//        CompletableFuture.runAsync(() -> {
//            while (!cond.get()) {}
//        }).get(10, SECONDS);
//    }
//
//    private static class FakePayloadSerializer implements IVersionedSerializer<Long>
//    {
//        private final int size;
//        private FakePayloadSerializer()
//        {
//            this(1);
//        }
//
//        // Takes long and repeats it size times
//        private FakePayloadSerializer(int size)
//        {
//            this.size = size;
//        }
//
//        public void serialize(Long i, DataOutputPlus out, int version) throws IOException
//        {
//            for (int j = 0; j < size; j++)
//            {
//                out.writeLong(i);
//            }
//        }
//
//        public Long deserialize(DataInputPlus in, int version) throws IOException
//        {
//            long l = in.readLong();
//            for (int i = 0; i < size - 1; i++)
//            {
//                if (in.readLong() != l)
//                    throw new AssertionError();
//            }
//
//            return l;
//        }
//
//        public long serializedSize(Long t, int version)
//        {
//            return Long.BYTES * size;
//        }
//    }
//    interface ManualSendTest
//    {
//        void accept(ConnectionTest.Settings settings, ProxyServer proxy, InboundSockets inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
//    }
//
//    private void testManual(ManualSendTest test) throws Throwable
//    {
//        for (ConnectionTest.Settings s: SETTINGS)
//        {
//            doTestManual(s, test);
//            resetVerbs();
//        }
//    }
//
//    private void testOneManual(ManualSendTest test) throws Throwable
//    {
//        testOneManual(test, 0);
//    }
//    private void testOneManual(ManualSendTest test, int i) throws Throwable
//    {
//        ConnectionTest.Settings s = SETTINGS.get(i);
//        doTestManual(s, test);
//        resetVerbs();
//    }
//
//    private void doTestManual(ConnectionTest.Settings settings, ManualSendTest test) throws Throwable
//    {
//        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
//
//        InboundConnectionSettings inboundSettings = settings.inbound.apply(new InboundConnectionSettings())
//                                                                    .withBindAddress(endpoint)
//                                                                    .withSocketFactory(factory);
//
//        InboundSockets inbound = new InboundSockets(Collections.singletonList(inboundSettings));
//
//        boolean legacySSL = inboundSettings.encryption != null && inboundSettings.encryption.enable_legacy_ssl_storage_port;
//        InetAddressAndPort proxyEndpoint = InetAddressAndPort.getByAddressOverrideDefaults(endpoint.address,
//                                                                                           SocketUtils.findAvailablePort());
//
//        if (legacySSL)
//            System.setProperty(Config.PROPERTY_PREFIX + "ssl_storage_port", Integer.toString(proxyEndpoint.port));
//
//        OutboundConnectionSettings outboundTemplate = settings.outbound.apply(new OutboundConnectionSettings(proxyEndpoint, proxyEndpoint))
//                                                                       .withConnectTo(proxyEndpoint)
//                                                                       .withDefaultReserveLimits()
//                                                                       .withSocketFactory(factory);
//
//        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundTemplate.applicationReserveSendQueueEndpointCapacityInBytes), outboundTemplate.applicationReserveSendQueueGlobalCapacityInBytes);
//        OutboundConnection outbound = new OutboundConnection(settings.type, outboundTemplate, reserveCapacityInBytes);
//        try
//        {
//            // Need to connect before toxyproxy
//            inbound.open().sync();
//            ProxyServer inJVMProxy = new ProxyServer();
//            inJVMProxy.register(proxyEndpoint, endpoint);
//            test.accept(settings, inJVMProxy, inbound, outbound, endpoint);
//            inJVMProxy.close();
//        }
//        finally
//        {
//            if (legacySSL)
//                System.setProperty(Config.PROPERTY_PREFIX + "ssl_storage_port", Integer.toString(DatabaseDescriptor.getSSLStoragePort()));
//            outbound.close(false);
//            inbound.close().get(30L, SECONDS);
//            outbound.close(false).get(30L, SECONDS);
//            MessagingService.instance().messageHandlers.clear();
//        }
//    }
//
//    private void connect(OutboundConnection outbound) throws Throwable
//    {
//        CountDownLatch connectionLatch = new CountDownLatch(1);
//        unsafeSetHandler(Verb._TEST_1, () -> v -> {
//            connectionLatch.countDown();
//        });
//        outbound.enqueue(Message.out(Verb._TEST_1, 1L));
//        connectionLatch.await(10, SECONDS);
//        Assert.assertEquals(0, connectionLatch.getCount());
//    }
//}
