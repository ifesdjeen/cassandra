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

package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import io.netty.buffer.ByteBuf;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AllowAllNetworkAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.AssertUtil;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class DriverBurnTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(CQLConnectionTest.class);

    @Before
    public void setup()
    {
        requireNetwork();
    }

    private static class SizeCaps
    {
        private final int valueMinSize;
        private final int valueMaxSize;
        private final int columnCountCap;
        private final int rowsCountCap;

        private SizeCaps(int valueMinSize, int valueMaxSize, int columnCountCap, int rowsCountCap)
        {
            this.valueMinSize = valueMinSize;
            this.valueMaxSize = valueMaxSize;
            this.columnCountCap = columnCountCap;
            this.rowsCountCap = rowsCountCap;
        }

        public String toString()
        {
            return "SizeCaps{" +
                   "valueMinSize=" + valueMinSize +
                   ", valueMaxSize=" + valueMaxSize +
                   ", columnCountCap=" + columnCountCap +
                   ", rowsCountCap=" + rowsCountCap +
                   '}';
        }
    }
    @Test
    public void test() throws Throwable
    {
        final SizeCaps smallMessageCap = new SizeCaps(10, 20, 5, 10);
        final SizeCaps largeMessageCap = new SizeCaps(1000, 2000, 5, 150);
        int largeMessageFrequency = 1000;

        Message.Type.QUERY.unsafeSetCodec(new Message.Codec<QueryMessage>() {
            public QueryMessage decode(ByteBuf body, ProtocolVersion version)
            {
                QueryMessage queryMessage = QueryMessage.codec.decode(body, version);
                return new QueryMessage(queryMessage.query, queryMessage.options) {
                    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
                    {
                        try
                        {
                            int idx = Integer.parseInt(queryMessage.query);
                            SizeCaps caps = idx % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            // TODO: assert values

                            ResultMessage.Rows response = getRows(idx, caps);
                            return response;
                        }
                        catch (NumberFormatException e)
                        {
                            // for the requests driver issues under the hood
                            return super.execute(state, queryStartNanoTime, traceRequest);
                        }
                    }
                };
            }

            public void encode(QueryMessage queryMessage, ByteBuf dest, ProtocolVersion version)
            {
                QueryMessage.codec.encode(queryMessage, dest, version);
            }

            public int encodedSize(QueryMessage queryMessage, ProtocolVersion version)
            {
                return 0;
            }
        });

        List<Thread> threads = new ArrayList<>();

//        List<AssertUtil.ThrowingSupplier<SimpleClient>> suppliers =


        for (int t = 0; t < 10; t++)
        {
            int threadId = t;
            threads.add(new Thread(() -> {
                try (Cluster driver = Cluster.builder().addContactPoint(nativeAddr.getHostAddress())
//                                             .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.V4)
                                             .allowBetaProtocolVersion()
                                             .withPort(nativePort)
                                             .build();
                     Session session = driver.connect())
                {
                    int idx = 0;
                    while(!Thread.interrupted())
                    {
                        Map<Integer, ResultSetFuture> futures = new HashMap<>();

                        for (int j = 0; j < 10; j++)
                        {
                            int descriptor = idx + j * 100 + threadId * 10000;
                            SizeCaps caps = descriptor % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            futures.put(j, session.executeAsync(getQueryMessage(descriptor, caps)));
                        }

                        for (Map.Entry<Integer, ResultSetFuture> e : futures.entrySet())
                        {
                            final int j = e.getKey().intValue();
                            final int descriptor = idx + j * 100 + threadId * 10000;
                            SizeCaps caps = descriptor % largeMessageFrequency == 0 ? largeMessageCap : smallMessageCap;
                            ResultMessage.Rows expectedRS = getRows(descriptor, caps);
                            List<Row> actualRS = e.getValue().get().all();

                            for (int i = 0; i < actualRS.size(); i++)
                            {
                                List<ByteBuffer> expected = expectedRS.result.rows.get(i);
                                Row actual = actualRS.get(i);

                                for (int col = 0; col < expected.size(); col++)
                                    Assert.assertEquals(actual.getBytes(col), expected.get(col));
                            }
                        }
                        idx++;
                    }
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                    fail("No exceptions should've been thrown: " + e.getMessage());
                }
            }));
        }

        for (Thread thread : threads)
            thread.start();

        for (Thread thread : threads)
        {
            thread.join();
        }
    }

    public static SimpleStatement getQueryMessage(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);

        ByteBuffer[] values = new ByteBuffer[sizeCaps.columnCountCap];
        for (int i = 0; i < sizeCaps.columnCountCap; i++)
            values[i] = bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize);

        return new SimpleStatement(Integer.toString(idx), values);
    }

    public static ResultMessage.Rows getRows(int idx, SizeCaps sizeCaps)
    {
        Random rnd = new Random(idx);
        List<ColumnSpecification> columns = new ArrayList<>();
        for (int i = 0; i < sizeCaps.columnCountCap; i++)
        {
            columns.add(new ColumnSpecification("ks", "cf",
                                                new ColumnIdentifier(bytes(rnd, 5, 10), BytesType.instance),
                                                BytesType.instance));
        }

        List<List<ByteBuffer>> rows = new ArrayList<>();
        int count = rnd.nextInt(sizeCaps.rowsCountCap);
        for (int i = 0; i < count; i++)
        {
            List<ByteBuffer> row = new ArrayList<>();
            for (int j = 0; j < sizeCaps.columnCountCap; j++)
                row.add(bytes(rnd, sizeCaps.valueMinSize, sizeCaps.valueMaxSize));
            rows.add(row);
        }

        ResultSet resultSet = new ResultSet(new ResultSet.ResultMetadata(columns), rows);
        return new ResultMessage.Rows(resultSet);
    }

    public static ByteBuffer bytes(Random rnd, int minSize, int maxSize)
    {
        byte[] bytes = new byte[rnd.nextInt(maxSize) + minSize];
        rnd.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }
}

// Uninvestigated:
// Recent access records:
//Created at:
//	io.netty.buffer.PooledByteBufAllocator.newDirectBuffer(PooledByteBufAllocator.java:363)
//	io.netty.buffer.AbstractByteBufAllocator.directBuffer(AbstractByteBufAllocator.java:187)
//	io.netty.buffer.AbstractByteBufAllocator.directBuffer(AbstractByteBufAllocator.java:178)
//	io.netty.buffer.AbstractByteBufAllocator.ioBuffer(AbstractByteBufAllocator.java:139)
//	io.netty.channel.DefaultMaxMessagesRecvByteBufAllocator$MaxMessageHandle.allocate(DefaultMaxMessagesRecvByteBufAllocator.java:114)
//	io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:147)
//	io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:714)
//	io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:650)
//	io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:576)
//	io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:493)
//	io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:989)
//	io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
//	io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
//	java.base/java.lang.Thread.run(Thread.java:834)


// WARN  [nioEventLoopGroup-3-1] 2020-07-17 11:11:10,289 DefaultChannelPipeline.java:1152 - An exceptionCaught() event was fired, and it reached at the tail of the pipeline. It usually means the last handler in the pipeline did not handle the exception.
//org.apache.cassandra.net.Crc$InvalidCrc: Read -854589741, Computed 1432984585
//	at org.apache.cassandra.transport.CQLMessageHandler.processCorruptFrame(CQLMessageHandler.java:328)
//	at org.apache.cassandra.net.AbstractMessageHandler.process(AbstractMessageHandler.java:217)
//	at org.apache.cassandra.net.FrameDecoder.deliver(FrameDecoder.java:321)
//	at org.apache.cassandra.net.FrameDecoder.channelRead(FrameDecoder.java:285)
//	at org.apache.cassandra.net.FrameDecoder.channelRead(FrameDecoder.java:269)
//	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:379)
//	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:365)
//	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:357)
//	at io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1410)
//	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:379)
//	at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:365)
//	at io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:919)
//	at io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:163)
//	at io.netty.channel.nio.NioEventLoop.processSelectedKey(NioEventLoop.java:714)
//	at io.netty.channel.nio.NioEventLoop.processSelectedKeysOptimized(NioEventLoop.java:650)
//	at io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:576)
//	at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:493)
//	at io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:989)
//	at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
//	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
//	at java.base/java.lang.Thread.run(Thread.java:834)