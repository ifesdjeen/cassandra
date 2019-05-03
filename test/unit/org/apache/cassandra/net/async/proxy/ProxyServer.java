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

package org.apache.cassandra.net.async.proxy;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.cassandra.locator.InetAddressAndPort;

public class ProxyServer
{
    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup workerGroup;
    private final InetAddressAndPort listen;
    public Channel channel;

    public ProxyServer(EventLoopGroup acceptGroup, EventLoopGroup workerGroup,


                       InetAddressAndPort listen)
    {
        this.acceptGroup = acceptGroup;
        this.workerGroup = workerGroup;
        this.listen = listen;
    }

    public void init(ChannelInitializer<SocketChannel> init)
    {
        ServerBootstrap b = new ServerBootstrap();

        ChannelFuture channelFuture = b.group(acceptGroup, workerGroup)
                                       .channel(NioServerSocketChannel.class)
                                       .childHandler(init)
                                       .childOption(ChannelOption.AUTO_READ, false)
                                       .bind(new InetSocketAddress(listen.address, listen.port));

        if (channelFuture.awaitUninterruptibly().isSuccess())
        {
            channel = channelFuture.channel();
        }
        else
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();
            throw new RuntimeException("Couldn't connect: ", channelFuture.cause());
        }
    }

    public void close()
    {
        acceptGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    static void flushAndClose(Channel ch)
    {
        if (ch.isActive())
        {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}


