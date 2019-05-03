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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import org.apache.cassandra.locator.InetAddressAndPort;

public class UpstreamHandler extends ChannelInboundHandlerAdapter
{
    private final InetAddressAndPort upstream;
    private Channel upstreamChannel;
    private Channel listeningChannel;
    private DownstreamHandler downstreamHandler;

    public UpstreamHandler(InetAddressAndPort upstream)
    {
        this.upstream = upstream;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        // Listening connection
        listeningChannel = ctx.channel();

        downstreamHandler = new DownstreamHandler(listeningChannel);
        // Start the connection attempt.
        Bootstrap b = new Bootstrap();
        b.group(listeningChannel.eventLoop())
         .channel(ctx.channel().getClass())
         .handler(downstreamHandler)
         .option(ChannelOption.AUTO_READ, false);
        ChannelFuture channelFuture = b.connect(upstream.address, upstream.port);
        upstreamChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (future.isSuccess())
            {
                // connection complete start to read first data
                listeningChannel.read();
            }
            else
            {
                // Close the connection if the connection attempt has failed.
                listeningChannel.close();
            }
        });
    }

    // Strategies: latency, read and close, just close
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        if (upstreamChannel.isActive())
            ctx.fireChannelRead(msg);
        else
            ctx.close();
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        if (upstreamChannel != null)
        {
            ProxyServer.flushAndClose(upstreamChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        ProxyServer.flushAndClose(ctx.channel());
    }
}
