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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Version;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.BufferPoolAllocator;
import org.apache.cassandra.net.FrameDecoder;
import org.apache.cassandra.net.FrameDecoderCrc;
import org.apache.cassandra.net.FrameDecoderLZ4;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.net.FrameEncoderCrc;
import org.apache.cassandra.net.FrameEncoderLZ4;
import org.apache.cassandra.net.GlobalBufferPoolAllocator;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.messages.StartupMessage;

/**
 * Takes care of intializing a Netty Channel and Pipeline for client protocol connections.
 * The pipeline is first set up with some common handlers for connection limiting, dropping
 * idle connections and optionally SSL, along with a handler to deal with the handshake
 * between client and server. That handshake handler calls back to this class to reconfigure
 * the pipeline once the protocol version for the connection has been established.
 */
public class PipelineConfigurator
{
    private static final Logger logger = LoggerFactory.getLogger(PipelineConfigurator.class);

    // Not to be used in production, this causes a Netty logging handler to be added to the pipeline,
    // which will throttle a system under any normal load.
    private static final boolean DEBUG = Boolean.getBoolean("cassandra.unsafe_verbose_debug_client_protocol");

    // Stateless handlers
    private static final ConnectionLimitHandler connectionLimitHandler = new ConnectionLimitHandler();

    // Names of handlers used regardless of protocol version
    private static final String CONNECTION_LIMIT_HANDLER    = "connectionLimitHandler";
    private static final String IDLE_STATE_HANDLER          = "idleStateHandler";
    private static final String INITIAL_HANDLER             = "initialHandler";
    private static final String EXCEPTION_HANDLER           = "exceptionHandler";
    private static final String DEBUG_HANDLER               = "debugHandler";
    private static final String SSL_HANDLER                 = "ssl";

    // Names of handlers used in pre-V5 pipelines only
    private static final String CQL_FRAME_DECODER           = "cqlFrameDecoder";
    private static final String CQL_FRAME_ENCODER           = "cqlFrameEncoder";
    private static final String CQL_FRAME_DECOMPRESSOR      = "cqlFrameDecompressor";
    private static final String CQL_FRAME_COMPRESSOR        = "cqlFrameCompressor";
    private static final String CQL_MESSAGE_DECODER         = "cqlMessageDecoder";
    private static final String CQL_MESSAGE_ENCODER         = "cqlMessageEncoder";
    private static final String LEGACY_MESSAGE_PROCESSOR    = "legacyCqlProcessor";

    // Names of handlers used in V5 and later pipelines
    private static final String MESSAGE_FRAME_DECODER       = "messageFrameDecoder";
    private static final String MESSAGE_FRAME_ENCODER       = "messageFrameEncoder";
    private static final String MESSAGE_PROCESSOR           = "cqlProcessor";

    private final boolean epoll;
    private final boolean keepAlive;
    private final EncryptionOptions encryptionOptions;
    private final Dispatcher dispatcher;

    public PipelineConfigurator(boolean epoll,
                                boolean keepAlive,
                                boolean legacyFlusher,
                                EncryptionOptions encryptionOptions)
    {
        this.epoll              = epoll;
        this.keepAlive          = keepAlive;
        this.encryptionOptions  = encryptionOptions;
        this.dispatcher         = dispatcher(legacyFlusher);
    }

    public ChannelFuture initializeChannel(final EventLoopGroup workerGroup,
                                           final InetSocketAddress socket,
                                           final Connection.Factory connectionFactory)
    {

        ServerBootstrap bootstrap = new ServerBootstrap()
                                    .channel(epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                                    .childOption(ChannelOption.TCP_NODELAY, true)
                                    .childOption(ChannelOption.SO_LINGER, 0)
                                    .childOption(ChannelOption.SO_KEEPALIVE, keepAlive)
                                    .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
                                    .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
                                    .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        if (workerGroup != null)
            bootstrap = bootstrap.group(workerGroup);

        ChannelInitializer<Channel> initializer = initializer(connectionFactory);
        bootstrap.childHandler(initializer);

        // Bind and start to accept incoming connections.
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
        logger.info("Starting listening for CQL clients on {} ({})...", socket, encryptionOptions.isEnabled() ? "encrypted" : "unencrypted");
        return bootstrap.bind(socket);
    }

    protected ChannelInitializer<Channel> initializer(Connection.Factory connectionFactory)
    {
        // the initializer will perform the common initial setup
        // then any additional steps mandated by the encryption options
        final EncryptionConfig encryptionConfig = encryptionConfig();
        return new ChannelInitializer<Channel>()
        {
            protected void initChannel(Channel channel) throws Exception
            {
                configureInitialPipeline(channel, connectionFactory);
                encryptionConfig.applyTo(channel);
            }
        };
    }

    // Essentially just a Consumer<Channel> which may throw
    interface EncryptionConfig
    {
        void applyTo(Channel channel) throws Exception;
    }

    protected EncryptionConfig encryptionConfig()
    {
        // if encryption is not enabled, no further steps are required after the initial setup
        if (!encryptionOptions.isEnabled())
            return channel -> {};

        // If optional, install a handler which detects whether or not the client is sending
        // encrypted bytes. If so, on receipt of the next bytes, replace that handler with
        // an SSL Handler, otherwise just remove it and proceed with an unencrypted channel.
        if (encryptionOptions.optional)
        {
            logger.info("Enabling optionally encrypted CQL connections between client and server");
            return channel -> {

                SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions,
                                                                         encryptionOptions.require_client_auth,
                                                                         SSLFactory.SocketType.SERVER);

                channel.pipeline().addFirst(SSL_HANDLER, new ByteToMessageDecoder()
                {
                    @Override
                    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception
                    {
                        if (byteBuf.readableBytes() < 5)
                        {
                            // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                            // once more bytes a ready.
                            return;
                        }
                        if (SslHandler.isEncrypted(byteBuf))
                        {
                            // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use
                            // encryption.
                            SslHandler sslHandler = sslContext.newHandler(channel.alloc());
                            channelHandlerContext.pipeline().replace(SSL_HANDLER, SSL_HANDLER, sslHandler);
                        }
                        else
                        {
                            // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
                            // SslHandler in the pipeline.
                            channelHandlerContext.pipeline().remove(SSL_HANDLER);
                        }
                    }
                });
            };
        }
        else
        {
            logger.info("Enabling encrypted CQL connections between client and server");
            return channel -> {
                SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions,
                                                                         encryptionOptions.require_client_auth,
                                                                         SSLFactory.SocketType.SERVER);
                channel.pipeline().addFirst(SSL_HANDLER, sslContext.newHandler(channel.alloc()));
            };
        }
    }

    public void configureInitialPipeline(Channel channel, Connection.Factory connectionFactory)
    {
        ChannelPipeline pipeline = channel.pipeline();

        // Add the ConnectionLimitHandler to the pipeline if configured to do so.
        if (DatabaseDescriptor.getNativeTransportMaxConcurrentConnections() > 0
            || DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0)
        {
            // Add as first to the pipeline so the limit is enforced as first action.
            pipeline.addFirst(CONNECTION_LIMIT_HANDLER, connectionLimitHandler);
        }

        long idleTimeout = DatabaseDescriptor.nativeTransportIdleTimeout();
        if (idleTimeout > 0)
        {
            pipeline.addLast(IDLE_STATE_HANDLER, new IdleStateHandler(false, 0, 0, idleTimeout, TimeUnit.MILLISECONDS)
            {
                @Override
                protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt)
                {
                    logger.info("Closing client connection {} after timeout of {}ms", channel.remoteAddress(), idleTimeout);
                    ctx.close();
                }
            });
        }

        if (DEBUG)
            pipeline.addLast(DEBUG_HANDLER, new LoggingHandler(LogLevel.INFO));

        pipeline.addLast(CQL_FRAME_ENCODER, Frame.Encoder.instance);
        pipeline.addLast(INITIAL_HANDLER, new InitialConnectionHandler(new Frame.Decoder(), connectionFactory, this));
        // The exceptionHandler will take care of handling exceptionCaught(...) events while still running
        // on the same EventLoop as all previous added handlers in the pipeline. This is important as the used
        // eventExecutorGroup may not enforce strict ordering for channel events.
        // As the exceptionHandler runs in the EventLoop as the previous handlers we are sure all exceptions are
        // correctly handled before the handler itself is removed.
        // See https://issues.apache.org/jira/browse/CASSANDRA-13649
        pipeline.addLast(EXCEPTION_HANDLER, PreV5Handlers.ExceptionHandler.instance);
        onInitialPipelineReady(pipeline);
    }

    public void configureModernPipeline(ChannelHandlerContext ctx,
                                        Server.EndpointPayloadTracker tracker,
                                        ProtocolVersion version,
                                        Map<String, String> options)
    {
        BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
        ctx.channel().config().setOption(ChannelOption.ALLOCATOR, allocator);

        // Transport level encoders/decoders
        String compression = options.get(StartupMessage.COMPRESSION);
        FrameDecoder messageFrameDecoder = frameDecoder(compression, allocator);
        FrameEncoder messageFrameEncoder = frameEncoder(compression);
        FrameEncoder.PayloadAllocator payloadAllocator = messageFrameEncoder.allocator();
        ChannelInboundHandlerAdapter exceptionHandler = ExceptionHandlers.postV5Handler(payloadAllocator, version);

        // CQL level encoders/decoders
        Message.Decoder<Message.Request> messageDecoder = messageDecoder();
        Frame.Decoder frameDecoder = new Frame.Decoder();

        // Any non-fatal errors caught in CQLMessageHandler propagate back to the client
        // via the pipeline. Firing the exceptionCaught event on an inbound handler context
        // (in this case, the initial context) will cause it to propagate to to the
        // exceptionHandler provided none of the the intermediate handlers drop it
        // in their exceptionCaught implementation
        ChannelPipeline pipeline = ctx.channel().pipeline();
        final ChannelHandlerContext firstContext = pipeline.firstContext();
        CQLMessageHandler.ErrorHandler errorHandler = firstContext::fireExceptionCaught;

        // Capacity tracking and resource management
        int queueCapacity = DatabaseDescriptor.getNativeTransportReceiveQueueCapacityInBytes();
        ResourceLimits.Limit endpointReserve = endpointReserve(tracker);
        ResourceLimits.Limit globalReserve = globalReserve(tracker);
        boolean throwOnOverload = "1".equals(options.get(StartupMessage.THROW_ON_OVERLOAD));

        CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer = messageConsumer();
        CQLMessageHandler<Message.Request> processor =
            new CQLMessageHandler<>(ctx.channel(),
                                    messageFrameDecoder,
                                    frameDecoder,
                                    messageDecoder,
                                    messageConsumer,
                                    payloadAllocator,
                                    queueCapacity,
                                    endpointReserve,
                                    globalReserve,
                                    AbstractMessageHandler.WaitQueue.endpoint(endpointReserve),
                                    AbstractMessageHandler.WaitQueue.global(globalReserve),
                                    handler -> {},
                                    errorHandler,
                                    throwOnOverload);

        pipeline.remove(CQL_FRAME_ENCODER);    // remove old outbound cql frame encoder
        pipeline.addBefore(INITIAL_HANDLER, MESSAGE_FRAME_DECODER, messageFrameDecoder);
        pipeline.addBefore(INITIAL_HANDLER, MESSAGE_FRAME_ENCODER, messageFrameEncoder);
        pipeline.addBefore(INITIAL_HANDLER, MESSAGE_PROCESSOR, processor);
        pipeline.replace(EXCEPTION_HANDLER, EXCEPTION_HANDLER, exceptionHandler);
        pipeline.remove(INITIAL_HANDLER);
        onNegotiationComplete(pipeline);
    }

    protected void onInitialPipelineReady(ChannelPipeline pipeline) {}
    protected void onNegotiationComplete(ChannelPipeline pipeline) {}

    protected ResourceLimits.Limit endpointReserve(Server.EndpointPayloadTracker tracker)
    {
        return tracker.endpointAndGlobalPayloadsInFlight.endpoint();
    }

    protected ResourceLimits.Limit globalReserve(Server.EndpointPayloadTracker tracker)
    {
        return tracker.endpointAndGlobalPayloadsInFlight.global();
    }

    protected Dispatcher dispatcher(boolean useLegacyFlusher)
    {
        return new Dispatcher(useLegacyFlusher);
    }

    protected CQLMessageHandler.MessageConsumer<Message.Request> messageConsumer()
    {
        return dispatcher::dispatch;
    }

    protected Message.Decoder<Message.Request> messageDecoder()
    {
        return Message.requestDecoder();
    }

    protected FrameDecoder frameDecoder(String compression, BufferPoolAllocator allocator)
    {
        if (null == compression)
            return FrameDecoderCrc.create(allocator);
        if (compression.equalsIgnoreCase("LZ4"))
            return FrameDecoderLZ4.fast(allocator);
        throw new ProtocolException("Unsupported compression type: " + compression);
    }

    protected FrameEncoder frameEncoder(String compression)
    {
        if (Strings.isNullOrEmpty(compression))
            return FrameEncoderCrc.instance;
        if (compression.equalsIgnoreCase("LZ4"))
            return FrameEncoderLZ4.fastInstance;
        throw new ProtocolException("Unsupported compression type: " + compression);
    }

    public void configureLegacyPipeline(ChannelHandlerContext ctx,
                                        Server.EndpointPayloadTracker tracker)
    {
        ChannelPipeline pipeline = ctx.channel().pipeline();
        pipeline.addBefore(CQL_FRAME_ENCODER, CQL_FRAME_DECODER, new Frame.Decoder());
        pipeline.addBefore(INITIAL_HANDLER, CQL_FRAME_DECOMPRESSOR, Frame.Decompressor.instance);
        pipeline.addBefore(INITIAL_HANDLER, CQL_FRAME_COMPRESSOR, Frame.Compressor.instance);
        pipeline.addBefore(INITIAL_HANDLER, CQL_MESSAGE_DECODER, PreV5Handlers.ProtocolDecoder.instance);
        pipeline.addBefore(INITIAL_HANDLER, CQL_MESSAGE_ENCODER, PreV5Handlers.ProtocolEncoder.instance);
        pipeline.addBefore(INITIAL_HANDLER, LEGACY_MESSAGE_PROCESSOR, new PreV5Handlers.LegacyDispatchHandler(dispatcher, tracker));
        pipeline.remove(INITIAL_HANDLER);
        onNegotiationComplete(pipeline);
    }
}

