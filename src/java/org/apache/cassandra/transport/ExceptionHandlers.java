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

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.transport.messages.ErrorMessage;

public class ExceptionHandlers
{
    private static final Logger logger = LoggerFactory.getLogger(ExceptionHandlers.class);

    public static ChannelInboundHandlerAdapter postV5Handler(FrameEncoder.PayloadAllocator allocator,
                                                             ProtocolVersion version)
    {
        return new PostV5ExceptionHandler(allocator, version);
    }

    private static final class PostV5ExceptionHandler extends ChannelInboundHandlerAdapter
    {
        private final FrameEncoder.PayloadAllocator allocator;
        private final ProtocolVersion version;

        public PostV5ExceptionHandler(FrameEncoder.PayloadAllocator allocator, ProtocolVersion version)
        {
            this.allocator = allocator;
            this.version = version;
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            // Provide error message to client in case channel is still open
            UnexpectedChannelExceptionHandler handler = new UnexpectedChannelExceptionHandler(ctx.channel(), false);
            if (ctx.channel().isOpen())
            {
                ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
                Frame frame = errorMessage.encode(version);
                FrameEncoder.Payload payload = allocator.allocate(true, CQLMessageHandler.frameSize(frame.header));
                frame.encodeInto(payload.buffer);
                frame.release();
                payload.finish();
                ChannelPromise promise = ctx.newPromise();
                // On protocol exception, close the channel as soon as the message has been sent
                if (cause instanceof ProtocolException)
                    promise.addListener(future -> ctx.close());
                ctx.writeAndFlush(payload, promise);
                payload.release();
            }
        }
    }

    /**
     * Include the channel info in the logged information for unexpected errors, and (if {@link #alwaysLogAtError} is
     * false then choose the log level based on the type of exception (some are clearly client issues and shouldn't be
     * logged at server ERROR level)
     */
    static final class UnexpectedChannelExceptionHandler implements Predicate<Throwable>
    {

        /**
         * When we encounter an unexpected IOException we look for these {@link Throwable#getMessage() messages}
         * (because we have no better way to distinguish) and log them at DEBUG rather than INFO, since they
         * are generally caused by unclean client disconnects rather than an actual problem.
         */
        private static final Set<String> ioExceptionsAtDebugLevel = ImmutableSet.<String>builder().
            add("Connection reset by peer").
            add("Broken pipe").
            add("Connection timed out").
            build();

        private final Channel channel;
        private final boolean alwaysLogAtError;

        UnexpectedChannelExceptionHandler(Channel channel, boolean alwaysLogAtError)
        {
            this.channel = channel;
            this.alwaysLogAtError = alwaysLogAtError;
        }

        @Override
        public boolean apply(Throwable exception)
        {
            String message;
            try
            {
                message = "Unexpected exception during request; channel = " + channel;
            }
            catch (Exception ignore)
            {
                // We don't want to make things worse if String.valueOf() throws an exception
                message = "Unexpected exception during request; channel = <unprintable>";
            }

            // netty wraps SSL errors in a CodecExcpetion
            if (!alwaysLogAtError && (exception instanceof IOException || (exception.getCause() instanceof IOException)))
            {
                String errorMessage = exception.getMessage();
                boolean logAtTrace = false;

                for (String ioException : ioExceptionsAtDebugLevel)
                {
                    // exceptions thrown from the netty epoll transport add the name of the function that failed
                    // to the exception string (which is simply wrapping a JDK exception), so we can't do a simple/naive comparison
                    if (errorMessage.contains(ioException))
                    {
                        logAtTrace = true;
                        break;
                    }
                }

                if (logAtTrace)
                {
                    // Likely unclean client disconnects
                    logger.trace(message, exception);
                }
                else
                {
                    // Generally unhandled IO exceptions are network issues, not actual ERRORS
                    logger.info(message, exception);
                }
            }
            else
            {
                // Anything else is probably a bug in server of client binary protocol handling
                logger.error(message, exception);
            }

            // We handled the exception.
            return true;
        }
    }
}
