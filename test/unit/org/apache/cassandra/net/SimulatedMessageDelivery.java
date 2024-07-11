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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class SimulatedMessageDelivery implements MessageDelivery
{
    public enum Action { DELIVER, DELIVER_WITH_FAILURE, DROP, DROP_PARTITIONED, FAILURE }

    public interface ActionSupplier
    {
        Action get(InetAddressAndPort self, Message<?> message, InetAddressAndPort to);
    }

    public interface Scheduler
    {
        void schedule(Runnable command, long delay, TimeUnit unit);
    }

    public interface DropListener
    {
        void onDrop(Action action, InetAddressAndPort from, Message<?> msg);
    }

    private final InetAddressAndPort self;
    private final ActionSupplier actions;
    private final BiConsumer<InetAddressAndPort, Message<?>> reciever;
    private final DropListener onDropped;
    private final Scheduler scheduler;
    private final Consumer<Throwable> onError;
    private final Map<CallbackKey, CallbackContext> callbacks = new HashMap<>();
    private enum Status { Up, Down}
    private Status status = Status.Up;

    public SimulatedMessageDelivery(InetAddressAndPort self,
                                    ActionSupplier actions,
                                    BiConsumer<InetAddressAndPort, Message<?>> reciever,
                                    DropListener onDropped,
                                    Scheduler scheduler,
                                    Consumer<Throwable> onError)
    {
        this.self = self;
        this.actions = actions;
        this.reciever = reciever;
        this.onDropped = onDropped;
        this.scheduler = scheduler;
        this.onError = onError;
    }

    public void stop()
    {
        callbacks.clear();
        status = Status.Down;
    }

    @Override
    public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, null);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, cb);
    }

    @Override
    public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
    {
        message = message.withFrom(self);
        maybeEnqueue(message, to, cb);
    }

    @Override
    public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
    {
        AsyncPromise<Message<RSP>> promise = new AsyncPromise<>();
        sendWithCallback(message, to, new RequestCallback<RSP>()
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                promise.trySuccess(msg);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                promise.tryFailure(new MessagingService.FailureResponseException(from, failure.reason));
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        });
        return promise;
    }

    @Override
    public <V> void respond(V response, Message<?> message)
    {
        send(message.responseWith(response), message.respondTo());
    }

    private <REQ, RSP> void maybeEnqueue(Message<REQ> message, InetAddressAndPort to, @Nullable RequestCallback<RSP> callback)
    {
        if (status != Status.Up)
            return;
        CallbackContext cb;
        if (callback != null)
        {
            CallbackKey key = new CallbackKey(message.id(), to);
            if (callbacks.containsKey(key))
                throw new AssertionError("Message id " + message.id() + " to " + to + " already has a callback");
            cb = new CallbackContext(callback);
            callbacks.put(key, cb);
        }
        else
        {
            cb = null;
        }
        Action action = actions.get(self, message, to);
        switch (action)
        {
            case DELIVER:
                reciever.accept(to, message);
                break;
            case DROP:
            case DROP_PARTITIONED:
                onDropped.onDrop(action, to, message);
                break;
            case DELIVER_WITH_FAILURE:
                reciever.accept(to, message);
            case FAILURE:
                if (action == Action.FAILURE)
                    onDropped.onDrop(action, to, message);
                if (callback != null)
                    scheduler.schedule(() -> callback.onFailure(to, RequestFailure.UNKNOWN),
                                       message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
                return;
            default:
                throw new UnsupportedOperationException("Unknown action type: " + action);
        }
        if (cb != null)
        {
            scheduler.schedule(() -> {
                CallbackContext ctx = callbacks.remove(new CallbackKey(message.id(), to));
                if (ctx != null)
                {
                    assert ctx == cb;
                    try
                    {
                        ctx.onFailure(to, RequestFailure.TIMEOUT);
                    }
                    catch (Throwable t)
                    {
                        onError.accept(t);
                    }
                }
            }, message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @SuppressWarnings("rawtypes")
    public SimulatedMessageReceiver reciver(IVerbHandler onMessage)
    {
        return new SimulatedMessageReceiver(onMessage);
    }

    public class SimulatedMessageReceiver
    {
        @SuppressWarnings("rawtypes")
        final IVerbHandler onMessage;

        @SuppressWarnings("rawtypes")
        public SimulatedMessageReceiver(IVerbHandler onMessage)
        {
            this.onMessage = onMessage;
        }

        public void recieve(Message<?> msg)
        {
            if (status != Status.Up)
                return;
            if (msg.verb().isResponse())
            {
                CallbackKey key = new CallbackKey(msg.id(), msg.from());
                if (callbacks.containsKey(key))
                {
                    CallbackContext callback = callbacks.remove(key);
                    if (callback == null)
                        return;
                    try
                    {
                        if (msg.isFailureResponse())
                            callback.onFailure(msg.from(), (RequestFailure) msg.payload);
                        else callback.onResponse(msg);
                    }
                    catch (Throwable t)
                    {
                        onError.accept(t);
                    }
                }
            }
            else
            {
                try
                {
                    //noinspection unchecked
                    onMessage.doVerb(msg);
                }
                catch (Throwable t)
                {
                    onError.accept(t);
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static class SimpleVerbHandler implements IVerbHandler
    {
        private final Map<Verb, IVerbHandler<?>> handlers;

        public SimpleVerbHandler(Map<Verb, IVerbHandler<?>> handlers)
        {
            this.handlers = handlers;
        }

        @Override
        public void doVerb(Message msg) throws IOException
        {
            IVerbHandler<?> handler = handlers.get(msg.verb());
            if (handler == null)
                throw new AssertionError("Unexpected verb: " + msg.verb());
            //noinspection unchecked
            handler.doVerb(msg);
        }
    }

    private static class CallbackContext
    {
        @SuppressWarnings("rawtypes")
        final RequestCallback callback;

        @SuppressWarnings("rawtypes")
        private CallbackContext(RequestCallback callback)
        {
            this.callback = Objects.requireNonNull(callback);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void onResponse(Message msg)
        {
            callback.onResponse(msg);
        }

        public void onFailure(InetAddressAndPort from, RequestFailure failure)
        {
            if (callback.invokeOnFailure()) callback.onFailure(from, failure);
        }
    }

    private static class CallbackKey
    {
        private final long id;
        private final InetAddressAndPort peer;

        private CallbackKey(long id, InetAddressAndPort peer)
        {
            this.id = id;
            this.peer = peer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CallbackKey that = (CallbackKey) o;
            return id == that.id && peer.equals(that.peer);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, peer);
        }

        @Override
        public String toString()
        {
            return "CallbackKey{" +
                   "id=" + id +
                   ", peer=" + peer +
                   '}';
        }
    }
}