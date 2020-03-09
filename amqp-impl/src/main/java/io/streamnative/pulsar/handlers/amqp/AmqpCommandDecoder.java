/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import java.net.SocketAddress;
import java.util.Enumeration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A decoder that decodes amqp requests and responses.
 */
@Slf4j
public abstract class AmqpCommandDecoder extends ChannelInboundHandlerAdapter {
    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);

    // Queue to make request get responseFuture in order.
    protected final ConcurrentHashMap<UnsignedShort, Queue<ResponseAndRequest>> responsesQueue =
        new ConcurrentHashMap();

    public AmqpCommandDecoder() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        close();
    }

    protected void close() {
        if (isActive.getAndSet(false)) {
            log.info("close netty channel {}", ctx.channel());
            Enumeration<UnsignedShort> channels = responsesQueue.keys();
            while (channels.hasMoreElements()) {
                UnsignedShort channelId = channels.nextElement();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] close amqp channel {}.",
                            ctx.channel(), channelId);
                }
                writeAndFlushWhenInactiveChannel(channelId);
            }
            ctx.close();
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    protected ByteBuf responseToByteBuf(AmqpResponse response, AmqpRequest request) {
        // todo:
        return null;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;

        Channel nettyChannel = ctx.channel();
        checkState(nettyChannel.equals(this.ctx.channel()));

        try {
            AmqpRequest amqpRequest = new AmqpRequest(buffer);

            if (log.isDebugEnabled()) {
                log.debug("[{}] Received amqp cmd {}, the request content is: {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                        amqpRequest.toString());
            }

            // amqp channel id
            UnsignedShort channelId = amqpRequest.getChannel();

            CompletableFuture<AmqpResponse> responseFuture = new CompletableFuture<>();

            responsesQueue.compute(channelId, (key, queue) -> {
                if (queue == null) {
                    Queue<ResponseAndRequest> newQueue = Queues.newConcurrentLinkedQueue();
                    newQueue.add(ResponseAndRequest.of(responseFuture, amqpRequest));
                    return newQueue;
                } else {
                    queue.add(ResponseAndRequest.of(responseFuture, amqpRequest));
                    return queue;
                }
            });

            // todo: isActive false: handle inactive.
            //handleInactive(amqpRequest, responseFuture);

            switch (amqpRequest.getFrameId().getInt()) {
                case 1:
                    handleMethodFrame(amqpRequest, responseFuture);
                    break;

                case 2:
                    handleContentHeaderFrame(amqpRequest, responseFuture);
                    break;

                case 3:
                    handleContentBodyFrame(amqpRequest, responseFuture);
                    break;

                case 8:
                    handleHeartbeaatFrame(amqpRequest, responseFuture);
                    break;

                default:
                    handleError(amqpRequest, responseFuture);
            }

            responseFuture.whenComplete((response, e) -> {
                writeAndFlushResponseToClient(channelId);
            });
        } catch (Exception e) {
            log.error("error while handle command:", e);
            close();
        } finally {
            // the amqpRequest has already held the reference.
            buffer.release();
        }
    }

    // Write and flush continuously completed request back through channel.
    // This is to make sure request get responseFuture in the same order.
    protected void writeAndFlushResponseToClient(UnsignedShort channel) {
        Queue<ResponseAndRequest> responseQueue =
            responsesQueue.get(channel);

        // loop from first responseFuture.
        while (responseQueue != null && responseQueue.peek() != null
            && responseQueue.peek().getResponseFuture().isDone() && isActive.get()) {
            ResponseAndRequest response = responseQueue.remove();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Write amqp cmd response back to client. \n"
                            + "\trequest content: {} \n"
                            + "\tresponse content: {}",
                        response.getRequest().toString(),
                        response.getResponseFuture().join().toString());
                    log.debug("Write amqp cmd responseFuture back to client. request: {}",
                        response.getRequest());
                }

                ByteBuf result = responseToByteBuf(response.getResponseFuture().get(), response.getRequest());
                ctx.channel().writeAndFlush(result);
            } catch (Exception e) {
                // should not comes here.
                log.error("error to get Response ByteBuf:", e);
            }
        }
    }

    // return all the current command before a channel close. return Error response for all pending request.
    protected void writeAndFlushWhenInactiveChannel(UnsignedShort channel) {
        Queue<ResponseAndRequest> responseQueue =
            responsesQueue.get(channel);

        // loop from first responseFuture, and return them all
        while (responseQueue != null && responseQueue.peek() != null) {
            try {
                ResponseAndRequest pair = responseQueue.remove();

                if (log.isDebugEnabled()) {
                    log.debug("Channel Closing! Write amqp cmd responseFuture back to client. request: {}",
                        pair.getRequest());
                }
                AmqpRequest request = pair.getRequest();
                AmqpResponse apiResponse = request.getErrorResponse();
                pair.getResponseFuture().complete(apiResponse);

                ByteBuf result = responseToByteBuf(pair.getResponseFuture().get(), pair.getRequest());
                ctx.channel().writeAndFlush(result);
            } catch (Exception e) {
                // should not comes here.
                log.error("error to get Response ByteBuf:", e);
            }
        }
    }


    /**
     * A class that stores Amqp request and its related responseFuture.
     */
    static class ResponseAndRequest {
        @Getter
        private CompletableFuture<AmqpResponse> responseFuture;
        @Getter
        private AmqpRequest request;

        public static ResponseAndRequest of(CompletableFuture<AmqpResponse> response,
                                            AmqpRequest request) {
            return new ResponseAndRequest(response, request);
        }

        ResponseAndRequest(CompletableFuture<AmqpResponse> response, AmqpRequest request) {
            this.responseFuture = response;
            this.request = request;
        }
    }

    protected abstract void handleMethodFrame(AmqpRequest request,
                                              CompletableFuture<AmqpResponse> responseFuture);

    protected abstract void handleContentHeaderFrame(AmqpRequest request,
                                                     CompletableFuture<AmqpResponse> responseFuture);

    protected abstract void handleContentBodyFrame(AmqpRequest request,
                                                   CompletableFuture<AmqpResponse> responseFuture);

    protected abstract void handleHeartbeaatFrame(AmqpRequest request,
                                                  CompletableFuture<AmqpResponse> responseFuture);

    protected abstract void handleError(AmqpRequest request,
                                        CompletableFuture<AmqpResponse> responseFuture);

}
