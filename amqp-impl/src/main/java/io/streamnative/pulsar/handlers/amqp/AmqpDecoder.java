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

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

@Slf4j
public abstract class AmqpDecoder<T extends MethodProcessor> {

    public static final int FRAME_HEADER_SIZE = 7;
    private final T methodProcessor;

    /** Holds the protocol initiation decoder. */
    private ProtocolInitiation.Decoder piDecoder = new ProtocolInitiation.Decoder();

    /** Flag to indicate whether this decoder needs to handle protocol initiation. */
    private boolean expectProtocolInitiation;


    private boolean firstRead = true;

    public static final int FRAME_MIN_SIZE = 4096;
    private int maxFrameSize = FRAME_MIN_SIZE;

    private final Channel clientChannel;

    /**
     * Creates a new AMQP decoder.
     * @param expectProtocolInitiation <tt>true</tt> if this decoder needs to handle protocol initiation.
     * @param methodProcessor method processor
     */
    protected AmqpDecoder(boolean expectProtocolInitiation, T methodProcessor, Channel clientChannel) {
        this.expectProtocolInitiation = expectProtocolInitiation;
        this.methodProcessor = methodProcessor;
        this.clientChannel = clientChannel;
    }

    /**
     * Sets the protocol initation flag, that determines whether decoding is handled by the data decoder of the protocol
     * initation decoder. This method is expected to be called with <tt>false</tt> once protocol initation completes.
     *
     * @param expectProtocolInitiation <tt>true</tt> to use the protocol initiation decoder, <tt>false</tt> to use the
     *                                data decoder.
     */
    public void setExpectProtocolInitiation(boolean expectProtocolInitiation) {
        this.expectProtocolInitiation = expectProtocolInitiation;
    }

    public void setMaxFrameSize(final int frameMax) {
        maxFrameSize = frameMax;
    }

    public T getMethodProcessor() {
        return methodProcessor;
    }

    protected final int decode(final QpidByteBuffer buf) throws AMQFrameDecodingException {
        // If this is the first read then we may be getting a protocol initiation back if we tried to negotiate
        // an unsupported version
        if (firstRead && buf.hasRemaining()) {
            firstRead = false;
            if (!expectProtocolInitiation && (((int) buf.get(buf.position())) & 0xff) > 8) {
                expectProtocolInitiation = true;
            }
        }

        int required = 0;
        while (required == 0) {
            if (!expectProtocolInitiation) {
                required = processAMQPFrames(buf);
            } else {
                required = piDecoder.decodable(buf);
                if (required == 0) {
                    methodProcessor.receiveProtocolHeader(new ProtocolInitiation(buf));
                }
            }
        }
        return buf.hasRemaining() ? required : 0;
    }

    protected int processAMQPFrames(final QpidByteBuffer buf) throws AMQFrameDecodingException {
        final int required = decodable(buf);
        if (required == 0) {
            processInput(buf);
        }
        return required;
    }

    protected int decodable(final QpidByteBuffer in) throws AMQFrameDecodingException {
        final int remainingAfterAttributes = in.remaining() - FRAME_HEADER_SIZE;
        // type, channel, body length and end byte
        if (remainingAfterAttributes < 0) {
            return -remainingAfterAttributes;
        }


        // Get an unsigned int, lifted from MINA ByteBuffer getUnsignedInt()
        final long bodySize = ((long) in.getInt(in.position() + 3)) & 0xffffffffL;
        if (bodySize > maxFrameSize) {
            throw new AMQFrameDecodingException(
                    "Incoming frame size of "
                            + bodySize
                            + " is larger than negotiated maximum of  "
                            + maxFrameSize);
        }

        long required = (1L + bodySize) - remainingAfterAttributes;
        return required > 0 ? (int) required : 0;

    }

    protected void processInput(final QpidByteBuffer in)
            throws AMQFrameDecodingException, AMQProtocolVersionException {
        final byte type = in.get();

        final int channel = in.getUnsignedShort();
        final long bodySize = in.getUnsignedInt();

        // bodySize can be zero
        if ((channel < 0) || (bodySize < 0)) {
            throw new AMQFrameDecodingException(
                    "Undecodable frame: type = " + type + " channel = " + channel
                            + " bodySize = " + bodySize);
        }

        processFrame(channel, type, bodySize, in);

        byte marker = in.get();
        if ((marker & 0xFF) != 0xCE) {
            throw new AMQFrameDecodingException(
                    "End of frame marker not found. Read " + marker + " length=" + bodySize
                            + " type=" + type);
        }

    }

    protected void processFrame(final int channel, final byte type, final long bodySize, final QpidByteBuffer in)
            throws AMQFrameDecodingException {
        System.out.println("xxxx process frame " + type);
        switch (type) {
            case 1 -> processMethod(channel, in);
            case 2 -> ContentHeaderBody.process(in, methodProcessor.getChannelMethodProcessor(channel), bodySize);
            case 3 -> ContentBody.process(in, methodProcessor.getChannelMethodProcessor(channel), bodySize);
            case 8 ->
//                HeartbeatBody.process(channel, in, _methodProcessor, bodySize);
                    redirectData(in, bodySize, 8);
            case 9 -> redirectData(in, bodySize, 9);
            default -> throw new AMQFrameDecodingException("Unsupported frame type: " + type);
        }
    }

    protected abstract void processMethod(int channelId, QpidByteBuffer in) throws AMQFrameDecodingException;

    protected AMQFrameDecodingException newUnknownMethodException(final int classId,
                                                                  final int methodId,
                                                                  ProtocolVersion protocolVersion) {
        return new AMQFrameDecodingException(ErrorCodes.COMMAND_INVALID,
                "Method "
                        + methodId
                        + " unknown in AMQP version "
                        + protocolVersion
                        + " (while trying to decode class "
                        + classId
                        + " method "
                        + methodId
                        + ".", null);
    }

    protected void redirectData(QpidByteBuffer in, long messageSize, int type) {
        if (log.isDebugEnabled()) {
            log.debug("redirect data type: {}, messageSize: {}", type, messageSize);
        }
        QpidByteBuffer buffer = in.view(0, (int) messageSize);
        in.position((int) (in.position() + messageSize));
        clientChannel.writeAndFlush(buffer);
    }

}
