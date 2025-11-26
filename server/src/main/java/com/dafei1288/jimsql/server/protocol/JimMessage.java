package com.dafei1288.jimsql.server.protocol;

import io.netty.buffer.ByteBuf;

public class JimMessage {
    private static final int MAGIC_NUMBER = 0x4A494D53; // "JIMS" in ASCII
    private final JimMessageType type;
    private final int sequenceId;
    private final byte[] payload;

    public JimMessage(JimMessageType type, int sequenceId, byte[] payload) {
        this.type = type;
        this.sequenceId = sequenceId;
        this.payload = payload;
    }

    public static JimMessage decode(ByteBuf in) {
        if (in.readableBytes() < 16) {
            return null;
        }

        int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new IllegalArgumentException("Invalid magic number: " + magic);
        }

        int typeValue = in.readInt();
        JimMessageType type = JimMessageType.valueOf(typeValue);
        int length = in.readInt();
        int sequenceId = in.readInt();

        if (in.readableBytes() < length) {
            return null;
        }

        byte[] payload = new byte[length];
        in.readBytes(payload);

        return new JimMessage(type, sequenceId, payload);
    }

    public void encode(ByteBuf out) {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(type.getValue());
        out.writeInt(payload.length);
        out.writeInt(sequenceId);
        out.writeBytes(payload);
    }

    public JimMessageType getType() {
        return type;
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public byte[] getPayload() {
        return payload;
    }
} 