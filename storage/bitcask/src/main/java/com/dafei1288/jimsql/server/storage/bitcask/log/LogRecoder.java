package com.dafei1288.jimsql.server.storage.bitcask.log;

public class LogRecoder {
    private int crc;
    private long tstamp;
    private LogRecoderType logRecoderType;
    private int key_sz;
    private int value_sz;
    private String key;
    private Object value;

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }

    public LogRecoderType getLogRecoderType() {
        return logRecoderType;
    }

    public void setLogRecoderType(LogRecoderType logRecoderType) {
        this.logRecoderType = logRecoderType;
    }

    public int getKey_sz() {
        return key_sz;
    }

    public void setKey_sz(int key_sz) {
        this.key_sz = key_sz;
    }

    public int getValue_sz() {
        return value_sz;
    }

    public void setValue_sz(int value_sz) {
        this.value_sz = value_sz;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "LogRecoder{" +
                "crc=" + crc +
                ", tstamp=" + tstamp +
                ", logRecoderType=" + logRecoderType +
                ", key_sz=" + key_sz +
                ", value_sz=" + value_sz +
                ", key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
