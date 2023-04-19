package com.dafei1288.jimsql.server.storage.bitcask.log;

public class LogRecoderPos {

    public LogRecoderPos() {
    }

    public LogRecoderPos(String key, int file_id, int recoder_pos, int value_sz, long tstamp) {
        this.key = key;
        this.file_id = file_id;
        this.recoder_pos = recoder_pos;
        this.value_sz = value_sz;
        this.tstamp = tstamp;
    }

    private String key;
    private int file_id;
    private int recoder_pos;
    private int value_sz;
    private long tstamp;


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getFile_id() {
        return file_id;
    }

    public void setFile_id(int file_id) {
        this.file_id = file_id;
    }

    public int getValue_sz() {
        return value_sz;
    }

    public void setValue_sz(int value_sz) {
        this.value_sz = value_sz;
    }

    public int getRecoder_pos() {
        return recoder_pos;
    }

    public void setRecoder_pos(int recoder_pos) {
        this.recoder_pos = recoder_pos;
    }

    public long getTstamp() {
        return tstamp;
    }

    public void setTstamp(long tstamp) {
        this.tstamp = tstamp;
    }

    @Override
    public String toString() {
        return "LogRecoderPos{" +
                "key='" + key + '\'' +
                ", file_id=" + file_id +
                ", value_sz=" + value_sz +
                ", recoder_pos=" + recoder_pos +
                ", tstamp=" + tstamp +
                '}';
    }
}
