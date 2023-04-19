package com.dafei1288.jimsql.server.storage.bitcask.index;

public interface Index<T> {
    boolean put(String key , T o);

    T get(String key);

    boolean del(String key);
}
