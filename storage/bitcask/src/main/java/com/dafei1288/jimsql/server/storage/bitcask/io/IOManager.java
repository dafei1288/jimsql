package com.dafei1288.jimsql.server.storage.bitcask.io;

import com.dafei1288.jimsql.common.utils.CRC32Utils;
import com.dafei1288.jimsql.server.storage.bitcask.log.LogRecoderPos;
import xyz.proadap.aliang.BPlusTree;

import java.util.List;

public class IOManager {
    public static void main(String[] args) {
        System.out.println(CRC32Utils.encode("jianggujin".getBytes()));
        System.out.println("724585211");


        BPlusTree<Integer, LogRecoderPos> bPlusTree = new BPlusTree<>();
        bPlusTree.insert(0, new LogRecoderPos("d1",1,1,1,1));
        bPlusTree.insert(0, new LogRecoderPos("d2",1,1,1,1));
        bPlusTree.insert(1, new LogRecoderPos("d3",1,1,1,1));
        bPlusTree.insert(2, new LogRecoderPos("d4",1,1,1,1));
        bPlusTree.insert(3, new LogRecoderPos("d5",1,1,1,1));

        List<LogRecoderPos> queryResult = bPlusTree.query(0);
        System.out.println(queryResult);

    }

}
