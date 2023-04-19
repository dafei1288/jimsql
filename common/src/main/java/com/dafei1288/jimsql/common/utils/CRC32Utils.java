package com.dafei1288.jimsql.common.utils;

import java.util.Locale;
import java.util.zip.CRC32;

public class CRC32Utils {
    public static long encode(byte[] data){
        CRC32 crc32 = new CRC32();
        crc32.update(data);

        return crc32.getValue();
                //String.format(Locale.US,"%08X", crc32.getValue());
    }

    public static String toHexString(byte[] data){
        return String.format(Locale.US,"%08X", encode(data));
    }

}
