import java.util.Arrays;

public class T1 {

  public static void main(String[] args) {
    String res = "{\"ahmReceiptTime\":1655006308412,\"category\":\"OOOI\",\"content\":\"\\u0001QU BLVBOCR\\r\\n.BJSXCXA 111249\\r\\n\\u0002M31\\r\\nFI HU7704/AN B-7880\\r\\nDT BJS SZX 111249 M09A\\r\\n-  OUT\\r\\nZGSZ0458ZBAA 2643\\r\\n\\u0003\",\"contentTime\":1655006308412,\"destAddress\":\"BLVBOCR\",\"flightNo\":\"HU7704\",\"id\":\"62a5652d9f85ff3cb6db4110\",\"msgSeq\":\"M09A\",\"originStatus\":4,\"priority\":\"\\u0001QU\",\"providerAddress\":\".BJSXCXA\",\"providerTime\":1654951740000,\"receiptTime\":1654951740000,\"reportType\":\"M31\",\"sendTime\":1655006509606,\"smi\":\"M31\",\"status\":4,\"tailNumber\":\"B-7880\"}";
    String[] ss = res.split("\\\\r?\\\\n");
    Arrays.stream(ss).forEach(System.out::println);
//
    String unicodeString = "\\u0002M31";
    System.out.println(CharSetUtil.decodeUnicode(unicodeString));
  }
}
