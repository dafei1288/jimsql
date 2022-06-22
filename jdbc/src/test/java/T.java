public class T {

  public static void main(String[] args) {
    Object[] o = new Object[10];
    Object[] o_new = new Object[o.length];
    for(int i=0;i<o.length;i++){
      Object on = new Object();
//      o_new[i] = BeanUtils.copyProperties(o,on);
    }
  }
}
