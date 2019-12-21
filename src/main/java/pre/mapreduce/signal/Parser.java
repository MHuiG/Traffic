package pre.mapreduce.signal;

import pre.bean.Signal;

public class Parser {
    public static Signal parser(String line) {
        Signal o = new Signal();
        //把一行数据以,字符切割并存入数组arr中
        String[] arr = line.split(",");
        o.setTimestamp(arr[0]);
        o.setImsi(arr[1]);
        o.setLacId(arr[2]);
        o.setCellId(arr[3]);
        return o;
    }
}
