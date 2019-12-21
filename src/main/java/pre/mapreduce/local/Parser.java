package pre.mapreduce.local;

import pre.bean.Local;

public class Parser {
    public static Local parser(String line) {
        Local o = new Local();
        //把一行数据以,字符切割并存入数组arr中
        String[] arr = line.split(",");
        o.setLongitude(arr[0]);
        o.setLatitude(arr[1]);
        o.setLaci(arr[2]);
        return o;
    }
}
