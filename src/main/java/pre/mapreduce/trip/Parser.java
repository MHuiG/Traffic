package pre.mapreduce.trip;

import pre.bean.Trip;

public class Parser {
    public static Trip parser(String line) {
        Trip o = new Trip();
        //把一行数据以,字符切割并存入数组arr中
        String[] arr = line.split(",");
        o.setLongitude(arr[0]);
        o.setLatitude(arr[1]);
        o.setMode(arr[2]);
        o.setModeName(arr[3]);
        if (arr.length == 5) {
            o.setModeNum(arr[4]);
        }
        return o;
    }
}
