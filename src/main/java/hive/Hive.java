package hive;

import org.testng.annotations.Test;

import java.sql.*;

/*
$HIVE_HOME/bin/hive --service metastore &
$HIVE_HOME/bin/hive --service hiveserver2 &
 */
public class Hive {

    public static Statement state = null;
    public static Connection con = null;
    private static ResultSet res = null;

    //建立连接
    @Test
    public static void getconn() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
//        con = DriverManager.getConnection("jdbc:hive2://node01:10000/default", "root", "123456");
        con = DriverManager.getConnection("jdbc:hive2://node01:10000/trafficdb", "root", "123456");
        state = con.createStatement();
    }

    // 删除数据库
    @Test
    public void dropDb() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("drop database if exists trafficdb CASCADE");
    }

    // 创建数据库
    @Test
    public static void createdb() throws SQLException, ClassNotFoundException {
        getconn();
        boolean execute = state.execute("create database if not exists trafficdb");
        con.close();
    }


    // 创建Local表
    @Test
    public void createTabLocal() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("create table if not exists Local ( " +
                "longitude double , " +
                "latitude double , " +
                "lacId BIGINT ," +
                "cellId BIGINT )" +
                "row format delimited " +
                "fields terminated by '\001'" +
                "");
    }

    // 加载Local数据
    @Test
    public void loadDataLocal() throws SQLException, ClassNotFoundException {
        getconn();
        String infile = " '/Traffic/local/' ";
        state.execute("load data inpath " + infile + "overwrite into table Local");
    }


    // 创建Trip表
    @Test
    public void createTabTrip() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("create table if not exists Trip ( " +
                "longitude double , " +
                "latitude double , " +
                "mode string," +
                "modeName string," +
                "modeNum string)" +
                "row format delimited " +
                "fields terminated by '\001'" +
                "");
    }

    // 加载Trip数据
    @Test
    public void loadDataTrip() throws SQLException, ClassNotFoundException {
        getconn();
        String infile = " '/Traffic/trip/' ";
        state.execute("load data inpath " + infile + "overwrite into table Trip");
    }

    // 创建Signal表
    @Test
    public void createTabSignal() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("create table if not exists Signal ( " +
                "timestamp BIGINT , " +
                "imsi BIGINT , " +
                "lacId BIGINT," +
                "cellId BIGINT)" +
                "row format delimited " +
                "fields terminated by '\001'" +
                "");
    }

    // 加载Signal数据
    @Test
    public void loadDataSignal() throws SQLException, ClassNotFoundException {
        getconn();
        String infile = " '/Traffic/signal/' ";
        state.execute("load data inpath " + infile + "overwrite into table Signal");
    }


    // 创建SignalLocal表
    @Test
    public void createTabSignalLocal() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("create table if not exists SignalLocal ( " +
                "timestamp BIGINT , " +
                "imsi BIGINT , " +
                "longitude double," +
                "latitude double)" +
                "row format delimited " +
                "fields terminated by '\001'" +
                "");
    }

    //计算SignalLocal
    //去除两数据源关联后经纬度为空的数据条目
    //以人为单位，按时间正序排序
    @Test
    public void SignalLocal() throws SQLException, ClassNotFoundException {
        getconn();
        state.execute("insert overwrite table SignalLocal select timestamp, imsi,longitude,latitude" +
                " from Signal,Local where Signal.lacId=Local.lacId and Signal.cellId=Local.cellId " +
                "order by imsi ASC,timestamp ASC ");
    }

}

