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
//        con = DriverManager.getConnection("jdbc:hive2://192.168.52.100:10000/default", "root", "123456");
        con = DriverManager.getConnection("jdbc:hive2://192.168.52.100:10000/trafficdb", "root", "123456");
        state = con.createStatement();
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
                "laci int ," +
                "laci int )" +
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
                "timestamp int , " +
                "imsi int , " +
                "lacId int," +
                "cellId int)" +
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


//    //计算评分汇总
//    @Test
//    public void Ratings() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("insert overwrite table RatingSum select title, sum(rating) sum" +
//                " from rating,movie where rating.movieId=movie.movieId " +
//                "group by title  " +
//                "order by sum DESC ");
//    }
//
//    @Test
//    public void createTabRatingSumByMovieId() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("create table if not exists RatingSumByMovieId ( " +
//                "movieId int, " +
//                "sum double)" +
//                "row format delimited " +
//                "fields terminated by '\001'" +
//                "");
//    }
//
//    @Test
//    public void RatingsByMovieId() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("insert overwrite table RatingSumByMovieId select movieId, sum(rating) sum from rating group by movieId order by sum DESC ");
//    }
//
//    //创建流派汇总表
//    @Test
//    public void createTabGenresSum() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("create table if not exists GenresSum ( " +
//                "Action int, Adventure int, Animation int, Children int, " +
//                "Comedy int, Crime int, Documentary int, " +
//                "Drama int, Fantasy int, FilmNoir int, Horror int," +
//                "Musical int, Mystery int, " +
//                "Romance int,SciFi int,Thriller int,War int,W" +
//                "estern int,no int" +
//                ")" +
//                "row format delimited " +
//                "fields terminated by '\001'" +
//                "");
//    }
//
//    //计算流派汇总
//    @Test
//    public void sumgenres() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("insert overwrite table GenresSum select sum(Action),sum(Adventure)," +
//                "sum(Animation),sum(Children),sum(Comedy),sum(Crime),sum(Documentary)," +
//                "sum(Drama),sum(Fantasy),sum(FilmNoir),sum(Horror),sum(Musical),sum(Mystery)," +
//                "sum(Romance),sum(SciFi),sum(Thriller),sum(War),sum(Western),sum(no) " +
//                "from genres");
//    }
//
//    //创建流派汇总评分表
//    @Test
//    public void createTabGenresSumRating() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("create table if not exists GenresSumRating ( " +
//                "Action double, Adventure double, Animation double, Children double, " +
//                "Comedy double, Crime double, Documentary double, Drama double, " +
//                "Fantasy double, FilmNoir double, Horror double,Musical double, Mystery double, " +
//                "Romance double,SciFi double,Thriller double,War double,Western double,no double" +
//                ")" +
//                "row format delimited " +
//                "fields terminated by '\001'" +
//                "");
//    }
//
//    //计算流派评分
//    @Test
//    public void sumgenresRating() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("insert overwrite table GenresSumRating select sum(a.sum*b.Action),sum(a.sum*b.Adventure)," +
//                "sum(a.sum*b.Animation),sum(a.sum*b.Children),sum(a.sum*b.Comedy),sum(a.sum*b.Crime)," +
//                "sum(a.sum*b.Documentary),sum(a.sum*b.Drama),sum(a.sum*b.Fantasy),sum(a.sum*b.FilmNoir)," +
//                "sum(a.sum*b.Horror),sum(a.sum*b.Musical),sum(a.sum*b.Mystery),sum(a.sum*b.Romance),sum(a.sum*b.SciFi)," +
//                "sum(a.sum*b.Thriller),sum(a.sum*b.War),sum(a.sum*b.Western),sum(a.sum*b.no) " +
//                "from RatingSumByMovieId a,genres b where a.movieId=b.movieId");
//    }
//
//    //wordcount
//    @Test
//    public void createTagSum() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("create table if not exists TagSum ( " +
//                "tag String,sum int" +
//                ")" +
//                "row format delimited " +
//                "fields terminated by '\001'" +
//                "");
//    }
//
//    @Test
//    public void TagSum() throws SQLException, ClassNotFoundException {
//        getconn();
//        state.execute("insert overwrite table TagSum select tag,count(*) from tag group by tag");
//    }


}

