package pre.tool;

import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Tool {

    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt * 1000);
        res = simpleDateFormat.format(date);
        return res;
    }

    @Test
    public void Test() {
        System.out.println(stampToDate("1218207244"));
    }
}
