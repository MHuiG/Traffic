package pre.tool;

import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class stampToDate {

    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    @Test
    public void Test() {
        System.out.println(stampToDate("1538498482636"));
    }
}
