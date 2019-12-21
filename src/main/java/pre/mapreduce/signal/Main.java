package pre.mapreduce.signal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pre.bean.Signal;

import java.io.IOException;

/*
数据清洗规则
1）抽取timestamp,imsi,lac_id,cell_id 四个字段*
2）去除imsi中，包含特殊字符的数据条目（‘#’,’*’,’^’）*
3）去除空间信息残缺的记录条目（imsi、lac_id、cell_id中为空）*
4）timestamp时间戳转换格式 ‘20190603000000’--年月日时分秒*
5）去除干扰数据条目（不是2018.10.03当天的数据）*
6）去除两数据源关联后经纬度为空的数据条目
7）以人为单位，按时间正序排序
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(Main.MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\Idea\\Traffic\\input\\原始数据.csv"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Idea\\Traffic\\output\\signal"));
        job.setNumReduceTasks(0);
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!key.toString().equals("0")) {
                //获取一行数据
                String line = value.toString();
                Signal o = Parser.parser(line);
                //去除空间信息残缺的记录条目（imsi、lac_id、cell_id中为空）
                if (o.getImsi().trim().isEmpty() || o.getLacId().trim().isEmpty() || o.getCellId().trim().isEmpty()) {
                    return;
                }
                //去除imsi中，包含特殊字符的数据条目（‘#’,’*’,’^’）
                if (o.getImsi().contains("#") || o.getImsi().contains("*") || o.getImsi().contains("^")) {
                    return;
                }
                //去除干扰数据条目（不是2018.10.03当天的数据
                if (!o.getTimestamp().contains("20181003")) {
                    return;
                }
                k.set(o.toString());
                context.write(k, v);

            }
        }

    }

}
