package pre.mapreduce.trip;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pre.bean.Trip;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(Main.MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\Idea\\Traffic\\input\\出行方式静态数据.csv"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Idea\\Traffic\\output\\trip"));
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
                Trip o = Parser.parser(line);
                k.set(o.toString());
                context.write(k, v);
            }
        }

    }

}
