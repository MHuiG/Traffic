package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.annotations.Test;

import java.net.URI;


public class hadoop {
    @Test
    public void get1() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void get2() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }

    @Test
    public void get3() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node:8020");
        FileSystem fileSystem = FileSystem.newInstance(conf);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void get4() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }

    @Test
    public void createDir() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node:8020"), new Configuration());
        fileSystem.mkdirs(new Path("/slg3/jsj/dsj"));
        fileSystem.close();
    }

    @Test
    public void upLoad() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node:8020"), new Configuration());
        fileSystem.copyFromLocalFile(false, new Path("file:///F:\\活着.txt"), new Path("/slg3/jsj/dsj"));
        fileSystem.close();
    }

    @Test
    public void listFiles() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path, true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path1 = next.getPath();
            System.out.println(path1.toString());
        }
        fileSystem.close();
    }
}

