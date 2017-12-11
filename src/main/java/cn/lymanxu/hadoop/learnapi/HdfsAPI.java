package cn.lymanxu.hadoop.learnapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class HdfsAPI {

    public static void main(String[] args) throws Exception {

        HdfsAPI hdfs = new HdfsAPI();
        //hdfs.readdfs();
        hdfs.writedfs();
    }

    public void readdfs() throws IOException {
        String uri = "hdfs://localhost:9000/input/BRAZ/488910newsML.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in =null;
        in = fs.open(new Path(uri));
        IOUtils.copyBytes(in, System.out, 4096, false);
    }

    public void writedfs() throws Exception {
        String dst = "hdfs://localhost:9000/input/test.txt";
        String local = "/home/lyman/文档/job-pagination-commit-msg.txt";

        InputStream in = new BufferedInputStream(new FileInputStream(local));

        // get the outputstream for hdfs
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst));
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
