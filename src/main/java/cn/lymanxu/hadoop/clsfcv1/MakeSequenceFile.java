package cn.lymanxu.hadoop.clsfcv1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class MakeSequenceFile {

    public static String getFileContent(String uri) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        Path path = new Path(uri);

        long len = fs.getFileStatus(path).getLen();
        System.out.println(len);
        byte[] buff = new byte[(int)len];
        String str = null;

        InputStream in = new BufferedInputStream(fs.open(new Path(uri)));
        if(len == in.read(buff)){
            str = new String(buff);
            //System.out.println(str);
            return str;
        }

        return str;
    }


    public void squenceFileWrite(String uriInput, String uriSeq) throws IOException {
        // get the record of 1:-10 files
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uriInput), conf);
        Path path = new Path(uriInput);
        FileStatus[] status = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(status);

        Integer sumFile = paths.length;

        // the params for getting sequencefile
        FileSystem fsSeq = FileSystem.get(URI.create(uriSeq),  conf);
        Path pathSeq = new Path(uriSeq);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fsSeq, conf, pathSeq, key.getClass(), value.getClass());

        try{
            for(int i = 0; i < sumFile; i++){
                System.out.println(paths[i]);
                // read the content of the file
                String str = getFileContent(paths[i].toString());
                //System.out.println(str);
                if(str!=null){
                    key.set(i);
                    value.set(str);
                    writer.append(key, value);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            IOUtils.closeStream(writer);
        }

    }

    public void sequenceFileRead(String uri) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        long position = reader.getPosition();
        while(reader.next(key, value)){

            System.out.printf("[%s]\t%s\t", position, key);
            position = reader.getPosition();
        }
    }


    public static void main(String[] args) throws Exception {

        String inputBRAZ = "hdfs://localhost:9000/input/BRAZ";
        String inputSeq = "hdfs://localhost:9000/input/BRAZ.txt";

        MakeSequenceFile sequenceFile = new MakeSequenceFile();
        sequenceFile.squenceFileWrite(inputBRAZ, inputSeq);
        System.out.println("@-------------read sequencefile");
        sequenceFile.sequenceFileRead(inputSeq);
    }
}
