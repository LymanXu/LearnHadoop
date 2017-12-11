package cn.lymanxu.hadoop.clsfcv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class WithPathFileInputFormat extends FileInputFormat{
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WithPathRecordReader reader = new WithPathRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}

class WithPathRecordReader extends RecordReader<Text, Text> {

    private LineReader lr ;
    private FileSplit split;
    private Configuration conf;
    private Path path;
    private FileSystem fs;
    private Text key = new Text();
    private Text value = new Text();
    private long start ;
    private long end;
    private long currentPos;
    private Text line = new Text();
    private String parentDir = "";

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
        this.path = split.getPath();
        this.fs = path.getFileSystem(conf);
        FSDataInputStream in = fs.open(path);
        lr = new LineReader(in, conf);
        // 处理起始点和终止点
        start =split.getStart();
        end = start + split.getLength();

        // the direct of train files
        String[] paths = path.getParent().toString().split("/");
        this.parentDir = paths[paths.length-1];

        in.seek(start);
        if(start!=0){
            start += lr.readLine(new Text(),0,
                    (int)Math.min(Integer.MAX_VALUE, end-start));
        }
        currentPos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentPos > end){
            return false;
        }
        currentPos += lr.readLine(line);
        if(line.getLength()==0){
            return false;
        }

        String words = line.toString();
        key.set(this.parentDir);
        value.set(words);
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lr.close();
    }
}