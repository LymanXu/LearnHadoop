package cn.lymanxu.hadoop.clsfcv1;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCountP {

    private static Map<String, Float> Bpdc = new HashMap();
    private static int filesNum = 0;
    private static Float Bp = 0f;

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            Bpdc.put(key.toString(), (float) sum);
            filesNum++;
            context.write(key, result);
        }
    }

    public static void caculateBpdc() throws IOException {
        Set<Map.Entry<String, Float>> iter = Bpdc.entrySet();
        // the params for getting sequencefile
        String uriSeq = "hdfs://localhost:9000/input/BRAZPtc.txt";
        Configuration conf = new Configuration();
        FileSystem fsSeq = FileSystem.get(URI.create(uriSeq),  conf);
        Path pathSeq = new Path(uriSeq);
        Text key = new Text();
        FloatWritable value = new FloatWritable();
        SequenceFile.Writer writer = SequenceFile.createWriter(fsSeq, conf, pathSeq, key.getClass(), value.getClass());


        String text = "";
        Float ptc = 0f;
        try{
            for(Map.Entry<String, Float> entry : iter){

                text = entry.getKey();
                ptc = entry.getValue()/filesNum;
                Bpdc.put(text, ptc);
                key.set(text);
                value.set(ptc);
                writer.append(key, value);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(writer);
        }


    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"hdfs://localhost:9000/input/BRAZ.txt", "hdfs://localhost:9000/output"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCountP.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        if(job.waitForCompletion(true) ?  true: false){
            System.out.println("----------------run job success , caculate Pptc-----");
        }
        WordCountP.caculateBpdc();
    }
}
