package cn.lymanxu.hadoop.clsfcv2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCountTest {

    public static final String separator = ",";
    public static Map<String, Map<String, Double>> wordPMap = new HashMap();
    // record the number of file with same classification
    public static Map<String, Integer> fileNumMap = new HashMap();
    // the p offiles in C class
    public static Map<String, Double> filePc = new HashMap<String, Double>();

    public static class MyMapper extends Mapper<Text, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String keyClassfication = key.toString();

            while (itr.hasMoreTokens()){
                word.set(keyClassfication + separator + itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // transform the format of the data
            Map<String, Double> wordEntry = new HashMap();
            String[] classAndWord = key.toString().split(separator);
            String classfication = classAndWord[0];
            String word = classAndWord[1];

            // update the num of each classification
            if (fileNumMap.containsKey(classfication)){
                Integer num = fileNumMap.get(classfication);
                num += sum;
                fileNumMap.put(classfication.toString(), num);
            }else {
                fileNumMap.put(classfication.toString(), sum);
            }
            // update the num of each word
            if (wordPMap.containsKey(word)) {
                wordEntry = wordPMap.get(word);
                wordEntry.put(classfication, Double.valueOf(sum));
            } else {
                wordEntry.put(classfication, Double.valueOf(sum));
                wordPMap.put(word, wordEntry);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Map.Entry<String, Map<String, Double>> aWord : wordPMap.entrySet()){
                Text textWord = new Text();
                textWord.set(aWord.getKey());
                // caculate the P of each word in diffrent class
                Map<String, Double> wordNum2P = aWord.getValue();
                for (String classText : fileNumMap.keySet()){
                    if (wordNum2P.containsKey(classText)){
                        Double tkP = wordNum2P.get(classText) / fileNumMap.get(classText);
                        wordNum2P.put(classText, tkP);
                    }else{
                        Double tkP = 1.0d / fileNumMap.get(classText);
                        wordNum2P.put(classText, tkP);
                    }
                }
                Text textMap = new Text();
                textMap.set(aWord.getValue().toString());
                context.write(textWord, textMap);
            }

            System.out.println("-------------------fileNumMap-----------------");
            for (Map.Entry aFileNum : fileNumMap.entrySet()) {
                System.out.println(aFileNum.getKey() + ": --------------" + aFileNum.getValue());
            }
            // caculate the P of Pc
            Double sumFileNum = 0.0;
            for (Map.Entry<String, Integer> aClass : fileNumMap.entrySet()){
                sumFileNum += aClass.getValue();
            }
            for (Map.Entry<String, Integer> aClass : fileNumMap.entrySet()){
                Double pc = (aClass.getValue() / sumFileNum); // * Math.pow(10,200);
                filePc.put(aClass.getKey(), pc);
            }

            // test the class
            String parentDir = "hdfs://localhost:9000/test/";
            TestClassification testClassification = new TestClassification();
            testClassification.testClass( parentDir, wordPMap, filePc, fileNumMap);

        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        args = new String[]{"hdfs://localhost:9000/input/", "hdfs://localhost:9000/output"};

        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCountTest.class);
        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(WordCountTest.MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(WithPathFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        if(job.waitForCompletion(true) ?  true: false){
            System.out.println("----------------run job success , caculate Pptc-----");
        }
    }

}
