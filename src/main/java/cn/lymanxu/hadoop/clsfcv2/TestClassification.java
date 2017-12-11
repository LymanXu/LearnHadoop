package cn.lymanxu.hadoop.clsfcv2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class TestClassification {

    public static List<FileResult> fileResultList = new ArrayList<FileResult>();

    public void readDFSLine(Path aFilePath, Map<String, Map<String, Double>> wordPMap, Map<String, Double> fileClassPMap,
                            Map<String, Integer> fileClassNumMap) {
        //String uri = aFileUri; //"hdfs://localhost:9000/input/BRAZ/487455newsML.txt";
        Configuration conf = new Configuration();
        BufferedReader reader = null;
        //Path path = new Path(uri);
        String[] paths = aFilePath.getParent().toString().split("/");
        String parentDir = paths[paths.length-1];

        Map<String, Double> fileSumPMap = new HashMap<String, Double>();
        try {
            FileSystem fsAFile = aFilePath.getFileSystem(conf);
            InputStream in = fsAFile.open(aFilePath);
            reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null){

                List<Double> tempPList = new ArrayList<Double>();
                // update the P of each class
                for (Map.Entry<String, Double> aFileClass : fileClassPMap.entrySet()){
                    String classText = aFileClass.getKey();

                    if (fileSumPMap.containsKey(classText)){
                        // have recorded the P of this Class
                        Double wordPEachClass = 0.0;
                        if (wordPMap.containsKey(line)) {
                            wordPEachClass = wordPMap.get(line).get(classText);
                        }else{
                            wordPEachClass = 1.0 / fileClassNumMap.get(classText);
                        }

                        Double tempP = fileSumPMap.get(classText) * wordPEachClass;
                        if (tempP <= 0){
                             System.out.println("@-------------- word P2 <=0-------------" + "fileSumP:" + fileSumPMap.get(classText) + "wordPEach: "+wordPEachClass);

                        }
                        fileSumPMap.put(classText, tempP);
                    }else{
                        Double cP = fileClassPMap.get(classText);
                        fileSumPMap.put(classText, cP);
                    }
                    tempPList.add(fileSumPMap.get(classText));
                }

                //
                Double maxFileP = Collections.max(tempPList);
                for (Map.Entry<String, Double> largeFileP : fileSumPMap.entrySet()){
                    Double largeP = largeFileP.getValue() / maxFileP;
                    fileSumPMap.put(largeFileP.getKey(), largeP);
                }

            }

            // select the max P, and record the result
            Double tempPClass = 0.0;
            String dstClass = "";
            for (Map.Entry<String, Double> pClass: fileSumPMap.entrySet()){
                if (pClass.getValue() > tempPClass){
                    tempPClass = pClass.getValue();
                    dstClass = pClass.getKey();
                }
            }

            FileResult fileResult = new FileResult();
            fileResult.setFileName(aFilePath.toString());
            fileResult.setLableClassName(parentDir);
            fileResult.setCacClassName(dstClass);
            fileResult.setClassPOfFile(fileSumPMap);
            fileResultList.add(fileResult);

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // test the class
    public void testClass(String parentDir, Map<String, Map<String, Double>> wordPMap, Map<String, Double> fileClassPMap,
                          Map<String, Integer> fileClassNumMap) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(parentDir), conf);

        FileStatus[] testClassFiles = fs.listStatus(new Path(parentDir));

        for (int i = 0 ; i < testClassFiles.length; i++){
            // test the files in each test class
            FileStatus[] files = fs.listStatus(new Path(testClassFiles[i].getPath().toString()));

            for (int j = 0; j < files.length; j++){
                Path aFilePath = files[j].getPath();
                readDFSLine( aFilePath,  wordPMap, fileClassPMap, fileClassNumMap);
            }
         }

         // log the result
        System.out.println("-----------------file test reuslt ----------------------");
        for (FileResult fileResult : fileResultList){
            System.out.println(fileResult.toString());
        }
    }

    public static void main(String[] args) throws IOException {
       return;
    }
}
