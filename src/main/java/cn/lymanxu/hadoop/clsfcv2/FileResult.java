package cn.lymanxu.hadoop.clsfcv2;

import java.util.HashMap;
import java.util.Map;

public class FileResult {
    private String fileName = "";
    private String lableClassName = "";
    private String cacClassName = "";
    private Map<String, Double>  classPOfFile = new HashMap<String, Double>();

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getLableClassName() {
        return lableClassName;
    }

    public void setLableClassName(String lableClassName) {
        this.lableClassName = lableClassName;
    }

    public String getCacClassName() {
        return cacClassName;
    }

    public void setCacClassName(String cacClassName) {
        this.cacClassName = cacClassName;
    }

    public Map<String, Double> getClassPOfFile() {
        return classPOfFile;
    }

    public void setClassPOfFile(Map<String, Double> classPOfFile) {
        this.classPOfFile = classPOfFile;
    }

    @Override
    public String toString() {
        return "FileResult{" +
                "fileName='" + fileName + '\'' +
                ", lableClassName='" + lableClassName + '\'' +
                ", cacClassName='" + cacClassName + '\'' +
                ", classPOfFile=" + classPOfFile.toString() +
                '}';
    }
}
