package com.claire.modelTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

public class CAT {
	
	
	private static final String HDFS = "hdfs://localhost:9000/";
	
	/**
     * ~ hadoop fs -cat /tmp/new/item.csv
     * @param remoteFile
     * @throws IOException
     */
    public static String cat(String remoteFile) throws IOException {
    	Configuration conf = new Configuration();
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(HDFS), conf);
        FSDataInputStream fsdis = null;
        try {
            fsdis =fs.open(path);
            String str= "";
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while(out.size()!=0){
            	IOUtils.copyBytes(fsdis, out, 4096, false);
            	str += out.toString();
            }
            out.close();
            return str;
          } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
          }
    }

}
