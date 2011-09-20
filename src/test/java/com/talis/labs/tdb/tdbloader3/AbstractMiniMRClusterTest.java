package com.talis.labs.tdb.tdbloader3;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

public abstract class AbstractMiniMRClusterTest {

	private static MiniDFSCluster dfsCluster ; 
    private static MiniMRCluster mrCluster ;
    private static FileSystem fs ;
    private static final int numNodes = 2 ;
    protected static final String config = "target/hadoop-localhost-test.xml" ;
    
    public static void startCluster() throws IOException {
        Configuration configuration = new Configuration() ;
        System.setProperty("hadoop.log.dir", "build/test/logs") ;
        dfsCluster = new MiniDFSCluster(configuration, numNodes, true, null) ;
        mrCluster = new MiniMRCluster(numNodes, dfsCluster.getFileSystem().getUri().toString(), 1) ;
        
        // Generate Hadoop configuration
        FileOutputStream out = new FileOutputStream (config) ;
        mrCluster.createJobConf().writeXml(out) ;
        out.close() ;
        
        // Copy testing data onto (H)DFS
        fs = dfsCluster.getFileSystem() ;
        fs.copyFromLocalFile(new Path("src/test/resources"), new Path("src/test/resources")) ;
    }
    
    public static void stopCluster() {
        dfsCluster.shutdown() ;
        dfsCluster = null ;
        mrCluster.shutdown() ;
        mrCluster = null ;
    }

}
