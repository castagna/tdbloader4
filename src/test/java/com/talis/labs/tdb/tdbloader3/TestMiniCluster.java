package com.talis.labs.tdb.tdbloader3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.talis.labs.tdb.tdbloader3.dev.tdbloader3;

public class TestMiniCluster {

    
    private static MiniDFSCluster dfsCluster ; 
    private static MiniMRCluster mrCluster ;
    private static FileSystem fs ;
    private static final int numNodes = 2 ;
    private static final String config = "target/hadoop-localhost-test.xml" ;
    
    @BeforeClass public static void startCluster() throws IOException {
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
    
    @AfterClass public static void stopCluster() {
        dfsCluster.shutdown() ;
        dfsCluster = null ;
        mrCluster.shutdown() ;
        mrCluster = null ;
    }

    @Test public void test() throws Exception {
        String input = "src/test/resources/input" ;
        String output = "target/output" ;
        String[] args = new String[] {
                "-conf", config, 
                "-D", "overrideOutput=true", 
                "-D", "copyToLocal=true", 
                "-D", "verify=false", 
                "-D", "nquadInputFormat=false", 
                "-D", "runLocal=false",
                input, 
                output
        };
        
        assertEquals ( 0, ToolRunner.run(new tdbloader3(), args) );
        DatasetGraphTDB dsgMem = tdbloader3.load(input, Location.mem());
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(new Location(output)) ;
        assertTrue ( tdbloader3.dump(dsgMem, dsgDisk), tdbloader3.isomorphic ( dsgMem, dsgDisk ) );       
    }

}
