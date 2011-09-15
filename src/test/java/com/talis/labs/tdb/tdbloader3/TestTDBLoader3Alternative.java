package com.talis.labs.tdb.tdbloader3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.talis.labs.tdb.tdbloader3.dev.tdbloader3;

@RunWith(Parameterized.class)
public class TestTDBLoader3Alternative {

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

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "src/test/resources/test-01", "target/output" }, 
                { "src/test/resources/test-02", "target/output" }, 
                { "src/test/resources/test-03", "target/output" },
                { "src/test/resources/test-04", "target/output" },
        });
    }

    private String input ;
    private String output ;
    
    public TestTDBLoader3Alternative ( String input, String output ) {
        this.input = input ;
        this.output = output ;
    }
    
    @Test public void test() throws Exception { 
    	run (input, output); 
    }
    
    private void run ( String input, String output ) throws Exception {
        String[] args = new String[] {
        		"-conf", "conf/hadoop-local.xml", 
        		"-D", "overrideOutput=true", 
        		"-D", "copyToLocal=true", 
        		"-D", "verify=false", 
        		input, output};
        assertEquals ( 0, ToolRunner.run(new tdbloader3(), args) );
        TestNodeTableBuilding.fixNodeTable(new Location(output)); // TODO: this will go away!
        DatasetGraphTDB dsgMem = tdbloader3.load(input);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(new Location(output)) ;
        assertTrue ( tdbloader3.dump(dsgMem, dsgDisk), tdbloader3.isomorphic ( dsgMem, dsgDisk ) );        
    }
    
}
