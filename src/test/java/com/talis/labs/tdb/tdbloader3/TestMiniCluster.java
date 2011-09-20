package com.talis.labs.tdb.tdbloader3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

public class TestMiniCluster extends AbstractMiniMRClusterTest {

    @BeforeClass public static void before() throws IOException {
    	startCluster() ;
    }
    
    @AfterClass public static void after() {
    	stopCluster() ;
    }
	
    @Test public void test() throws Exception {
        String input = "src/test/resources/input" ;
        String output = "target/output" ;
        String[] args = new String[] {
                "-conf", config, 
                "-D", "overrideOutput=true", 
                "-D", "copyToLocal=true", 
                "-D", "verify=true", 
                "-D", "runLocal=false",
                "-D", "numReducers=3", 
                input, 
                output
        };
        assertEquals ( 0, ToolRunner.run(new tdbloader3(), args) );
        DatasetGraphTDB dsgMem = tdbloader3.load(input);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(new Location(output)) ;
        assertTrue ( tdbloader3.dump(dsgMem, dsgDisk), tdbloader3.isomorphic ( dsgMem, dsgDisk ) );       
    }

}
