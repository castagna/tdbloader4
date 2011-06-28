package com.talis.labs.tdb.tdbloader3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

@RunWith(Parameterized.class)
public class TestTDBLoader3 {

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
    
    public TestTDBLoader3 ( String input, String output ) {
        this.input = input ;
        this.output = output ;
    }
    
    @Test public void test() throws Exception { run (input, output); }
    
    private void run ( String input, String output ) throws Exception {
        String[] args = new String[] {"-conf", "conf/hadoop-local.xml", "-D", "overrideOutput=true", "-D", "copyToLocal=true", "-D", "verify=false", input, output};
        assertEquals ( 0, ToolRunner.run(new tdbloader3(), args) );
        DatasetGraphTDB dsgMem = tdbloader3.load(input);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(new Location(output)) ;
        assertTrue ( tdbloader3.dump(dsgMem, dsgDisk), tdbloader3.isomorphic ( dsgMem, dsgDisk ) );        
    }
    
}
