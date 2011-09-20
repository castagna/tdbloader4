/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.tdbloader3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openjena.atlas.lib.FileOps;

import cmd.tdbloader3;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

@RunWith(Parameterized.class)
public class TestScriptedLocal {

	public static final Object[][] TEST_DATA = new Object[][] {
        { "src/test/resources/test-01", "target/output"}, 
        { "src/test/resources/test-02", "target/output" }, 
        { "src/test/resources/test-03", "target/output" },
        { "src/test/resources/test-04", "target/output" },
	};

    @Before public void setup() {
    	FileOps.clearDirectory(output) ;
    }
	
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(TEST_DATA);
    }

    private String input ;
    private String output ;
    
    public TestScriptedLocal ( String input, String output ) {
        this.input = input ;
        this.output = output ;
    }
    
    @Ignore
    @Test public void test() throws Exception { 
    	run (input, output); 
    }
    
    private void run ( String input, String output ) throws Exception {
        String[] args = new String[] {
        		"-conf", "conf/hadoop-local.xml", 
        		"-D", "overrideOutput=true", 
        		"-D", "useCompression=true", 
        		"-D", "copyToLocal=true", 
        		"-D", "verify=false", 
        		"-D", "runLocal=true",
        		input, 
        		output
        };
        assertEquals ( 0, ToolRunner.run(new tdbloader3(), args) );
        DatasetGraphTDB dsgMem = tdbloader3.load(input);
        DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(new Location(output)) ;
        assertTrue ( tdbloader3.dump(dsgMem, dsgDisk), tdbloader3.isomorphic ( dsgMem, dsgDisk ) );        
    }
    
}
