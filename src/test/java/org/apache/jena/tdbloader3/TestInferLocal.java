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

import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.openjena.atlas.io.IO;
import org.openjena.atlas.lib.FileOps;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.Lang;
import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangRIOT;
import org.openjena.riot.lang.SinkQuadsToDataset;
import org.openjena.riot.pipeline.inf.InfFactory;

import cmd.tdbloader3;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.util.FileManager;

public class TestInferLocal {

	private static String output = "target/output" ;
	
    @Before public void setup() {
    	if ( FileOps.exists(output) ) {
        	FileOps.clearDirectory(output) ;    		
    	} else {
        	FileOps.ensureDir(output);    		
    	}
    }
	
    @Test public void test() throws Exception {
        String input = "src/test/resources/test-05/data.nt" ;
        String result = "src/test/resources/test-05/result.nt" ;
        String vocabulary = "src/test/resources/test-05/vocabulary.ttl" ;
        
        String[] args = new String[] {
        		"-conf", "conf/hadoop-local.xml",  
                "-D", Constants.OPTION_OVERRIDE_OUTPUT + "=true", 
        		"-D", Constants.OPTION_USE_COMPRESSION + "=" + Constants.OPTION_USE_COMPRESSION_DEFAULT,
        		vocabulary, 
                input, 
                output
        };
        assertEquals ( 0, ToolRunner.run(new InferDriver(), args) );

        // load the output of the MapReduce job
        DatasetGraph dsgMR = load(output + "/part-m-00000");

        // perform inference using RIOT infer 
        DatasetGraph dsgRIOT = infer(input, vocabulary);
        
        // expected result
        DatasetGraph dsgExpected = load(result);

        // assert isomorphism
        assertTrue( tdbloader3.dump(dsgRIOT, dsgMR), tdbloader3.isomorphic(dsgRIOT, dsgMR));
        assertTrue( tdbloader3.dump(dsgExpected, dsgMR), tdbloader3.isomorphic(dsgExpected, dsgMR));
    }
    
    private static DatasetGraph load(String input) {
        DatasetGraph dsg = DatasetFactory.create().asDatasetGraph();
        Sink<Quad> sink = new SinkQuadsToDataset(dsg);
        RiotReader.parseQuads(input, sink);
        
        return dsg;
    }
    
    private static DatasetGraph infer(String input, String vocabulary) {
    	Dataset dataset = DatasetFactory.create();
        Sink<Quad> sink = new SinkQuadsToDataset(dataset.asDatasetGraph());
        sink = InfFactory.infQuads(sink, FileManager.get().loadModel(vocabulary));
        LangRIOT parser = RiotReader.createParserQuads(IO.openFile(input), Lang.NQUADS, null, sink) ; 
        parser.parse() ;        

    	return dataset.asDatasetGraph();
    }

}
