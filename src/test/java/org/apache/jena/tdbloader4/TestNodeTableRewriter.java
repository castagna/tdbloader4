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

package org.apache.jena.tdbloader4;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openjena.atlas.lib.FileOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cmd.tdbloader4;

import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.TDBMaker;
import com.hp.hpl.jena.tdb.transaction.DatasetGraphTransaction;

@RunWith(Parameterized.class)
public class TestNodeTableRewriter {

    private static final Logger log = LoggerFactory.getLogger(TestNodeTableRewriter.class);
    
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(TestScriptedLocal.TEST_DATA);
    }

    private String input ;
    private String output ;
    
    public TestNodeTableRewriter ( String input, String output ) {
        this.input = input ;
        this.output = output ;
    }

    @Before public void setup() {
        if ( FileOps.exists(output) ) {
            FileOps.clearDirectory(output) ;            
        } else {
            FileOps.ensureDir(output);          
        }
    }
    
    @Test public void test() throws Exception { 
        run (input, output); 
    }
    
    private void run ( String input, String output ) throws Exception {
        List<String> urls = new ArrayList<String>();
        for (File file : new File(input).listFiles()) {
            if (file.isFile()) {
                urls.add(file.getAbsolutePath());
            }
        }
        
        Location location = new Location(output);
        DatasetGraphTDB dsgDisk = TDBMaker.uncachedFactory.createDatasetGraph(location);
        TDBLoader.load(dsgDisk, urls);

        DatasetGraphTransaction dsgMem = (DatasetGraphTransaction)TDBFactory.createDatasetGraph();
        TDBLoader.load(dsgMem.getBaseDatasetGraph(), urls);
        
        NodeTableRewriter.fixNodeTable2(location, log, null);
        assertTrue ( tdbloader4.dump(dsgMem, dsgDisk), tdbloader4.isomorphic ( dsgMem, dsgDisk ) );
    }
    
}
