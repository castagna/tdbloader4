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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
    	FileUtils.deleteDirectory(new File("build/test")) ;

    	Configuration configuration = new Configuration() ;
    	configuration.set("dfs.datanode.data.dir.perm", "775") ;
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
    
    public static void stopCluster() throws IOException {
        if ( dfsCluster != null ) {
        	dfsCluster.shutdown() ;
            dfsCluster = null ;
        }
        if ( mrCluster != null ) {
            mrCluster.shutdown() ;
            mrCluster = null ;        	
        }
    }

}
