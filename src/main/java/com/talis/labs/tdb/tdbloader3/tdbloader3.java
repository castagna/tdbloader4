/*
 * Copyright 2010,2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.labs.tdb.tdbloader3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class tdbloader3 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);

        boolean overrideOutput = configuration.getBoolean("overrideOutput", false);
        boolean copyToLocal = configuration.getBoolean("copyToLocal", true);
        boolean verify = configuration.getBoolean("verify", false);
        boolean nquadInputFormat = configuration.getBoolean("nquadInputFormat", false);
        
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
            fs.delete(new Path(args[1] + "_1"), true);
            fs.delete(new Path(args[1] + "_2"), true);
            fs.delete(new Path(args[1] + "_3"), true);
        }
		
        if ( nquadInputFormat ) {
        	FirstDriver.setUseNQuadsInputFormat(true);
        }
        FirstDriver first = new FirstDriver(configuration);
        first.run(new String[] { args[0], args[1] + "_1" });
        
        SecondDriver second = new SecondDriver(configuration);
        second.run(new String[] { args[1] + "_1", args[1] + "_2" });
        
        ThirdDriver third = new ThirdDriver(configuration);
        third.run(new String[] { args[1] + "_2", args[1] + "_3" });
        
        if ( copyToLocal ) {
            Location location = new Location(args[1]);
            DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
            dsgDisk.sync(); 
            dsgDisk.close();

            copyToLocalFile(fs, new Path(args[1] + "_1"), new Path(args[1]));
            copyToLocalFile(fs, new Path(args[1] + "_3"), new Path(args[1]));            
        }
        
        if ( verify ) {
            DatasetGraphTDB dsgMem = load(args[0]);
            Location location = new Location(args[1]);
            DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
            boolean isomorphic = isomorphic ( dsgMem, dsgDisk );
            System.out.println ("> " + isomorphic);
        }
        
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new tdbloader3(), args);
		System.exit(exitCode);
	}
	
	private void copyToLocalFile ( FileSystem fs, Path src, Path dst ) throws FileNotFoundException, IOException {
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(src, true);
        while ( iter.hasNext() ) {
            LocatedFileStatus lfs = iter.next();
            Path path = lfs.getPath();
            String pathName = path.getName();
            if ( pathName.endsWith(".idn") || pathName.endsWith(".dat") ) {
                fs.copyToLocalFile(path, new Path(dst, path.getName()));
            }
        }
	}

    public static boolean isomorphic(DatasetGraphTDB dsgMem, DatasetGraphTDB dsgDisk) {
        if (!dsgMem.getDefaultGraph().isIsomorphicWith(dsgDisk.getDefaultGraph()))
            return false;
        Iterator<Node> graphsMem = dsgMem.listGraphNodes();
        Iterator<Node> graphsDisk = dsgDisk.listGraphNodes();
        Set<Node> seen = new HashSet<Node>();

        while (graphsMem.hasNext()) {
            Node graphNode = graphsMem.next();
            if (dsgDisk.getGraph(graphNode) == null) return false;
            if (!dsgMem.getGraph(graphNode).isIsomorphicWith(dsgDisk.getGraph(graphNode))) return false;
            seen.add(graphNode);
        }

        while (graphsDisk.hasNext()) {
            Node graphNode = graphsDisk.next();
            if (!seen.contains(graphNode)) {
                if (dsgMem.getGraph(graphNode) == null) return false;
                if (!dsgMem.getGraph(graphNode).isIsomorphicWith(dsgDisk.getGraph(graphNode))) return false;
            }
        }

        return true;
    }

    public static DatasetGraphTDB load(String inputPath) {
        List<String> urls = new ArrayList<String>();
        for (File file : new File(inputPath).listFiles()) {
            if (file.isFile()) {
                urls.add(file.getAbsolutePath());
            }
        }
        DatasetGraphTDB dsg = TDBFactory.createDatasetGraph();
        TDBLoader.load(dsg, urls);

        return dsg;
    }
    
    public static String dump(DatasetGraphTDB dsgMem, DatasetGraphTDB dsgDisk) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        
        if (!dsgMem.getDefaultGraph().isIsomorphicWith(dsgDisk.getDefaultGraph())) {
            sb.append("Default graphs are not isomorphic [FAIL]\n");
            sb.append("    First:\n");
            dump(sb, dsgMem.getDefaultGraph());
            sb.append("    Second:\n");
            dump(sb, dsgDisk.getDefaultGraph());
        } else {
            sb.append("Default graphs are isomorphic [OK]\n");
        }
            
        return sb.toString();
    }
    
    private static void dump (StringBuffer sb, Graph graph) {
        ExtendedIterator<Triple> iter = graph.find(Node.ANY, Node.ANY, Node.ANY);
        while ( iter.hasNext() ) {
            Triple triple = iter.next();
            sb.append(triple).append("\n");
        }
    }
    
}
