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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.labs.tdb.tdbloader3.NodeTableBuilder;

public class tdbloader3 extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(tdbloader3.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration configuration = getConf();
		configuration.set("runId", String.valueOf(System.currentTimeMillis()));
        boolean overrideOutput = configuration.getBoolean("overrideOutput", false);
        boolean copyToLocal = configuration.getBoolean("copyToLocal", true);
        boolean verify = configuration.getBoolean("verify", false);
        
        FileSystem fs = FileSystem.get(configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
            fs.delete(new Path(args[1] + "_1"), true);
            fs.delete(new Path(args[1] + "_2"), true);
            fs.delete(new Path(args[1] + "_3"), true);
            fs.delete(new Path(args[1] + "_4"), true);
        }
        
        if ( copyToLocal ) {
        	File path = new File(args[1]);
        	path.mkdirs();
        }
		
        Tool first = new FirstDriver(configuration);
        first.run(new String[] { args[0], args[1] + "_1" });

        createOffsetsFile(fs, args[1] + "_1", args[1] + "_1");
        Path offsets = new Path(args[1] + "_1", "offsets.txt");
        DistributedCache.addCacheFile(offsets.toUri(), configuration);
        
        Tool second = new SecondDriver(configuration);
        second.run(new String[] { args[0], args[1] + "_2" });

        Tool third = new ThirdDriver(configuration);
        third.run(new String[] { args[1] + "_2", args[1] + "_3" });
        
        Tool fourth = new FourthDriver(configuration);
        fourth.run(new String[] { args[1] + "_3", args[1] + "_4" });
        
        if ( copyToLocal ) {
            Location location = new Location(args[1]);
            DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
            dsgDisk.sync(); 
            dsgDisk.close();

            mergeToLocalFile(fs, new Path(args[1] + "_2"), args[1], configuration);
            copyToLocalFile(fs, new Path(args[1] + "_4"), new Path(args[1]));
            
            // TODO: this is a sort of a cheat and it could go away (if it turns out to be too slow)!
            NodeTableBuilder.fixNodeTable(location);            	
        }
        
        if ( verify ) {
            DatasetGraphTDB dsgMem = load(args[0]);
            Location location = new Location(args[1]);
            
            if (!copyToLocal) {
            	// TODO: this is a sort of a cheat and it could go away (if it turns out to be too slow)!
            	NodeTableBuilder.fixNodeTable(location);
            }

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
	
	private void createOffsetsFile(FileSystem fs, String input, String output) throws IOException {
		log.debug("Creating offsets file...");
        Map<Long, Long> offsets = new TreeMap<Long, Long>();
        FileStatus[] status = fs.listStatus(new Path(input));
        for (FileStatus fileStatus : status) {
        	Path file = fileStatus.getPath();
        	if ( file.getName().startsWith("part-r-") ) {
        		log.debug("Processing: {}", file.getName());
       			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(file)));
       			String line = in.readLine();
       			String[] tokens = line.split("\\s");
       			long partition = Long.valueOf(tokens[0]); 
               	long offset = Long.valueOf(tokens[1]);
               	log.debug("Partition {} has offset {}", partition, offset);
               	offsets.put(partition, offset);
        	}
		}

        Path outputPath = new Path(output, "offsets.txt");
        PrintWriter out = new PrintWriter(new OutputStreamWriter( fs.create(outputPath)));
        for (Long partition : offsets.keySet()) {
			out.println(partition + "\t" + offsets.get(partition));
		}
        out.close();
        log.debug("Offset file created.");
	}
	
	private void copyToLocalFile ( FileSystem fs, Path src, Path dst ) throws FileNotFoundException, IOException {
		FileStatus[] status = fs.listStatus(src);
		for ( FileStatus fileStatus : status ) {
            Path path = fileStatus.getPath();
            String pathName = path.getName();
            if ( pathName.startsWith("first_") || pathName.startsWith("third_") ) {
            	copyToLocalFile(fs, path, dst);
            }
            if ( pathName.endsWith(".idn") || pathName.endsWith(".dat") ) {
                fs.copyToLocalFile(path, new Path(dst, path.getName()));
            }
        }
	}

	private void mergeToLocalFile ( FileSystem fs, Path src, String outPath, Configuration configuration ) throws FileNotFoundException, IOException {
		FileStatus[] status = fs.listStatus(src);
		Map<String, Path> paths = new TreeMap<String, Path>();
		for ( FileStatus fileStatus : status ) {
            Path path = fileStatus.getPath();
            String pathName = path.getName();
            if ( pathName.startsWith("second-alternative_") ) {
            	paths.put(pathName, path);
            }
        }

		File outFile = new File(outPath, Names.indexId2Node + ".dat");
        OutputStream out = new FileOutputStream(outFile);
        for (String pathName : paths.keySet()) {
        	Path path = new Path(src, paths.get(pathName));
        	log.debug("Concatenating {} into {}...", path.toUri(), outFile.getAbsoluteFile());
        	InputStream in = fs.open(new Path(path, Names.indexId2Node + ".dat"));
        	IOUtils.copyBytes(in, out, configuration, false);
        	in.close();			
		}
		out.close();
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
