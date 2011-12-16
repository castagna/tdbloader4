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

package cmd;

import static org.apache.jena.tdbloader4.Constants.OUTPUT_PATH_POSTFIX_1;
import static org.apache.jena.tdbloader4.Constants.OUTPUT_PATH_POSTFIX_2;
import static org.apache.jena.tdbloader4.Constants.OUTPUT_PATH_POSTFIX_3;
import static org.apache.jena.tdbloader4.Constants.OUTPUT_PATH_POSTFIX_4;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jena.tdbloader4.Constants;
import org.apache.jena.tdbloader4.FirstDriver;
import org.apache.jena.tdbloader4.FourthDriver;
import org.apache.jena.tdbloader4.SecondDriver;
import org.apache.jena.tdbloader4.ThirdDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class tdbloader4 extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(tdbloader4.class);
	
    public tdbloader4 () {
		super();
        log.debug("constructed with no configuration.");
	}

	public tdbloader4 (Configuration configuration) {
		super(configuration);
        log.debug("constructed with configuration.");
	}
    
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration configuration = getConf();
		configuration.set(Constants.RUN_ID, String.valueOf(System.currentTimeMillis()));
        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERRIDE_OUTPUT, Constants.OPTION_OVERRIDE_OUTPUT_DEFAULT);
        boolean copyToLocal = configuration.getBoolean(Constants.OPTION_COPY_TO_LOCAL, Constants.OPTION_COPY_TO_LOCAL_DEFAULT);
        boolean verify = configuration.getBoolean(Constants.OPTION_VERIFY, Constants.OPTION_VERIFY_DEFAULT);
        boolean runLocal = configuration.getBoolean(Constants.OPTION_RUN_LOCAL, Constants.OPTION_RUN_LOCAL_DEFAULT);
        
        FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
            fs.delete(new Path(args[1] + OUTPUT_PATH_POSTFIX_1), true);
            fs.delete(new Path(args[1] + OUTPUT_PATH_POSTFIX_2), true);
            fs.delete(new Path(args[1] + OUTPUT_PATH_POSTFIX_3), true);
            fs.delete(new Path(args[1] + OUTPUT_PATH_POSTFIX_4), true);
        }
        
        if ( ( copyToLocal ) || ( runLocal ) ) {
        	File path = new File(args[1]);
        	path.mkdirs();
        }
		
        Tool first = new FirstDriver(configuration);
        int status = first.run(new String[] { args[0], args[1] + OUTPUT_PATH_POSTFIX_1 });
        if (status != 0){ return status ;}

        createOffsetsFile(fs, args[1] + OUTPUT_PATH_POSTFIX_1, args[1] + OUTPUT_PATH_POSTFIX_1);
        Path offsets = new Path(args[1] + OUTPUT_PATH_POSTFIX_1, Constants.OFFSETS_FILENAME);
        DistributedCache.addCacheFile(offsets.toUri(), configuration);
        
        Tool second = new SecondDriver(configuration);
        status = second.run(new String[] { args[0], args[1] + OUTPUT_PATH_POSTFIX_2 });
        if (status != 0){ return status ;}

        Tool third = new ThirdDriver(configuration);
        status = third.run(new String[] { args[1] + OUTPUT_PATH_POSTFIX_2, args[1] + OUTPUT_PATH_POSTFIX_3 });
        if (status != 0){ return status ;}

        Tool fourth = new FourthDriver(configuration);
        status = fourth.run(new String[] { args[1] + OUTPUT_PATH_POSTFIX_3, args[1] + OUTPUT_PATH_POSTFIX_4 });
        if (status != 0){ return status ;}

        if ( copyToLocal ) {
        	Tool download = new download(configuration);
        	download.run(new String[] { args[1] + OUTPUT_PATH_POSTFIX_2, args[1] + OUTPUT_PATH_POSTFIX_4, args[1] });
        }
        
        if ( verify ) {
            DatasetGraphTDB dsgMem = load(args[0]);
            Location location = new Location(args[1]);
            
            if (!copyToLocal) {
            	// TODO: this is a sort of a cheat and it could go away (if it turns out to be too slow)!
            	download.fixNodeTable2(location);
            }

            DatasetGraphTDB dsgDisk = SetupTDB.buildDataset(location) ;
            boolean isomorphic = isomorphic ( dsgMem, dsgDisk );
            System.out.println ("> " + isomorphic);
        }
        
		return status;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new tdbloader4(), args);
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

        Path outputPath = new Path(output, Constants.OFFSETS_FILENAME);
        PrintWriter out = new PrintWriter(new OutputStreamWriter( fs.create(outputPath)));
        for (Long partition : offsets.keySet()) {
			out.println(partition + "\t" + offsets.get(partition));
		}
        out.close();
        log.debug("Offset file created.");
	}
	
    public static boolean isomorphic(DatasetGraph dsgMem, DatasetGraph dsgDisk) {
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
        DatasetGraphTDB dsg = (DatasetGraphTDB)TDBFactory.createDatasetGraph();
        TDBLoader.load(dsg, urls);

        return dsg;
    }
    
    public static String dump(DatasetGraph dsgMem, DatasetGraph dsgDisk) {
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

        Iterator<Node> graphsMem = dsgMem.listGraphNodes();
        Iterator<Node> graphsDisk = dsgDisk.listGraphNodes();
        Set<Node> seen = new HashSet<Node>();

        while (graphsMem.hasNext()) {
            Node graphNode = graphsMem.next();
            if (dsgDisk.getGraph(graphNode) == null) sb.append(graphNode + " is present only in one dataset. [FAIL]\n");
            if (!dsgMem.getGraph(graphNode).isIsomorphicWith(dsgDisk.getGraph(graphNode))) {
                sb.append("\n" + graphNode + " graphs are not isomorphic [FAIL]\n");
                sb.append("    First:\n");
                dump(sb, dsgMem.getGraph(graphNode));
                sb.append("    Second:\n");
                dump(sb, dsgDisk.getGraph(graphNode));
            }
            seen.add(graphNode);
        }

        while (graphsDisk.hasNext()) {
            Node graphNode = graphsDisk.next();
            if (!seen.contains(graphNode)) {
                if (dsgMem.getGraph(graphNode) == null) sb.append(graphNode + " is present only in one dataset. [FAIL]\n");
                if (!dsgMem.getGraph(graphNode).isIsomorphicWith(dsgDisk.getGraph(graphNode))) {
                    sb.append("\n" + graphNode + " graphs are not isomorphic [FAIL]\n");
                    sb.append("    First:\n");
                    dump(sb, dsgMem.getGraph(graphNode));
                    sb.append("    Second:\n");
                    dump(sb, dsgDisk.getGraph(graphNode));
                }
            }
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
