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

import static com.hp.hpl.jena.sparql.util.Utils.nowAsString;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.jena.tdbloader3.NodeTableBuilder;
import org.openjena.atlas.lib.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.index.RangeIndex;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.store.bulkloader2.ProgressLogger;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class nodetablebuilder {

    private static final Logger log = LoggerFactory.getLogger(nodetablebuilder.class);

	public static void main(String[] args) {
		if ( ( args.length != 1 ) && ( args.length !=2 ) ) { print_usage(); }
		if ( ( args.length == 2 ) && (!"--dump".equals(args[0])) ) { print_usage(); }

		ProgressLogger monitor = new ProgressLogger(log, "Data", BulkLoader.DataTickPoint,BulkLoader.superTick) ;
		Location location = new Location(args.length==1?args[0]:args[1]);
		monitor.start();
		NodeTableBuilder.fixNodeTable(location, monitor);
        long time = monitor.finish() ;

        long total = monitor.getTicks() ;
        float elapsedSecs = time/1000F ;
        float rate = (elapsedSecs!=0) ? total/elapsedSecs : 0 ;
        String str =  String.format("Total: %,d RDF nodes : %,.2f seconds : %,.2f nodes/sec [%s]", total, elapsedSecs, rate, nowAsString()) ;
        log.info(str);
		
		if ( "--dump".equals(args[0]) ) {
			dump(location, "SPO");
			dump(location, "GSPO");
			
			Dataset ds = TDBFactory.createDataset(location);
			Model model = ds.getDefaultModel();
			StmtIterator stmtIter = model.listStatements();
			while ( stmtIter.hasNext() ) {
				System.out.println(stmtIter.next());
			}			
		}

	}
	
	private static void print_usage() {
		System.out.println("Usage: nodetablebuilder [--dump] <path>");
		System.exit(0);		
	}
	
	private static void dump (Location location, String indexName) {
		System.out.println("--------[ " + indexName + " ]--------");
        FileSet fileset = new FileSet(location, indexName) ;
        RangeIndex rIndex = IndexBuilder.createRangeIndex(fileset, indexName.length()==3?SystemTDB.indexRecordTripleFactory:SystemTDB.indexRecordQuadFactory) ;
        Iterator<Record> iter = rIndex.iterator();
        while ( iter.hasNext() ) {
        	log.debug(iter.next().toString());
        }		
	}
	
	@SuppressWarnings("unused")
	private static void dumpObject (Location location) {
		log.debug("{}", location);
		String path = location.getDirectoryPath() ;
		ObjectFile objects = FileFactory.createObjectFileDisk(path + File.separator + "nodes.dat");
		Iterator<Pair<Long,ByteBuffer>> iter = objects.all();
		while ( iter.hasNext() ) {
			Pair<Long, ByteBuffer> pair = iter.next();
			long id = pair.getLeft() ;
			Node node = NodeLib.fetchDecode(id, objects) ;
			log.debug("{} : {}", id, node);
		}
	}

}
