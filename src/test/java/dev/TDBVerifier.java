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

package dev;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.openjena.atlas.lib.ColumnMap;
import org.openjena.atlas.lib.Pair;
import org.openjena.atlas.lib.Tuple;

import com.hp.hpl.jena.tdb.base.StorageException;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.lib.TupleLib;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class TDBVerifier {

	public static void main(String[] args) {
		if (args.length != 1) {
			print_usage();
		}

		String path = args[0];
		Location location = new Location(path);

		Set<Long> nodeIds = verifyNodeTable(location);

		for (String indexName : Names.tripleIndexes) {
			verifyIndex (location, indexName, nodeIds);
		}
		
		for (String indexName : Names.quadIndexes) {
			verifyIndex (location, indexName, nodeIds);
		}

	}
	
	private static Set<Long> verifyNodeTable(final Location location) {
		System.out.println ("---- Scanning node table ... ----");
		Set<Long> nodeIds = new HashSet<Long>();
		String filename = location.absolute(Names.indexId2Node, Names.extNodeData);
		ObjectFile objects = FileFactory.createObjectFileDisk(filename);
		Iterator<Pair<Long, ByteBuffer>> iter = objects.all();
		int countRdfNodes = 0;
		Long nodeId = 0L;
		ByteBuffer nodeValue = null;
		while (iter.hasNext()) {
			Pair<Long, ByteBuffer> entry = iter.next();
			nodeId = entry.getLeft();
			nodeValue = entry.getRight();
			nodeIds.add(nodeId);
			countRdfNodes++;
			System.out.println(nodeId + ": " + nodeValue);
		}
		System.out.println("Found " + countRdfNodes + " RDF nodes.");
		System.out.println("Nodes.dat file size is: " + new File(filename).length());
		long filesize = nodeId + nodeValue.capacity() + SystemTDB.SizeOfInt;
		System.out.println(nodeId + " + " + nodeValue.capacity() + " + " + SystemTDB.SizeOfInt + " = " + filesize);
		
		if ( new File(filename).length() != filesize ) {
			error("The nodes.dat has a problem!");
		}
		
		return nodeIds;
	}

	private static void verifyIndex(final Location location, final String indexName, final Set<Long> nodeIds) {
		System.out.println ("---- Scanning " + indexName + " ... ----");
		
		try {
			RecordFactory quadRecordFactory = new RecordFactory(SystemTDB.LenIndexQuadRecord, 0);
			Index index = IndexBuilder.createIndex(new FileSet(location, indexName), quadRecordFactory);
			Iterator<Record> iter = index.iterator();
			int countRecords = 0;

			int tupleLength = indexName.length();
	        String primaryOrder = null;
	        if ( tupleLength == 3 ) {
	            primaryOrder = Names.primaryIndexTriples;
	        } else if ( tupleLength == 4 ) {
	            primaryOrder = Names.primaryIndexQuads;
	        }
	        ColumnMap colMap = new ColumnMap(primaryOrder, indexName) ;
			
			while (iter.hasNext()) {
				Record record = iter.next();
				System.out.println(record);
				Tuple<NodeId> tuple = TupleLib.tuple(record, colMap);

				for (NodeId nodeId : tuple.asList()) {
					if ( ! nodeIds.contains(nodeId) ) {
						System.out.println("NodeId " + nodeId.getId() + " is not in the node table!"); 
					}
					// assertTrue(nodeIds.contains(nodeId));		
				}

				countRecords++;
			}
			System.out.println("Found " + countRecords + " records.");

		} catch (StorageException e) {
			System.out.println("------------------------------------------------------------------------------");
			e.printStackTrace(System.out);
			System.out.println("------------------------------------------------------------------------------");
		}


	}

	private static void error(String msg) {
		System.out.println("------------------------------------------------------------------------------");
		System.out.println(msg);
		System.out.println("------------------------------------------------------------------------------");
	}
	
	private static void print_usage() {
		System.out.println("Usage: TDBVerifier <path>");
		System.exit(0);		
	}
	
}
