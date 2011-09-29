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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class TDBMerge {

	public static void main(String[] args) {
		Location location1 = new Location("target/out-01") ;
		DatasetGraphTDB dsg1 = TDBFactory.createDatasetGraph(location1) ;
		TDBLoader.load(dsg1, "src/test/resources/input/data.nq") ;
		dsg1.close() ;

		Location location2 = new Location("target/out-02") ;
		DatasetGraphTDB dsg2 = TDBFactory.createDatasetGraph(location2) ;
		TDBLoader.load(dsg2, "src/test/resources/input/data.nt") ;
		dsg2.close() ;

		
		RecordFactory tripleRecordFactory = new RecordFactory(SystemTDB.LenIndexTripleRecord, 0) ;
		RecordFactory quadRecordFactory = new RecordFactory(SystemTDB.LenIndexQuadRecord, 0) ;
		RecordFactory nodeTableRecordFactory = new RecordFactory(SystemTDB.LenNodeHash, SystemTDB.SizeOfNodeId) ;

		Index n2id1 = IndexBuilder.createIndex(new FileSet(location1, Names.indexNode2Id), nodeTableRecordFactory) ;
		Index n2id2 = IndexBuilder.createIndex(new FileSet(location2, Names.indexNode2Id), nodeTableRecordFactory) ;

		System.out.println("Node table 01: node2id index") ;
		Iterator<Record> iter1 = n2id1.iterator() ;
		while ( iter1.hasNext() ) {
			System.out.println(iter1.next()) ;
		}

		System.out.println("Node table 02: node2id index") ;
		Iterator<Record> iter2 = n2id2.iterator() ;
		while ( iter2.hasNext() ) {
			System.out.println(iter2.next()) ;
		}
		
		System.out.println("Node table: how do we merge the node2id indexes?") ;
		List<Iterator<Record>> indexes = new ArrayList<Iterator<Record>>();
		indexes.add(n2id1.iterator()) ;
		indexes.add(n2id2.iterator()) ;
		MergeSortIterator<Record> merger = new MergeSortIterator<Record>(indexes, new RecordComparator()) ;
		while ( merger.hasNext() ) {
			System.out.println(merger.next());
		}

		ObjectFile of1 = FileFactory.createObjectFileDisk("target/out-01/nodes.dat") ;		
		ObjectFile of2 = FileFactory.createObjectFileDisk("target/out-02/nodes.dat") ;

		System.out.println("Node table 01: nodes.dat (i.e. object file)") ;
		Iterator<Pair<Long, ByteBuffer>> it1 = of1.all() ;
		while ( it1.hasNext() ) {
			Pair<Long, ByteBuffer> pair = it1.next() ;
			System.out.println(pair.getLeft() + ":" + NodeLib.decode(pair.getRight())); 
		}
		
		System.out.println("Node table 02: nodes.dat (i.e. object file)") ;
		Iterator<Pair<Long, ByteBuffer>> it2 = of2.all() ;
		while ( it2.hasNext() ) {
			Pair<Long, ByteBuffer> pair = it2.next() ;
			System.out.println(pair.getLeft() + ":" + NodeLib.decode(pair.getRight())); 
		}
		
		
//		Iterator<Record> iter1 = n2id1.iterator() ;
//		while ( iter1.hasNext() ) {
//			System.out.println(iter1.next()) ;
//		}
//
//		Index index4 = IndexBuilder.createIndex(new FileSet(location1, Names.primaryIndexQuads) , quadRecordFactory) ;
//		Iterator<Record> iter4 = index4.iterator() ;
//		while ( iter4.hasNext() ) {
//			System.out.println(iter4.next()) ;
//		}
//		
//		Index index3 = IndexBuilder.createIndex(new FileSet(location1, Names.primaryIndexTriples) , tripleRecordFactory) ;
//		Iterator<Record> iter3 = index3.iterator() ;
//		while ( iter3.hasNext() ) {
//			System.out.println(iter3.next()) ;
//		}
//		
//		Iterator<Pair<Long, ByteBuffer>> iter = of1.all() ;
//		while ( iter.hasNext() ) {
//			System.out.println(iter.next()) ;
//		}

//		System.out.println(dsg1) ;
//		System.out.println(dsg2) ;

	}

}
