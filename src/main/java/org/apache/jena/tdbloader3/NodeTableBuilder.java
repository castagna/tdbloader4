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

import static com.hp.hpl.jena.tdb.lib.NodeLib.setHash;
import static com.hp.hpl.jena.tdb.sys.SystemTDB.LenNodeHash;
import static com.hp.hpl.jena.tdb.sys.SystemTDB.SizeOfNodeId;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.openjena.atlas.lib.Bytes;
import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.Hash;
import com.hp.hpl.jena.tdb.store.bulkloader2.ProgressLogger;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;

public class NodeTableBuilder {
	
	public static void fixNodeTable(Location location) {
		fixNodeTable(location, null);
	}

	public static void fixNodeTable(Location location, ProgressLogger monitor) {
		String path = location.getDirectoryPath() ;
		new File(path, "node2id.dat").delete() ;
		new File(path, "node2id.idn").delete() ;
		
		ObjectFile objects = FileFactory.createObjectFileDisk(path + File.separator + "nodes.dat");
		Index nodeToId = SetupTDB.makeIndex(location, Names.indexNode2Id, LenNodeHash, SizeOfNodeId, -1 ,-1) ;
		RecordFactory recordFactory = nodeToId.getRecordFactory();


		Iterator<Pair<Long,ByteBuffer>> iter = objects.all();
		while ( iter.hasNext() ) {
			Pair<Long, ByteBuffer> pair = iter.next();
			long id = pair.getLeft() ;
			Node node = NodeLib.fetchDecode(id, objects) ;
	        Hash hash = new Hash(recordFactory.keyLength()) ;
	        setHash(hash, node) ;
	        byte k[] = hash.getBytes() ;        
	        Record record = recordFactory.create(k) ;
	        Bytes.setLong(id, record.getValue(), 0) ;
			nodeToId.add(record);
			if ( monitor != null ) monitor.tick();
		}

		nodeToId.sync();
		nodeToId.close();
		objects.sync();
		objects.close();		
	}

}
