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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;

import org.openjena.atlas.AtlasException;
import org.openjena.atlas.data.SerializationFactory;
import org.openjena.atlas.data.SortedDataBag;
import org.openjena.atlas.data.ThresholdPolicyCount;
import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.iterator.Transform;
import org.openjena.atlas.lib.Bytes;
import org.openjena.atlas.lib.Closeable;
import org.openjena.atlas.lib.Pair;
import org.openjena.atlas.lib.Sink;
import org.slf4j.Logger;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.tdb.base.block.BlockMgr;
import com.hp.hpl.jena.tdb.base.block.BlockMgrFactory;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTree;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTreeParams;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTreeRewriter;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.Hash;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.store.bulkloader2.ProgressLogger;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class NodeTableRewriter {
	
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
	
	public static void fixNodeTable2(Location location, Logger log, ProgressLogger monitor) throws IOException {
		String path = location.getDirectoryPath() ;
		new File(path, "node2id.dat").delete() ;
		new File(path, "node2id.idn").delete() ;
		
		// input
		ObjectFile objects = FileFactory.createObjectFileDisk(path + File.separator + "nodes.dat");
		// sorted bag
	    ThresholdPolicyCount<Pair<byte[],Long>> policy = new ThresholdPolicyCount<Pair<byte[],Long>>(10000000);
	    Comparator<Pair<byte[],Long>> comparator = new PairComparator();
		SortedDataBag<Pair<byte[],Long>> sortedDataBag = new SortedDataBag<Pair<byte[],Long>>(policy, new PairSerializationFactory(), comparator);
		
		Iterator<Pair<Long,ByteBuffer>> iter = objects.all();
		while ( iter.hasNext() ) {
			Pair<Long, ByteBuffer> pair = iter.next();
			long id = pair.getLeft();
			Node node = NodeLib.fetchDecode(id, objects);
	        Hash hash = new Hash(SystemTDB.LenNodeHash);
	        setHash(hash, node);
	        byte k[] = hash.getBytes();
	        sortedDataBag.send(new Pair<byte[],Long>(k, id));
			if ( monitor != null ) monitor.tick();
		}
		objects.sync();
		objects.close();		
		
        // output
		final ProgressLogger monitor2 = new ProgressLogger(log, "Data (2/2)", BulkLoader.DataTickPoint,BulkLoader.superTick);
		log.info("Data (2/2)...");
		monitor2.start();

		final RecordFactory recordFactory = new RecordFactory(LenNodeHash, SizeOfNodeId) ;
		Transform<Pair<byte[],Long>, Record> transformPair2Record = new Transform<Pair<byte[],Long>, Record>() {
		    @Override public Record convert(Pair<byte[],Long> pair) {
		        monitor2.tick();
		        return recordFactory.create(pair.getLeft(), Bytes.packLong(pair.getRight()));
		    }
		};

		int order = BPlusTreeParams.calcOrder(SystemTDB.BlockSize, recordFactory) ;
		BPlusTreeParams bptParams = new BPlusTreeParams(order, recordFactory) ;
		int readCacheSize = 10 ;
		int writeCacheSize = 100 ;
		FileSet destination = new FileSet(location, Names.indexNode2Id) ;
		BlockMgr blkMgrNodes = BlockMgrFactory.create(destination, Names.bptExt1, SystemTDB.BlockSize, readCacheSize, writeCacheSize) ;
		BlockMgr blkMgrRecords = BlockMgrFactory.create(destination, Names.bptExt2, SystemTDB.BlockSize, readCacheSize, writeCacheSize) ;
		Iterator<Record> iter2 = Iter.iter(sortedDataBag.iterator()).map(transformPair2Record) ;
	    BPlusTree bpt2 = BPlusTreeRewriter.packIntoBPlusTree(iter2, bptParams, recordFactory, blkMgrNodes, blkMgrRecords) ;
	    bpt2.sync() ;
		
		sortedDataBag.close();
	}
	
    static class PairSerializationFactory implements SerializationFactory<Pair<byte[],Long>> {
        @Override public Iterator<Pair<byte[],Long>> createDeserializer(InputStream in) { return new PairInputStream(in); }
        @Override public Sink<Pair<byte[],Long>> createSerializer(OutputStream out) { return new PairOutputStream(out); }
        @Override public long getEstimatedMemorySize(Pair<byte[],Long> item) { throw new AtlasException("Method not implemented.") ; }
    }

    static class PairComparator implements Comparator<Pair<byte[],Long>> {
        @Override
        public int compare(Pair<byte[],Long> p1, Pair<byte[],Long> p2) {
            int size = p1.getLeft().length;
            if ( size != p2.getLeft().length ) throw new AtlasException("Cannot compare pairs with hash values of different sizes.") ;
            return Bytes.compare(p1.getLeft(), p2.getLeft()) ;
        }
    }
    
    static class PairInputStream implements Iterator<Pair<byte[],Long>>, Closeable {

        private DataInputStream in ;
        private Pair<byte[],Long> slot = null ;
        
        public PairInputStream(InputStream in) {
            this.in = new DataInputStream(new BufferedInputStream(in)) ;
            slot = readNext() ;
        }

        @Override
        public boolean hasNext() {
            return slot != null ;
        }

        @Override
        public Pair<byte[],Long> next() {
        	Pair<byte[],Long> result = slot ;
            slot = readNext() ;
            return result ;
        }
        
        private Pair<byte[],Long> readNext() {
            try {
            	int len = in.readInt() ;
            	byte hash[] = new byte[len] ;
            	in.readFully(hash) ;
            	long id = in.readLong() ;
            	return new Pair<byte[],Long>(hash, id) ;
            } catch (IOException e) {
                return null ;
            }
        }

        @Override
        public void remove() {
            throw new AtlasException("Method not implemented.") ;
        }

        @Override
        public void close() {
            try {
                in.close() ;
            } catch (IOException e) {
                new AtlasException(e) ;
            }        
        }
        
    }
    
    static class PairOutputStream implements Sink<Pair<byte[],Long>> {

        private DataOutputStream out ;
        
        public PairOutputStream(OutputStream out) {
            this.out = new DataOutputStream(new BufferedOutputStream(out)) ;
        }

        @Override
        public void send(Pair<byte[],Long> pair) {
        	try {
            	byte hash[] = pair.getLeft() ;
            	long id = pair.getRight() ;
            	out.writeInt(hash.length) ;
				out.write(hash) ;
	        	out.writeLong(id) ;
			} catch (IOException e) {
				new AtlasException(e) ;
			}
        }

        @Override
        public void flush() {
            try {
                out.flush() ;
            } catch (IOException e) {
                new AtlasException(e) ;
            }
        }

        @Override
        public void close() {
            try {
                out.close() ;
            } catch (IOException e) {
                new AtlasException(e) ;
            }
        }
        
    }
    
}
