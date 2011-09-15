package com.talis.labs.tdb.tdbloader3;

import static com.hp.hpl.jena.tdb.lib.NodeLib.setHash;
import static com.hp.hpl.jena.tdb.sys.SystemTDB.LenNodeHash;
import static com.hp.hpl.jena.tdb.sys.SystemTDB.SizeOfNodeId;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.openjena.atlas.lib.Bytes;
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
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.index.RangeIndex;
import com.hp.hpl.jena.tdb.lib.NodeLib;
import com.hp.hpl.jena.tdb.store.Hash;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SetupTDB;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class TestNodeTableBuilding {
	
    private static final Logger log = LoggerFactory.getLogger(TestNodeTableBuilding.class);
	
	public static void fixNodeTable(Location location) {
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
			log.debug(pair + " -> " + Utils.toHex(id) + " -> " + node + " -> " + record);
			nodeToId.add(record);
		}

		nodeToId.sync();
		if ( log.isDebugEnabled() ) {
			Iterator<Record> iterRecord = nodeToId.iterator();
			while ( iterRecord.hasNext() ) {
				log.debug(iterRecord.next().toString());
			}			
		}
		nodeToId.close();
		objects.sync();
		objects.close();		
	}

	public static void dump (Location location, String indexName) {
		System.out.println("--------[ " + indexName + " ]--------");
        FileSet fileset = new FileSet(location, indexName) ;
        RangeIndex rIndex = IndexBuilder.createRangeIndex(fileset, indexName.length()==3?SystemTDB.indexRecordTripleFactory:SystemTDB.indexRecordQuadFactory) ;
        Iterator<Record> iter = rIndex.iterator();
        while ( iter.hasNext() ) {
        	log.debug(iter.next().toString());
        }		
	}
	
	public static void main(String[] args) {
		Location location = new Location("target/output");
		
		fixNodeTable(location);
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
