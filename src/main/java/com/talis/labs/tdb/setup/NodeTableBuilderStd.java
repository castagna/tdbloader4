package com.talis.labs.tdb.setup;

import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.nodetable.NodeTableCache;
import com.hp.hpl.jena.tdb.nodetable.NodeTableInline;
import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class NodeTableBuilderStd implements NodeTableBuilder
{
    private final IndexBuilder indexBuilder ;
    private final ObjectFileBuilder objectFileBuilder ;
    
    public NodeTableBuilderStd(IndexBuilder indexBuilder, ObjectFileBuilder objectFileBuilder)
    { 
        this.indexBuilder = indexBuilder ;
        this.objectFileBuilder = objectFileBuilder ;
    }
    
    public NodeTable buildNodeTable(FileSet fsIndex, FileSet fsObjectFile, int sizeNode2NodeIdCache, int sizeNodeId2NodeCache)
    {
        RecordFactory recordFactory = new RecordFactory(SystemTDB.LenNodeHash, SystemTDB.SizeOfNodeId) ;
        Index idx = indexBuilder.buildIndex(fsIndex, recordFactory) ;
        ObjectFile objectFile = objectFileBuilder.buildObjectFile(fsObjectFile, Names.extNodeData) ;
        NodeTable nodeTable = new NodeTableNative(idx, objectFile) ;
        nodeTable = NodeTableCache.create(nodeTable, sizeNode2NodeIdCache, sizeNodeId2NodeCache) ;
        nodeTable = NodeTableInline.create(nodeTable) ;
        return nodeTable ;
    }
}