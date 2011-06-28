package com.talis.labs.tdb.setup;

import com.hp.hpl.jena.tdb.base.block.BlockMgr;
import com.hp.hpl.jena.tdb.base.block.BlockMgrFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class BlockMgrBuilderStd implements BlockMgrBuilder
{
    public BlockMgrBuilderStd() {}

    public BlockMgr buildBlockMgr(FileSet fileset, String ext, int blockSize)
    {
        //int readCacheSize = PropertyUtils.getPropertyAsInteger(config, Names.pBlockReadCacheSize) ;
        //int writeCacheSize = PropertyUtils.getPropertyAsInteger(config, Names.pBlockWriteCacheSize) ;
        
        int readCacheSize = SystemTDB.BlockReadCacheSize ;
        int writeCacheSize = SystemTDB.BlockWriteCacheSize ;
        
        BlockMgr mgr = BlockMgrFactory.create(fileset, ext, blockSize, readCacheSize, writeCacheSize) ;
        return mgr ;
    }
    
}
