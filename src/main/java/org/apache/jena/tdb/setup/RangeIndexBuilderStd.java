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

package org.apache.jena.tdb.setup;

import org.slf4j.Logger;

import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBException;
import com.hp.hpl.jena.tdb.base.block.BlockMgr;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.MetaFile;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.index.RangeIndex;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTree;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTreeParams;
import com.hp.hpl.jena.tdb.sys.Names;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class RangeIndexBuilderStd implements RangeIndexBuilder
{
    private static final Logger log = TDB.logInfo ;
    
    private BlockMgrBuilder bMgr1 ;
    private BlockMgrBuilder bMgr2 ;
    public RangeIndexBuilderStd(BlockMgrBuilder bMgr1, BlockMgrBuilder bMgr2)
    {
        this.bMgr1 = bMgr1 ;
        this.bMgr2 = bMgr2 ;
    }

    public RangeIndex buildRangeIndex(FileSet fileSet, RecordFactory recordFactory)
    {
        // ---- BPlusTree based range index.
        // Get parameters.
        /*
         * tdb.bplustree.record=24,0
         * tdb.bplustree.blksize=
         * tdb.bplustree.order=
         */

        // TODO Respect tdb.bplustree.record=24,0 (and so don't need RecordFactory argument).
        // No - Node table resorc size if different.
        
        MetaFile metafile = fileSet.getMetaFile() ;
        //RecordFactory recordFactory = new RecordFactory(keyLength, valueLength) ; // makeRecordFactory(metafile, "tdb.bplustree.record", dftKeyLength, dftValueLength) ;

        String blkSizeStr = metafile.getOrSetDefault("tdb.bplustree.blksize", Integer.toString(SystemTDB.BlockSize)) ; 
        int blkSize = parseInt(blkSizeStr, "Bad block size") ;

        // IndexBuilder.getBPlusTree().newRangeIndex(fs, recordFactory) ;
        // Does not set order.

        int calcOrder = BPlusTreeParams.calcOrder(blkSize, recordFactory.recordLength()) ;
        String orderStr = metafile.getOrSetDefault("tdb.bplustree.order", Integer.toString(calcOrder)) ;
        int order = parseInt(orderStr, "Bad order for B+Tree") ;
        if ( order != calcOrder )
            error(log, "Wrong order (" + order + "), calculated = "+calcOrder) ;

        //        int readCacheSize = PropertyUtils.getPropertyAsInteger(config, Names.pBlockReadCacheSize) ;
        //        int writeCacheSize = PropertyUtils.getPropertyAsInteger(config, Names.pBlockWriteCacheSize) ;

        int readCacheSize = SystemTDB.BlockReadCacheSize ;
        int writeCacheSize = SystemTDB.BlockWriteCacheSize ;

        RangeIndex rIndex = createBPTree(fileSet, order, blkSize, readCacheSize, writeCacheSize, bMgr1, bMgr2, recordFactory) ;
        
        metafile.flush() ;
        return rIndex ;
    }
    
    /** Knowing all the parameters, create a B+Tree */
    private RangeIndex createBPTree(FileSet fileset, int order, 
                                    int blockSize,
                                    int readCacheSize, int writeCacheSize,
                                    BlockMgrBuilder blockMgrBuilder1,
                                    BlockMgrBuilder blockMgrBuilder2,
                                    RecordFactory factory)
    {
        // ---- Checking
        if (blockSize < 0 && order < 0) throw new IllegalArgumentException("Neither blocksize nor order specified") ;
        if (blockSize >= 0 && order < 0) order = BPlusTreeParams.calcOrder(blockSize, factory.recordLength()) ;
        if (blockSize >= 0 && order >= 0)
        {
            int order2 = BPlusTreeParams.calcOrder(blockSize, factory.recordLength()) ;
            if (order != order2) throw new IllegalArgumentException("Wrong order (" + order + "), calculated = "
                                                                    + order2) ;
        }
    
        // Iffy - does not allow for slop.
        if (blockSize < 0 && order >= 0)
        {
            // Only in-memory.
            blockSize = BPlusTreeParams.calcBlockSize(order, factory) ;
        }
    
        BPlusTreeParams params = new BPlusTreeParams(order, factory) ;
        
//        BlockMgr blkMgrNodes = BlockMgrFactory.create(fileset, Names.bptExt1, blockSize, readCacheSize, writeCacheSize) ;
//        BlockMgr blkMgrRecords = BlockMgrFactory.create(fileset, Names.bptExt2, blockSize, readCacheSize, writeCacheSize) ;
        BlockMgr blkMgrNodes = blockMgrBuilder1.buildBlockMgr(fileset, Names.bptExt1, blockSize) ;
        BlockMgr blkMgrRecords = blockMgrBuilder2.buildBlockMgr(fileset, Names.bptExt2, blockSize) ;
        
        return BPlusTree.attach(params, blkMgrNodes, blkMgrRecords) ;
    }

    private static void error(Logger log, String msg)
    {
        if ( log != null )
            log.error(msg) ;
        throw new TDBException(msg) ;
    }
    
    private static int parseInt(String str, String messageBase)
    {
        try { return Integer.parseInt(str) ; }
        catch (NumberFormatException ex) { error(log, messageBase+": "+str) ; return -1 ; }
    }
}