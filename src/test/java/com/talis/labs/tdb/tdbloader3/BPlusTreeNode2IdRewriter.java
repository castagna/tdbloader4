/*
 * Copyright 2010,2011 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.talis.labs.tdb.tdbloader3;

import java.util.Iterator;

import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.iterator.IteratorWithBuffer;
import org.openjena.atlas.iterator.Transform;
import org.openjena.atlas.lib.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.tdb.base.block.BlockMgr;
import com.hp.hpl.jena.tdb.base.buffer.PtrBuffer;
import com.hp.hpl.jena.tdb.base.buffer.RecordBuffer;
import com.hp.hpl.jena.tdb.base.record.Record;
import com.hp.hpl.jena.tdb.base.record.RecordFactory;
import com.hp.hpl.jena.tdb.base.recordfile.RecordBufferPage;
import com.hp.hpl.jena.tdb.base.recordfile.RecordBufferPageMgr;
import com.hp.hpl.jena.tdb.index.bplustree.BPTreeException;
import com.hp.hpl.jena.tdb.index.bplustree.BPTreeNode;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTree;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTreeParams;
import com.hp.hpl.jena.tdb.index.bplustree.BPlusTreeRewriter;

public class BPlusTreeNode2IdRewriter
{
//    static private Logger log = LoggerFactory.getLogger(BPlusTreeRewriter.class) ; 
//
//    static boolean rebalance = true ;
//    static boolean debug = false ;
//    static boolean materialize = debug ;
//    
//    private final BPlusTreeParams bptParams ;
//    private final RecordFactory recordFactory ;
//    private final BlockMgr blkMgrNodes ;
//    private final BlockMgr blkMgrRecords ;
//    
//    private boolean no_records = true ;
//    
//    private BPlusTree bpt2 ;
//    
//    public BPlusTreeNode2IdRewriter(BPlusTreeParams bptParams, RecordFactory recordFactory, BlockMgr blkMgrNodes, BlockMgr blkMgrRecords) 
//    {
//    	this.bptParams = bptParams ;
//    	this.recordFactory = recordFactory ;
//    	this.blkMgrNodes = blkMgrNodes ;
//    	this.blkMgrRecords = blkMgrRecords ;
//
//    	// Dummy B+tree needed to carry parameters around.
//        bpt2 = BPlusTree.dummy(bptParams, blkMgrNodes, blkMgrRecords) ;
//
//        // Allocate and format a root index block.
//        // We will use this slot later and write in the correct root.
//        /// The root has to block zero currently.
//
//        BPTreeNode root = bpt2.getNodeManager().createNode(BPlusTreeParams.RootParent) ;
//        int rootId = root.getId() ;
//        if ( rootId != 0 )
//        {
//            log.error("**** Not the root: "+rootId) ;
//            throw new BPTreeException() ;
//        }
//    
//
//    }
//    
//    /** 
//     * Given a stream of records and details of the B+Tree to build, go and build it. 
//     * 
//     * @return A newly built BPlusTree
//     * @see BPlusTreeRewriter written by Andy Seaborne in TDB
//     */ 
//    public BPlusTree packIntoBPlusTree(Record record)
//    {
//    	no_records = false ;
//
//    
//        // ******** Pack data blocks.
//        Iterator<Pair<Integer, Record>> iter = writePackedDataBlocks(iterRecords, bpt2) ;
//    
//    }
//
//    public BPlusTree getBPlusTree() 
//    {
//    	if ( no_records )
//    	{
//    		return BPlusTree.attach(bptParams, blkMgrNodes, blkMgrRecords) ;
//    	}
//    	else
//    	{
//            // ******** Index layer
//            // Loop until one block only.
//            // Never zero blocks.
//            // Output is a single pair pointing to the root - but the root is in the wrong place.
//        
//            boolean leafLayer= true ;
//            while(true)
//            {
//                iter = genTreeLevel(iter, bpt2, leafLayer) ;
//                // Advances iter.
//                IteratorWithBuffer<Pair<Integer, Record>> iter2 = new IteratorWithBuffer<Pair<Integer, Record>>(iter, 2) ;
//                boolean singleBlock = ( iter2.peek(1) == null ) ;
//                // Having peeked ahead, use the real stream.
//                iter = iter2 ;
//                if ( singleBlock )
//                    break  ;
//                leafLayer = false ;
//            }
//
//            // ******** Put root in right place.
//            Pair<Integer, Record> pair = iter.next() ;
//            if ( iter.hasNext() )
//            {
//                log.error("**** Building index layers didn't result in a single block") ;
//                return null ;
//            }
//            fixupRoot(root, pair, bpt2) ;
//
//            // ****** Finish the tree.
//            blkMgrNodes.sync() ;
//            blkMgrRecords.sync() ;
//            // Force root reset.
//            bpt2 = BPlusTree.attach(bptParams, blkMgrNodes, blkMgrRecords) ;
//            return bpt2 ;
//    	}
//
//    }
//    
//    // **** data block phase
//
//    /** Pack record blocks into linked RecordBufferPages */
//    private Iterator<Pair<Integer, Record>> writePackedDataBlocks()
//    {
//        RecordBufferPageMgr mgr = bpt2.getRecordsMgr().getRecordBufferPageMgr() ;
//        Iterator<RecordBufferPage> iter = new RecordBufferPageLinker(new RecordBufferPagePacker(records, mgr)) ;
//
//        Transform<RecordBufferPage, Pair<Integer, Record>> transform = new Transform<RecordBufferPage, Pair<Integer, Record>>()
//        {
//            //@Override
//            public Pair<Integer, Record> convert(RecordBufferPage rbp)
//            {
//                RecordBufferPageMgr mgr = rbp.getPageMgr() ;
//                
//                rbp.getPageMgr().put(rbp) ;
//                Record r = rbp.getRecordBuffer().getHigh() ;
//                r = bpt2.getRecordFactory().createKeyOnly(r) ;
//                return new Pair<Integer, Record>(rbp.getId(), r) ;
//            }
//        } ;
//        // Write and convert to split pairs.
//        Iterator<Pair<Integer, Record>> iter2 = Iter.map(iter, transform) ;
//
//        if ( rebalance )
//            iter2 = new RebalenceDataEnd(iter2, bpt2) ;
//
//        // Testing - materialize - debug wil have done this
//        if ( materialize && ! debug )
//            iter2 = Iter.toList(iter2).iterator() ;
//
//        if ( debug && rebalance )
//        {
//            System.out.println("After rebalance (data)") ;
//            iter2 = summarizeDataBlocks(iter2, bpt2.getRecordsMgr().getRecordBufferPageMgr()) ;
//            //iter2 = printDataBlocks(iter2, bpt.getRecordsMgr().getRecordBufferPageMgr()) ;
//        }
//        return iter2 ;
//    }
//    
//    private static class RebalenceDataEnd extends RebalenceBase
//    {
//        private RecordBufferPageMgr mgr ;
//        private BPlusTree bpt ;
//
//        public RebalenceDataEnd(Iterator<Pair<Integer, Record>> iter, BPlusTree bpt)
//        {
//            super(iter) ;
//            this.bpt = bpt ;
//        }
//        
//        @Override
//        protected Record rebalance(int id1, Record r1, int id2, Record r2)
//        {
//            RecordBufferPageMgr mgr = bpt.getRecordsMgr().getRecordBufferPageMgr() ;
//            RecordBufferPage page1 = mgr.get(id1) ;
//            RecordBufferPage page2 = mgr.get(id2) ;
//            
//            // Wrong calculatation.
//            for ( int i = page2.getCount() ; i <  page1.getMaxSize()/2 ; i++ )
//            {
//                //shiftOneup(node1, node2) ;
//                Record r = page1.getRecordBuffer().getHigh() ;
//                page1.getRecordBuffer().removeTop() ;
//
//                page2.getRecordBuffer().add(0, r) ;
//            }
//
//            bpt.getRecordsMgr().getRecordBufferPageMgr().put(page1) ;
//            bpt.getRecordsMgr().getRecordBufferPageMgr().put(page2) ;
//            
//            Record splitPoint = page1.getRecordBuffer().getHigh() ;
//            splitPoint = bpt.getRecordFactory().createKeyOnly(splitPoint) ;
//            //Record splitPoint = node1.maxRecord() ;
//            return splitPoint ;
//        }
//    }
//    
//    // ---- Block stream to BTreeNodeStream.
//    
//    private static Iterator<Pair<Integer, Record>> genTreeLevel(Iterator<Pair<Integer, Record>> iter,
//                                                               BPlusTree bpt,
//                                                               boolean leafLayer)
//    {
//        Iterator<Pair<Integer, Record>> iter2 = new BPTreeNodeBuilder(iter, bpt.getNodeManager(), leafLayer, bpt.getRecordFactory()) ;
//        
//        if ( rebalance )
//            iter2 = new RebalenceIndexEnd(iter2, bpt, leafLayer) ;
//        
//        if ( materialize && !debug )
//            iter2 = Iter.toList(iter2).iterator() ;
//        
//        if ( debug && rebalance )
//        {
//            System.out.println("After rebalance (index)") ;
//            //iter2 = summarizeIndexBlocks(iter2, bpt.getNodeManager()) ;
//            iter2 = printIndexBlocks(iter2, bpt.getNodeManager()) ;
//        }
//        return iter2 ;
//    }
//    
//    private abstract static class RebalenceBase extends IteratorWithBuffer<Pair<Integer, Record>>
//    {
//        protected RebalenceBase(Iterator<Pair<Integer, Record>> iter)
//        {
//            super(iter, 2) ;
//        }
//        
//        @Override
//        protected final void endReachedInner()
//        {
//            Pair<Integer, Record> pair1 = peek(0) ;
//            Pair<Integer, Record> pair2 = peek(1) ;
//            if ( pair1 == null || pair2 == null )
//                // Insufficient blocks to repack.
//                return ;
//            
//            if ( debug ) System.out.printf("Rebalance: %s %s\n", pair1, pair2) ; 
//            Record newSplitPoint = rebalance(pair1.car(), pair1.cdr(), pair2.car(), pair2.cdr()) ; 
//            // Needed??
//            if ( newSplitPoint != null )
//            {
//                if ( debug ) System.out.println("Reset split point: "+pair1.cdr()+" => "+newSplitPoint) ;
//                pair1 = new Pair<Integer, Record>(pair1.car(), newSplitPoint) ;
//                if ( debug ) System.out.printf("   %s %s\n", pair1, pair2) ;
//                set(0, pair1) ;
//            }
//        }
//        
//        protected abstract Record rebalance(int id1, Record r1, int id2, Record r2) ;
//    }
//    
//    private static class RebalenceIndexEnd extends RebalenceBase
//    {
//        private BPlusTree bpt ;
//
//        public RebalenceIndexEnd(Iterator<Pair<Integer, Record>> iter, BPlusTree bpt, boolean leafLayer)
//        {
//            super(iter) ;
//            this.bpt = bpt ;
//        }
//        
//        @Override
//        protected Record rebalance(int id1, Record r1, int id2, Record r2)
//        {
//            BPTreeNode node1 = bpt.getNodeManager().get(id1, -1) ;
//            BPTreeNode node2 = bpt.getNodeManager().get(id2, -1) ;
//            
//            // rebalence
//            // ** Need rebalance of data leaf layer. 
//            int x = node2.getCount() ;
//            if ( node2.getCount() >= bpt.getParams().getMinRec() )
//                return null ;
//
//            Record splitPoint = r1 ;
//            
//            {
//                // Shift up all in one go and use .set.
//                // Convert to block move ; should be code in BPTreeNode to do this (insert).
//                for ( int i = node2.getCount() ; i <  bpt.getParams().getMinRec() ; i++ )
//                {
//
//                    Record r = splitPoint ;
//
//                    int ptr = node1.getPtrBuffer().getHigh() ;
//                    splitPoint = node1.getRecordBuffer().getHigh() ; 
//
//                    node1.getPtrBuffer().removeTop() ;
//                    node1.getRecordBuffer().removeTop() ;
//                    node1.setCount(node1.getCount()-1) ;
//
//                    node2.getPtrBuffer().add(0, ptr) ;
//                    node2.getRecordBuffer().add(0, r) ;
//                    node2.setCount(node2.getCount()+1) ;
//
//
//                }
//            }
//            bpt.getNodeManager().put(node1) ;
//            bpt.getNodeManager().put(node2) ;
//            
//            return splitPoint ;
//        }
//    }
//
//    private static void fixupRoot(BPTreeNode root, Pair<Integer, Record> pair, BPlusTree bpt2)
//    {
//        root.getPtrBuffer().clear() ;
//        root.getRecordBuffer().clear() ;
//        
//        //BPTreeNode => BPTree copy.
//        BPTreeNode node = bpt2.getNodeManager().get(pair.car(), BPlusTreeParams.RootParent) ;
//        copyBPTreeNode(node, root, bpt2) ;
//        
//        bpt2.getNodeManager().release(node.getId()) ;
//    }
//    
//    private static void copyBPTreeNode(BPTreeNode nodeSrc, BPTreeNode nodeDst, BPlusTree bpt2)
//    {
//        PtrBuffer pBuff = nodeSrc.getPtrBuffer() ;
//        pBuff.copy(0, nodeDst.getPtrBuffer(), 0, pBuff.getSize()) ;
//        RecordBuffer rBuff = nodeSrc.getRecordBuffer() ;
//        rBuff.copy(0, nodeDst.getRecordBuffer(), 0, rBuff.getSize()) ;
//        nodeDst.setCount(nodeSrc.getCount()) ;
//        nodeDst.setIsLeaf(nodeSrc.isLeaf()) ;
//        bpt2.getNodeManager().put(nodeDst) ;
//    }
}
