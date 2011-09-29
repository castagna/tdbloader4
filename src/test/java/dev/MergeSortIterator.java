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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.openjena.atlas.iterator.Iter;
import org.openjena.atlas.lib.Closeable;

public class MergeSortIterator<T> implements Iterator<T>, Closeable
{
    private final List<Iterator<T>> inputs;
    private final Comparator<? super T> comp;
    private final PriorityQueue<Item<T>> minHeap;
//    private Record prev = null;
    
    public MergeSortIterator(List<Iterator<T>> inputs, Comparator<? super T> comp)
    {
        this.inputs = inputs;
        this.comp = comp;
        this.minHeap = new PriorityQueue<Item<T>>(inputs.size());
        
        // Prime the heap
        for (int i=0; i<inputs.size(); i++)
        {
            replaceItem(i);
        }
    }
    
    private void replaceItem(int index)
    {
        Iterator<T> it = inputs.get(index);
        if (it.hasNext())
        {
            T tuple = it.next();
            minHeap.add(new Item<T>(index, tuple, comp));
        }
    }

    public boolean hasNext()
    {
        return (minHeap.peek() != null);
    }

    public T next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }
        
        Item<T> curr = null;
    	curr = minHeap.poll();
    	// Read replacement item
    	replaceItem(curr.getIndex());
        return curr.getTuple();        	

       
        // Work in progress: we need to eliminate duplicates and adjust ids...
//        Record record = null;
//        Map<Long, Record> items = new TreeMap<Long, Record>();
//        do {
//        	record = (Record)curr.getTuple();
//        	curr = minHeap.poll();
//    		Long id = Bytes.getLong(record.getValue());
//    		items.put (id, curr) ;
//        	// Read replacement item
//        	replaceItem(curr.getIndex());
//        } while ( Bytes.compare(record.getKey(), prev.getKey()) == 0 );
//
//        Iterator<Long> iter = items.keySet().iterator();
//        if ( iter.hasNext() ) {
//        	Record result = items.get(iter.next());
//        	prev = result ;
//        	return result;
//        } else {
//            return curr.getTuple();        	
//        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException("SpillSortIterator.remove");
    }

    public void close()
    {
        for (Iterator<T> it : inputs)
        {
            Iter.close(it);
        }
    }
    
    private final class Item<U> implements Comparable<Item<U>>
    {
        private final int index;
        private final U tuple;
        private final Comparator<? super U> c;
        
        public Item(int index, U tuple, Comparator<? super U> c)
        {
            this.index = index;
            this.tuple = tuple;
            this.c = c;
        }
        
        public int getIndex()
        {
            return index;
        }
        
        public U getTuple()
        {
            return tuple;
        }
        
        @SuppressWarnings("unchecked")
        public int compareTo(Item<U> o)
        {
            return (null != c) ? c.compare(tuple, o.getTuple()) : ((Comparable<U>)tuple).compareTo(o.getTuple());
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof Item)
            {
                return compareTo((Item<U>)obj) == 0;
            }
            
            return false;
        }
        
        @Override
        public int hashCode()
        {
            return tuple.hashCode();
        }
        
        @Override
        public String toString() {
        	return getIndex() + " : " + getTuple();
        }
        
    }
    
}
