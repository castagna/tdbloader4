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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.tdbloader4.TDBLoader4Exception;
import org.apache.jena.tdbloader4.Utils;
import org.openjena.atlas.data.SerializationFactory;
import org.openjena.atlas.data.SortedDataBag;
import org.openjena.atlas.data.ThresholdPolicyCount;
import org.openjena.atlas.lib.Closeable;
import org.openjena.atlas.lib.Sink;
import org.openjena.atlas.lib.Tuple;


public class ExternalSort {

    public static void main(String[] args) throws IOException, InterruptedException {

        boolean compression = false ;
        
        if ( args.length != 2 ) {
            System.out.println("Usage: ExternalSort <input> <output>") ;
            System.exit(0) ;
        }
        
        for ( int i = 5; i < 9; i++ ) {
            int n = (int)Math.round(Math.pow(10, i)) ;
            System.out.println("------------------------------");
            generateRandomBinaryData(n, args[0] + ".dat", compression) ;
            generateRandomTextData(n, args[0] + ".txt", compression) ;
            sortBinaryData(args[0] + ".dat", args[1] + ".dat", compression) ;
            sortTextData(n, args[0] + ".txt", args[1] + ".txt", compression) ;
            System.out.println("------------------------------");
            generateRandomBinaryData(n, args[0] + ".dat", !compression) ;
            generateRandomTextData(n, args[0] + ".txt", !compression) ;
            sortBinaryData(args[0] + ".dat", args[1] + ".dat", !compression) ;
            sortTextData(n, args[0] + ".txt", args[1] + ".txt", !compression) ;
        }
    }
    
    public static void sortBinaryData(String input, String output, boolean compression) throws IOException {
        long start = System.currentTimeMillis() ;
        ThresholdPolicyCount<Tuple<Long>> policy = new ThresholdPolicyCount<Tuple<Long>>(1000000);
        SerializationFactory<Tuple<Long>> serializerFactory = new TupleSerializationFactory();
        Comparator<Tuple<Long>> comparator = new TupleComparator();
        SortedDataBag<Tuple<Long>> sortedDataBag = new SortedDataBag<Tuple<Long>>(policy, serializerFactory, comparator);

        long count = 0L;
        TupleInputStream in ;
        if ( compression ) {
            in = new TupleInputStream(new GZIPInputStream(new FileInputStream(input + ".gz")), 3) ;
        } else {
            in = new TupleInputStream(new FileInputStream(input), 3) ;
        }
        while ( in.hasNext() ) {
            sortedDataBag.add( in.next() ) ;
            count++;
        }
        in.close() ;
        Iterator<Tuple<Long>> iter = sortedDataBag.iterator() ;
        TupleOutputStream out = new TupleOutputStream(new FileOutputStream(output)) ;
        while ( iter.hasNext() ) {
            out.send( iter.next() );
        }
        out.close() ;
        sortedDataBag.close() ;
        long stop = System.currentTimeMillis() ;
        System.out.println ("Sort(binary" + (compression?" compressed":"") + "): " + count + " in " + (stop-start) + "ms") ;                
    }
    
    public static void sortTextData(long n, String input, String output, boolean compression) throws IOException, InterruptedException {
        long start = System.currentTimeMillis() ;
        Process p ;
        if ( compression ) {
            p = new ProcessBuilder("/bin/sh", "-c", "/bin/gunzip -c " + input + ".gz | /usr/bin/sort -u -k1,1 -k2,2 -k3,3 - > " + output).start() ;
        } else {
            p = new ProcessBuilder("/bin/sh", "-c", "/usr/bin/sort -u -k1,1 -k2,2 -k3,3 < " + input + " > " + output).start() ;
        }
        p.waitFor() ;
        long stop = System.currentTimeMillis() ;
        System.out.println ("Sort(text" + (compression?" compressed":"") + ")  : " + n + " in " + (stop-start) + "ms") ;                
    }

    public static void generateRandomBinaryData(int n, String output, boolean compression) throws IOException {
        long start = System.currentTimeMillis() ;
        Random r = new Random() ;
        TupleOutputStream out ;
        if ( compression ) {
            out = new TupleOutputStream(new GZIPOutputStream(new FileOutputStream (output + ".gz"))) ;
        } else {
            out = new TupleOutputStream(new FileOutputStream (output)) ;
        }
        for ( int i = 0; i < n; i++ ) {
            out.send(Tuple.create(r.nextLong(), r.nextLong(), r.nextLong())) ;
        }
        out.close() ;
        long stop = System.currentTimeMillis() ;
        System.out.println ("Gen(binary" + (compression?" compressed":"") + ") : " + n + " in " + (stop-start) + "ms") ;                
    }

    public static void generateRandomTextData(int n, String output, boolean compression) throws IOException {
        long start = System.currentTimeMillis() ;
        Random r = new Random() ;
        OutputStream out ;
        if ( compression ) {
            out = new BufferedOutputStream (new GZIPOutputStream(new FileOutputStream(output + ".gz"))) ;
        } else {
            out = new BufferedOutputStream (new FileOutputStream(output)) ;
        }
        for ( int i = 0; i < n; i++ ) {
            out.write(Utils.toHex(r.nextLong())) ;
            out.write(' ') ;
            out.write(Utils.toHex(r.nextLong())) ;
            out.write(' ') ;
            out.write(Utils.toHex(r.nextLong())) ;
            out.write(' ') ;
            out.write('\n');
        }
        out.close();
        long stop = System.currentTimeMillis() ;
        System.out.println ("Gen(text" + (compression?" compressed":"") + ")   : " + n + " in " + (stop-start) + "ms") ;                
    }
}




class TupleSerializationFactory implements SerializationFactory<Tuple<Long>> {

    @Override public Iterator<Tuple<Long>> createDeserializer(InputStream in) { return new TupleInputStream(in, 3); }
    @Override public Sink<Tuple<Long>> createSerializer(OutputStream out) { return new TupleOutputStream(out); }
    @Override public long getEstimatedMemorySize(Tuple<Long> item) { throw new TDBLoader4Exception("Method not implemented.") ; }

}

class TupleComparator implements Comparator<Tuple<Long>> {
    @Override
    public int compare(Tuple<Long> t1, Tuple<Long> t2) {
        int size = t1.size();
        if ( size != t2.size() ) throw new TDBLoader4Exception("Cannot compare tuple of different sizes.") ;
        for ( int i = 0; i < size; i++ ) {
            int result = t1.get(i).compareTo(t2.get(i)) ;
            if ( result != 0 ) {
                return result ;
            }
        }
        return 0;
    }
}

class TupleOutputStream implements Sink<Tuple<Long>> {

    private DataOutputStream out ;
    
    public TupleOutputStream(OutputStream out) {
        this.out = new DataOutputStream(new BufferedOutputStream(out)) ;
    }

    @Override
    public void send(Tuple<Long> tuple) {
        Iterator<Long> iter = tuple.iterator() ;
        while ( iter.hasNext() ) {
            try {
                out.writeLong( iter.next() ) ;
            } catch (IOException e) {
                new TDBLoader4Exception("Problems writing") ;
            }
        }
    }

    @Override
    public void flush() {
        try {
            out.flush() ;
        } catch (IOException e) {
            new TDBLoader4Exception("Problems flushing") ;
        }
    }

    @Override
    public void close() {
        try {
            out.close() ;
        } catch (IOException e) {
            new TDBLoader4Exception("Problems closing") ;
        }
    }
    
}

class TupleInputStream implements Iterator<Tuple<Long>>, Closeable {

    private DataInputStream in ;
    private int size ;
    private Tuple<Long> slot = null ;
    
    public TupleInputStream(InputStream in, int size) {
        this.in = new DataInputStream(new BufferedInputStream(in)) ;
        this.size = size ;
        slot = readNext() ;
    }

    @Override
    public boolean hasNext() {
        return slot != null ;
    }

    @Override
    public Tuple<Long> next() {
        Tuple<Long> result = slot ;
        slot = readNext() ;
        return result ;
    }
    
    private Tuple<Long> readNext() {
        try {
            if ( size == 3 ) {
                long s = in.readLong() ;
                long p = in.readLong() ;
                long o = in.readLong() ;
                return Tuple.create(s, p, o) ;
            } else if ( size == 4 ) {
                long s = in.readLong() ;
                long p = in.readLong() ;
                long o = in.readLong() ;
                long g = in.readLong() ;
                return Tuple.create(s, p, o, g) ;                
            } else {
                throw new TDBLoader4Exception("Unsupported size.") ;
            }
        } catch (IOException e) {
            return null ;
        }
    }

    @Override
    public void remove() {
        throw new TDBLoader4Exception("Method not implemented.") ;
    }

    @Override
    public void close() {
        try {
            in.close() ;
        } catch (IOException e) {
            new TDBLoader4Exception("Problems closing") ;
        }        
    }
    
}