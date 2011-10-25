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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.jena.tdbloader3.io.QuadWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;

public class SecondMapper extends Mapper<LongWritable, QuadWritable, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(SecondMapper.class);

    private Text st = new Text();
    private Text pt = new Text();
    private Text ot = new Text();
    private Text gt = new Text();
    private Text ht = new Text();
    private static byte[] S;
    private static byte[] P;
    private static byte[] O;
    private static byte[] G;
    
    private Counters counters;

    static {
        try {
            S = new String("|S").getBytes("UTF-8");
            P = new String("|P").getBytes("UTF-8");
            O = new String("|O").getBytes("UTF-8");
            G = new String("|G").getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new TDBLoader3Exception(e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	counters = new Counters(context);
    };
    
    @Override
    public void map (LongWritable key, QuadWritable value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);
        Quad quad = value.getQuad();
        String s = Utils.serialize(quad.getSubject());
        String p = Utils.serialize(quad.getPredicate());
        String o = Utils.serialize(quad.getObject());
        String g = null;
        if ( !quad.isDefaultGraphGenerated() ) {
            g = Utils.serialize(quad.getGraph());
        }

        // TODO: reuse hash from TDB NodeTableNative?
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("MD5");
            digest.update(s.getBytes("UTF-8"));
            digest.update(p.getBytes("UTF-8"));
            digest.update(o.getBytes("UTF-8"));
            if ( g != null ) digest.update(g.getBytes("UTF-8"));
            String hash = new String(Hex.encodeHex(digest.digest()));
            ht.set(hash);
            if ( ( s != null ) && ( p != null ) && ( o != null) ) {
                st.set(s);
                pt.set(p);
                ot.set(o);
                
                Text hs = new Text(ht); hs.append(S, 0, S.length);
                Text hp = new Text(ht); hp.append(P, 0, P.length);
                Text ho = new Text(ht); ho.append(O, 0, O.length);
                
                emit(context, st, hs);
                emit(context, pt, hp);
                emit(context, ot, ho);
            }
            if ( g != null ) {
                gt.set(g);
                Text hg = new Text(ht); hg.append(G, 0, G.length);
                emit(context, gt, hg);
                counters.incrementQuads();
            } else {
            	counters.incrementTriples();
            }
        } catch (Exception e) {
            throw new TDBLoader3Exception(e);
        } finally {
            st.clear();
            pt.clear();
            ot.clear();
            gt.clear();
            ht.clear();
        }
    }

    private void emit ( Context context, Text key, Text value ) throws IOException, InterruptedException {
        context.write(key, value);
        log.debug("> ({}, {})", key, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	counters.close();
    }

}
