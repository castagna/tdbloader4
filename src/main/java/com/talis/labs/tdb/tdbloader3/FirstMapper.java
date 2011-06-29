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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.openjena.atlas.lib.Sink;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.core.Quad;

public class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(FirstMapper.class);
    private ParserProfile profile;

    public void setup(Context context) throws IOException, InterruptedException {
        profile = Utils.createParserProfile(context.getJobID(), ((FileSplit) context.getInputSplit()).getPath());
    }
    
    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if ( log.isDebugEnabled() ) log.debug("< ({}, {})", key, value);
        SinkToContext sink = new SinkToContext(context);
        Tokenizer tokenizer = TokenizerFactory.makeTokenizerString(value.toString());
        LangNQuads parser = new LangNQuads(tokenizer, profile, sink) ;
        parser.parse();
    }

}

class SinkToContext implements Sink<Quad> {

    private static final Logger log = LoggerFactory.getLogger(FirstMapper.class);
    
    private Mapper<LongWritable, Text, Text, Text>.Context context;
    private Text st = new Text();
    private Text pt = new Text();
    private Text ot = new Text();
    private Text gt = new Text();
    private Text ht = new Text();
    private static byte[] S; 
    private static byte[] P; 
    private static byte[] O; 
    private static byte[] G; 

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
    
    public SinkToContext(Mapper<LongWritable, Text, Text, Text>.Context context) {
        this.context = context;
    }

    @Override
    public void send(Quad quad) {
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
                
                emit(st, hs);
                emit(pt, hp);
                emit(ot, ho);
            }
            if ( g != null ) {
                gt.set(g);
                Text hg = new Text(ht); hg.append(G, 0, G.length);
                emit(gt, hg);
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

    private void emit ( Text key, Text value ) throws IOException, InterruptedException {
        context.write(key, value);
        if ( log.isDebugEnabled() ) {
            log.debug("> ({}, {})", key, value);
        }
    }
    
    @Override public void flush() {}
    @Override public void close() {}
    
}