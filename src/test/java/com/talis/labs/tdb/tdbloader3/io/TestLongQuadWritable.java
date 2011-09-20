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

package com.talis.labs.tdb.tdbloader3.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestLongQuadWritable {

	@Test public void test1() {
		LongQuadWritable lqw = new LongQuadWritable(0,1,2,3);
		assertEquals(0l, lqw.get(0));
		assertEquals(1l, lqw.get(1));
		assertEquals(2l, lqw.get(2));
		assertEquals(3l, lqw.get(3));
	}
	
	@Test public void test2() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw3 = new LongQuadWritable(0,-1,2,3);
		assertEquals(lqw1, lqw2);
		assertEquals(lqw2, lqw1);
		assertFalse(lqw1.equals(lqw3));
		assertFalse(lqw3.equals(lqw1));
	}

	@Test public void test3() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		assertEquals(0, lqw1.compareTo(lqw2));
	}

	@Test public void test3a() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3, null);
		assertEquals(lqw1, lqw2);
	}

	@Test public void test3b() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3, "NON EXISTING");
		assertEquals(lqw1, lqw2);
	}

	@Test public void test3c() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3, "SPO");
		assertFalse(lqw1.equals(lqw2));
		assertEquals(-1, lqw1.compareTo(lqw2));
		assertEquals(1, lqw2.compareTo(lqw1));
	}

	@Test public void test4() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(-0l,1,2,3);
		assertEquals(0, lqw1.compareTo(lqw2));
	}

	@Test public void test5() {
		LongQuadWritable lqw1 = new LongQuadWritable(1,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		assertEquals(1, lqw1.compareTo(lqw2));
	}
	
	@Test public void test6() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(1,1,2,3);
		assertEquals(-1, lqw1.compareTo(lqw2));
	}
	
	@Test public void test7() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,2,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		assertEquals(1, lqw1.compareTo(lqw2));
	}

	@Test public void test8() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,2,2,3);
		assertEquals(-1, lqw1.compareTo(lqw2));
	}
	
	@Test public void test9() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,3,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		assertEquals(1, lqw1.compareTo(lqw2));
	}

	@Test public void test10() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,3,3);
		assertEquals(-1, lqw1.compareTo(lqw2));
	}

	@Test public void test11() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,4);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,3);
		assertEquals(1, lqw1.compareTo(lqw2));
	}

	@Test public void test12() {
		LongQuadWritable lqw1 = new LongQuadWritable(0,1,2,3);
		LongQuadWritable lqw2 = new LongQuadWritable(0,1,2,4);
		assertEquals(-1, lqw1.compareTo(lqw2));
	}
	
}
