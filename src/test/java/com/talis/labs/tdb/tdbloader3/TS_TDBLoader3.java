package com.talis.labs.tdb.tdbloader3;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.talis.labs.tdb.tdbloader3.io.TestLongQuadWritable;

@RunWith(Suite.class)
@Suite.SuiteClasses( {
    TestTDBLoader3.class,
    TestLongQuadWritable.class
})

public class TS_TDBLoader3 {}
