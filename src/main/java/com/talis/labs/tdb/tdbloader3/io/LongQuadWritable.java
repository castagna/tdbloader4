package com.talis.labs.tdb.tdbloader3.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LongQuadWritable  extends BinaryComparable implements WritableComparable<BinaryComparable>, Serializable, Cloneable {

	private static final long serialVersionUID = -7228403148893164927L;
	static final int LONG_BYTE_LENGTH = 8;
	static final int LONG_QUAD_BYTE_LENGTH = 4 * LONG_BYTE_LENGTH;
	private byte[] b = new byte [LONG_QUAD_BYTE_LENGTH + 1];
	private static final Map<String, Byte> indexNameToByte = new HashMap<String,Byte>(); 
	private static final Map<Byte, String> indexByteToName = new HashMap<Byte,String>(); 

	static {
		WritableComparator.define(LongQuadWritable.class, new LongQuadComparator());
		
		indexNameToByte.put("SPO", (byte)0);
		indexNameToByte.put("POS", (byte)1);
		indexNameToByte.put("OSP", (byte)2);
		indexNameToByte.put("SPOG", (byte)3);
		indexNameToByte.put("POSG", (byte)4);
		indexNameToByte.put("OSPG", (byte)5);
		indexNameToByte.put("GSPO", (byte)6);
		indexNameToByte.put("GPOS", (byte)7);
		indexNameToByte.put("GOSP", (byte)8);
		
		indexByteToName.put((byte)0, "SPO");
		indexByteToName.put((byte)1, "POS");
		indexByteToName.put((byte)2, "OSP");
		indexByteToName.put((byte)3, "SPOG");
		indexByteToName.put((byte)4, "POSG");
		indexByteToName.put((byte)5, "OSPG");
		indexByteToName.put((byte)6, "GSPO");
		indexByteToName.put((byte)7, "GPOS");
		indexByteToName.put((byte)8, "GOSP");
	}
	
	public LongQuadWritable() {
		set(0, 0l);
		set(1, 0l);
		set(2, 0l);
		set(3, 0l);
		b[LONG_QUAD_BYTE_LENGTH] = -1;
	}
	
	public LongQuadWritable(long l1, long l2, long l3, long l4) {
		set(0, l1);
		set(1, l2);
		set(2, l3);
		set(3, l4);
		b[LONG_QUAD_BYTE_LENGTH] = -1;
	}
	
	public LongQuadWritable(long l1, long l2, long l3, long l4, String indexName) {
		set(0, l1);
		set(1, l2);
		set(2, l3);
		set(3, l4);
		setIndexName(indexName);
	}
	
	public LongQuadWritable(LongQuadWritable that) {
		set(0, that.get(0));
		set(1, that.get(1));
		set(2, that.get(2));
		set(3, that.get(3));
		setIndexName(that.getIndexName());
	}
	
	public void set(int i, long value) {
	    for (int j = LONG_BYTE_LENGTH * i, k = 56; k >= 0; j++, k -= 8) {
	    	b[j] = (byte) (value >> k);
	    }
	}
	
	public long get(int i) {
	    long value = 0l;
	    for (int j = LONG_BYTE_LENGTH * i, k = 56; k >= 0; j++, k -= 8) {
	      value |= (b[j] & 0xFF) << k;
	    }
	    return value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		in.readFully(b);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(b);
	}

	@Override
	public byte[] getBytes() {
		return b;
	}

	@Override
	public int getLength() {
		return b.length;
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(b);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof LongQuadWritable)) return false;
		LongQuadWritable other = (LongQuadWritable) obj;
		return Arrays.equals(b, other.b);
	}

	@Override
	public int compareTo(BinaryComparable other) {
		return LongQuadComparator.doCompare(b, 0, ((LongQuadWritable) other).b, 0);
	}

	@Override
	public Object clone() {
		return new LongQuadWritable(this);
	}

	@Override
	public String toString() {
		return "{" + get(0) + ", " + get(1) + ", " + get(2) + ", " + get(3) + ", " + getIndexName() + '}';
	}
	
	public static final class LongQuadComparator extends WritableComparator implements Serializable {
		private static final long serialVersionUID = -8463347206346204927L;

		public LongQuadComparator() {
			super(LongQuadWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return doCompare(b1, s1, b2, s2);
		}

		static int doCompare(byte[] b1, int s1, byte[] b2, int s2) {
			for ( int i = 0; i < 4; i++ ) {
				int compare = compareLongs(b1, s1 + LONG_BYTE_LENGTH * i, b2, s2 + LONG_BYTE_LENGTH * i);
				if (compare != 0) {
					return compare;
				}				
			}
			return b1[LONG_QUAD_BYTE_LENGTH] - b2[LONG_QUAD_BYTE_LENGTH];
		}

		private static int compareLongs(byte[] b1, int s1, byte[] b2, int s2) {
			int end1 = s1 + LONG_BYTE_LENGTH;
			for (int i = s1, j = s2; i < end1; i++, j++) {
				int a = b1[i];
				int b = b2[j];
				if (i > s1) {
					a &= 0xff;
					b &= 0xff;
				}
				if (a != b) {
					return a - b;
				}
			}
			return 0;
		}
	}

	public void clear() {
		set ( -1l, -1l, -1l, -1l );
		b[LONG_QUAD_BYTE_LENGTH] = -1;
	}

	public void set(long l1, long l2, long l3, long l4) {
		set (0, l1);
		set (1, l2);
		set (2, l3);
		set (3, l4);
	}
	
	public void set(long l1, long l2, long l3) {
		set (0, l1);
		set (1, l2);
		set (2, l3);
		set (3, -1l);
	}

	public void setIndexName(String indexName) {
		Byte v = indexNameToByte.get(indexName);
		if ( v != null ) {
			b[LONG_QUAD_BYTE_LENGTH] = v;
		} else {
			b[LONG_QUAD_BYTE_LENGTH] = -1;
		}
	}

	public String getIndexName() {
		return indexByteToName.get(b[LONG_QUAD_BYTE_LENGTH]);
	}

	public void set(long s, long p, long o, long g, String indexName) {
		set(s,p,o,g);
		setIndexName(indexName);
	}

}
