package com.mapreduce.taxi.top10mostchargedrides;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator{

	public MySortComparator() {
		super(MyWritableComparable.class,true);
	}
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {

		MyWritableComparable a_key=(MyWritableComparable) key1;
		
		MyWritableComparable b_key=(MyWritableComparable) key2;
		
		String thiskey = a_key.getKey1();
		String thatkey = b_key.getKey1();
		Double thiskey2 = a_key.getKey2();
		Double thatkey2 = b_key.getKey2();
		
		return thiskey.compareTo(thatkey)!= 0 ? thiskey.compareTo(thatkey):(thiskey2<thatkey2 ? 1 :(thiskey2==thatkey2 ? 0 : -1));
	}
}
