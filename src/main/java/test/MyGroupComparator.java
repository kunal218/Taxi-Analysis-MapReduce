package test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator{

	public MyGroupComparator() {
		super(MyWritableComparable.class,true);
	}
	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {

MyWritableComparable a_key=(MyWritableComparable) key1;
		System.out.println("Grou ********");
		MyWritableComparable b_key=(MyWritableComparable) key2;
		
		String thiskey = a_key.getKey1();
		String thatkey = b_key.getKey1();
		return thiskey.compareTo(thatkey);
	}
}
