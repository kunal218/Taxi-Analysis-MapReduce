package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyWritableComparable implements WritableComparable<MyWritableComparable> {

	String key1 = new String();
	Long  key2;
	
	
	
	public String getKey1() {
		return key1;
	}

	public void setKey1(String key1) {
		this.key1 = key1;
	}

	public Long getKey2() {
		return key2;
	}

	public void setKey2(Long key2) {
		this.key2 = key2;
	}
	

	public MyWritableComparable(String key1, Long key2) {
		this.key1 = key1;
		this.key2 = key2;
	}
public MyWritableComparable() {
}
	public void write(DataOutput out) throws IOException {

		out.writeUTF(key1);
		out.writeLong(key2);
	}

	public void readFields(DataInput in) throws IOException {
		boolean check = true;
		while(check) {
			try {
		key1=in.readUTF();
		key2=in.readLong();
			}
			catch (Exception e) {
				check=false;
			}
			}
		
	}

	@Override
	public String toString() {
		return key1+","+key2;
	}

	public int compareTo(MyWritableComparable record) {
		String thiskey= this.key1;
		String thatkey = record.getKey1();
		
		Long thiskey2=this.getKey2();
		Long thatkey2 = record.getKey2();
		System.out.println("Mywritable Comparator -->"+thiskey2+":"+thatkey2);
		return thiskey.compareTo(thatkey)!= 0 ? thiskey.compareTo(thatkey):(thiskey2<thatkey2 ? 1 :(thiskey2==thatkey2 ? 0 : -1));
	}

}
