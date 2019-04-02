package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.ReflectionUtils;

public class SortReducerInputValues extends WritableComparator {
	Configuration conf = new Configuration();
	public WritableComparable newKey() {
	 
		return ReflectionUtils.newInstance(Text.class, conf);
	  }
	DataInputBuffer buffer= new DataInputBuffer();
	DataInputBuffer buffer1= new DataInputBuffer();
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		
		WritableComparable key1 = newKey();
		WritableComparable key2 = newKey();
		//buffer.
		buffer.reset(b1, s1, l1);
		
			try {
				key1.readFields(buffer);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		buffer1.reset(b2,s2,l2);
		
		try {
			key2.readFields(buffer1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String mykey=key1.toString();
		String[] data = mykey.split(",");
		
		Long value1 = Long.parseLong(data[2]);
		
		String mykey1=key2.toString();
		String[] data1 = mykey1.split(",");
		
		Long value2 = Long.parseLong(data1[2]);
		
		
		if(value2>value1)
			return 1;
		else if (value2<value1)
			return -1;
		else
		return (int) (0);
	}
	

}
