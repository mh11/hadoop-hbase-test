/**
 * 
 */
package uk.ac.cam.medschl.mapper;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author mh719
 *
 */
public class Echo extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	
	protected void map(LongWritable key, Text txt, Mapper<LongWritable,Text,ImmutableBytesWritable,Put>.Context context) throws java.io.IOException ,InterruptedException {
		ImmutableBytesWritable hKey = new ImmutableBytesWritable();

		try {
			Put put = parse(txt.toString(), hKey);
			if(null == put)
				return;
			context.write(hKey, put);
			context.getCounter("test","lines").increment(1);
		} catch (Exception e) {
			context.getCounter("Echo", "PARSE_ERRORS").increment(1);
			return;
		}
	}

	private Put parse(String row, ImmutableBytesWritable hKey) {
		hKey.set(Bytes.toBytes(row));
		Put put = new Put(hKey.copyBytes());
		put.add("c".getBytes(), "x".getBytes(), row.getBytes());
		return put;
	};

}
