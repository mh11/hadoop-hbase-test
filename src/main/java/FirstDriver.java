import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.medschl.mapper.Echo;

/**
 * 
 */

/**
 * @author mh719
 * 
 * http://stackoverflow.com/questions/8750764/what-is-the-fastest-way-to-bulk-load-data-into-hbase-programmatically
 *
 */
public class FirstDriver extends Configured implements Tool {
	  static Log LOG = LogFactory.getLog(FirstDriver.class);

	public static final String TABLE_NAME = "test-table";
	public static final String SERVER = "192.168.56.101";

	public int run(String[] args) throws Exception {
		String tableName = TABLE_NAME;
	    Path inputDir = new Path("variant-test-file.vcf");
	    Path outputDir = new Path("out.dir");
	    Class<? extends Mapper> clazz = Echo.class;
	    
	    Job job = Job.getInstance(getConf());
	    job.setJobName(this.getClass().getName() + "_" + tableName);	
	    
	    HBaseConfiguration.addHbaseResources(getConf());
	    job.setJarByClass(clazz);
	    
	    TextInputFormat.setInputPaths(job, inputDir);
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    job.setMapperClass(clazz);

	    job.setOutputFormatClass(HFileOutputFormat2.class);
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(Put.class);
	    HFileOutputFormat2.setOutputPath(job, outputDir);
	    
	    HTable hTable = new HTable(getConf(),tableName.getBytes());
	    HFileOutputFormat2.configureIncrementalLoad(job, hTable);
	    
		return job.waitForCompletion(true)?0:1;
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hadoop-local.xml");
		
//		Configuration conf = HBaseConfiguration.create();

		conf.set("hbase.zookeeper.quorum", SERVER);
		conf.set("hbase.master", SERVER+":60000");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		restTable(conf);
		int exitCode = 1;
		
		try {
			exitCode = ToolRunner.run(conf,new FirstDriver(),args);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		System.exit(exitCode);
	}


	public static void restTable(Configuration conf) {
		try (HConnection hconf = HConnectionManager.createConnection(conf)) {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor table = findTable(admin, TABLE_NAME);
			if(null != table){
				admin.disableTable(TABLE_NAME);
				admin.deleteTable(TABLE_NAME);
			}
			TableName tn = TableName.valueOf(TABLE_NAME);
			table = new HTableDescriptor(tn);
			table.addFamily(new HColumnDescriptor("c"));
			admin.createTable(table);
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
	
	private static HTableDescriptor findTable(HBaseAdmin admin, String tName) throws IOException{
		for (HTableDescriptor tab : admin.listTables()){
			if(tab.getNameAsString().equals(tName)){
				return tab;
			}
		}
		return null;
	}

}
