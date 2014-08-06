import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import net.sf.samtools.util.IOUtil;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.variant.hbase.JsonPutMapper;

/**
 * 
 */

/**
 * 
 * # scp dirctory over to server
 * scp -r out.dir root@192.168.56.101:/root/.
 * # log into server and copy data into hdfs
 * hadoop fs -copyFromLocal out.dir out-dir
 * # load into hbase
 * hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/root/out-dir/ test-table
 * 
 * @author mh719
 * 
 * http://stackoverflow.com/questions/8750764/what-is-the-fastest-way-to-bulk-load-data-into-hbase-programmatically
 *
 */
public class FirstDriver extends Configured implements Tool {
	  static Log LOG = LogFactory.getLog(FirstDriver.class);

	public static String TABLE_NAME = "test-table";
	public static String SERVER = "192.168.56.101";

	public int run(String[] args) throws Exception {
		String tableName = args[1];
		String fName = args[2];
		String oName = args[3];
	    Path inputDir = new Path(fName);
	    Path outputDir = new Path(oName);
	    Class<? extends Mapper> clazz = JsonPutMapper.class;
	    
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
		if(args.length != 4){ // quick fix
			System.err.println("Please provide the following parameters: <server> <table-name> <infile> <output>");
			System.exit(1);	
		}
		SERVER=args[0];
		TABLE_NAME = args[1];
				
		Configuration conf = new Configuration();
		conf.addResource(FirstDriver.class.getClassLoader().getResourceAsStream("hadoop-local.xml"));

		conf.set("hbase.zookeeper.quorum", SERVER);
		conf.set("hbase.master", SERVER+":60000");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		conf.setBoolean("includeSamples", true);
		resetTable(conf,false);
		int exitCode = 1;
		
		try {
			exitCode = ToolRunner.run(conf,new FirstDriver(),args);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		System.exit(exitCode);
	}


	public static void resetTable(Configuration conf,boolean delete) {
		try (HConnection hconf = HConnectionManager.createConnection(conf)) {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor table = findTable(admin, TABLE_NAME);
			boolean exist = null != table;
			
			if(exist){
				if(delete){
					admin.disableTable(TABLE_NAME);
					admin.deleteTable(TABLE_NAME);
				} else {
					return; // return if table exists - no need to continue
				}
			}
			TableName tn = TableName.valueOf(TABLE_NAME);
			table = new HTableDescriptor(tn);
			table.addFamily(new HColumnDescriptor("d"));
			admin.createTable(table);
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(1); // TODO quick fix - not nice
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
