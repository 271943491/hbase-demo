package com.demo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@SuppressWarnings("deprecation")
public class HbaseApi {

	public static HBaseConfiguration hconfig = new HBaseConfiguration();

	static {
		hconfig.set("hbase.zookeeper.quorum", "youzy.domain,youzy2.domain,youzy3.domain");
		hconfig.set("hbase.master", "youzy.domain:60000");
		hconfig.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");

		String input = "hdfs://10.0.12.114:9000/tmp/y1.txt";
		String outPath = "hdfs://10.0.12.114:9000/tmp/out";

		Job job = new Job(conf, "API");
		job.setJarByClass(HbaseApi.class);

		job.setMapperClass(KPIPVMapper.class);
		job.setReducerClass(KPIPVReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);

		// isExists("person");
		// scanTable("person");
		// selectByRowKey("person", "rowkey1");
		// insertTable("person", "rowkey3", "info", "name", "hq");

	}

	public static class KPIPVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int PAGENUM = 0;
		int VISITINGNUM = 1;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] lines = value.toString().split("\n");

			for (String line : lines) {

				String page = line.split("\\|")[PAGENUM];
				int visiting = Integer.valueOf(line.split("\\|")[VISITINGNUM]);
				context.write(new Text(page), new IntWritable(visiting));
			}
		}
	}

	public static class KPIPVReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(new Text(key), new IntWritable(sum));
			insertTable("person", "rowkey4", "info", "pageNum", key.toString());
			insertTable("person", "rowkey4", "info", "visitingNum", String.valueOf(sum));
		}
	}

	public static void isExists(String tableName) {

		try {
			HBaseAdmin admin = new HBaseAdmin(hconfig);
			if (admin.tableExists(tableName)) {
				System.out.println(tableName + ",该表存在");
			} else {
				System.out.println(tableName + ",该表不存在");
			}
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void listAllTable() {

		try {
			HBaseAdmin admin = new HBaseAdmin(hconfig);
			TableName[] tableas = admin.listTableNames();
			for (TableName table : tableas) {
				System.out.println(table.getNameAsString());
			}
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void selectByRowKey(String tableName, String rowkey) {

		try {
			Connection connection = ConnectionFactory.createConnection(hconfig);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowkey.getBytes());
			Result result = table.get(get);

			for (KeyValue keyValue : result.raw()) {
				System.out.println(new String(keyValue.getQualifier()) + "------" + new String(keyValue.getValue()));
			}

		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void insertTable(String tableName, String rowkey, String family, String qualifier, String value) {

		try {
			Connection connection = ConnectionFactory.createConnection(hconfig);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(rowkey.getBytes());
			put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
			table.put(put);

		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void scanTable(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(hconfig);
			Table table = connection.getTable(TableName.valueOf(tableName));
			ResultScanner resultScanner = table.getScanner(new Scan());
			for (Result result : resultScanner) {
				for (KeyValue keyvalue : result.raw()) {
					System.out.println(
							"行名:" + new String(keyvalue.getRow()) + ",列簇名:" + new String(keyvalue.getFamily()) + ",列名:"
									+ new String(keyvalue.getQualifier()) + ",值:" + new String(keyvalue.getValue()));
				}
			}
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}