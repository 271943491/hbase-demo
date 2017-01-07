package com.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

		job.setNumReduceTasks(3);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.waitForCompletion(true);

		// isExists("person");
		// updateData("person", "rowkey5", "info", "visitingName", "100");
		// selectByRowKeyFamilyQualifier("scores4", "Tom", "course", "math");
		// createTable("ytf", "salary");
		// listAllTable();
		// deleteByRowKey("scores4", "Jim");
		// scanTable("scores4");
		selectByRowKey("person", "rowkey1");
		// insertTable("person", "rowkey3", "info", "name", "hq");
		// filterData("scores4","course","math","97");
		// filterData("scores4", "Y");
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
			int taskNum = context.getNumReduceTasks();
			// task_local579161201_0001_r_000000
			int taskId = context.getTaskAttemptID().getTaskID().getId();
			context.write(new Text(key + "|" + String.valueOf(taskNum + taskId)), new IntWritable(sum));
			insertData("person", "rowkey4", "info", "pageNum", key.toString());
			insertData("person", "rowkey4", "info", "visitingNum", String.valueOf(sum));
		}
	}

	// 判断表是否存在
	public static void isExists(String tableName) {

		try {
			HBaseAdmin admin = new HBaseAdmin(hconfig);
			if (admin.tableExists(tableName)) {
				System.out.println(tableName + ",该表存在");
			} else {
				System.out.println(tableName + ",该表不存在");
			}
			admin.close();
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

	// 列出所有表
	public static void listAllTable() {

		try {
			HBaseAdmin admin = new HBaseAdmin(hconfig);
			TableName[] tableas = admin.listTableNames();
			for (TableName table : tableas) {
				System.out.println(table.getNameAsString());
			}
			admin.close();
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

	// 建表 tableName：表名，family:列族，qualifier：列，value：值
	public static void createTable(String tableName, String family) {
		try {
			HBaseAdmin admin = new HBaseAdmin(hconfig);
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			HColumnDescriptor hColumnDesc = new HColumnDescriptor(family);
			HTableDescriptor htableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			htableDesc.addFamily(hColumnDesc);
			// 初始化第一个rowkey，最后一个rowkey，region个数
			admin.createTable(htableDesc, "00".getBytes(), "10".getBytes(), 3);
			admin.close();
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

	// 查询表所有数据
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

	// 根据rowkey查询数据
	public static void selectByRowKey(String tableName, String rowkey) {

		try {

			HTableInterface table = new HTableFactory().createHTableInterface(hconfig, tableName.getBytes());

			// Connection connection =
			// ConnectionFactory.createConnection(hconfig);
			// Table table = connection.getTable(TableName.valueOf(tableName));
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

	// 根据rowkey和列族查询数据
	public static void selectByRowKeyAndFamily(String tableName, String rowkey, String family) {

		try {
			Connection connection = ConnectionFactory.createConnection(hconfig);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowkey.getBytes());
			get.addFamily(family.getBytes());
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

	// 根据rowkey、列族、列查询数据
	public static void selectByRowKeyFamilyQualifier(String tableName, String rowkey, String family, String qualifier) {

		try {
			Connection connection = ConnectionFactory.createConnection(hconfig);
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get = new Get(rowkey.getBytes());
			get.addColumn(family.getBytes(), qualifier.getBytes());
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

	// 插入数据到hbase
	public static void insertData(String tableName, String rowkey, String family, String qualifier, String value) {

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

	// 更新数据
	public static void updateData(String tableName, String rowkey, String family, String qualifier, String value) {
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

	public static void deleteByRowKey(String tableName, String rowkey) {
		try {

			Connection connection = ConnectionFactory.createConnection(hconfig);

			Table table = connection.getTable(TableName.valueOf(tableName));

			Delete delete = new Delete(rowkey.getBytes());

			table.delete(delete);

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

	// 过滤器，过滤value在一个范围的数据
	public static void filterData(String tableName, String family, String qualifier, String value) {
		try {

			HTable table = new HTable(hconfig, tableName);

			List<Filter> filters = new ArrayList<Filter>();

			filters.add(new SingleColumnValueFilter(family.getBytes(), qualifier.getBytes(), CompareOp.GREATER,
					value.getBytes())); // 大于该值
			filters.add(new SingleColumnValueFilter(family.getBytes(), qualifier.getBytes(), CompareOp.LESS,
					value.getBytes())); // 小于该值

			FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, filters);
			ResultScanner resultScanner = table.getScanner(new Scan().setFilter(filterList));

			for (Result result : resultScanner) {
				for (KeyValue keyValue : result.raw()) {
					System.out
							.println(new String(keyValue.getQualifier()) + "------" + new String(keyValue.getValue()));
				}
			}

			table.close();

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

	// 过滤器，按rowkay过滤
	public static void filterData(String tableName, String rowkay) {
		try {

			HTable table = new HTable(hconfig, tableName);
			Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowkay));
			ResultScanner resultScanner = table.getScanner(new Scan().setFilter(filter));

			for (Result result : resultScanner) {
				for (KeyValue keyValue : result.raw()) {
					System.out
							.println(new String(keyValue.getQualifier()) + "------" + new String(keyValue.getValue()));
				}
			}

			table.close();

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
