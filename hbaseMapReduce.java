package com.demo;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class hbaseMapReduce {

	@SuppressWarnings("deprecation")
	public static HBaseConfiguration hconfig = new HBaseConfiguration();

	static {
		hconfig.set("hbase.zookeeper.quorum", "youzy.domain,youzy2.domain,youzy3.domain");
		hconfig.set("hbase.master", "youzy.domain:60000");
		hconfig.set("hbase.zookeeper.property.clientPort", "2181");
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Job job = new Job(hconfig, "HBase_FindFriend");
		job.setJarByClass(hbaseMapReduce.class);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("author"), Bytes.toBytes("nickname"));
		scan.addColumn(Bytes.toBytes("article"), Bytes.toBytes("tag"));
		TableMapReduceUtil.initTableMapperJob("blog2", scan, hbaseMapReduce.Mapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("tag_friend", hbaseMapReduce.Reducer.class, job);
		job.waitForCompletion(true);
	}

	public static class Mapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		@SuppressWarnings("deprecation")
		@Override
		public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			ImmutableBytesWritable value = null;
			String[] tags = null;
			for (KeyValue kv : values.list()) {
				if ("author".equals(Bytes.toString(kv.getFamily()))
						&& "nickname".equals(Bytes.toString(kv.getQualifier()))) {
					value = new ImmutableBytesWritable(kv.getValue());
				}
				if ("article".equals(Bytes.toString(kv.getFamily()))
						&& "tag".equals(Bytes.toString(kv.getQualifier()))) {
					tags = Bytes.toString(kv.getValue()).split(",");
				}
			}
			for (int i = 0; i < tags.length; i++) {
				ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(tags[i].toLowerCase()));
				try {
					context.write(key, value);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		}
	}

	public static class Reducer
			extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		@SuppressWarnings("deprecation")
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			String friends = "";
			for (ImmutableBytesWritable val : values) {
				friends += (friends.length() > 0 ? "," : "") + Bytes.toString(val.get());
			}
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("person"), Bytes.toBytes("nicknames"), Bytes.toBytes(friends));
			context.write(key, put);
		}
	}
}
