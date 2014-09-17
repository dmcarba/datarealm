
package com.dcm.datarealm.sequencer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple example job that writes the same records that reads but sorted (input
 * must be text)
 */
public class SequencerJob extends Configured implements Tool
{

	public static class SequencerMap extends
			Mapper<LongWritable, Text, SplitOffsetTuple, OffsetLineTuple>
	{
		private SplitOffsetTuple currentSplitOffset;
		private OffsetLineTuple currentOffsetText;
		private int totalSplitCount;
		private int splitId;
		private long lineCount;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			currentOffsetText.setLine(String.valueOf(lineCount));
			System.out.println("Split: " + splitId);
			System.out.println("Count: " + lineCount);
			for (int i = splitId; i < totalSplitCount; i++)
			{
				currentSplitOffset.setSplit(i + 1);
				currentSplitOffset.setOffset(-splitId);
				currentOffsetText.setOffset(-splitId);
				context.write(currentSplitOffset, currentOffsetText);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			lineCount = 0;
			totalSplitCount = ((SequencerFileSplit) context.getInputSplit()).getTotalSplits();
			splitId = ((SequencerFileSplit) context.getInputSplit()).getSplitId();
			currentSplitOffset = new SplitOffsetTuple(splitId, 0);
			currentOffsetText = new OffsetLineTuple();
		}

		@Override
		protected void map(LongWritable offset, Text line, Context context) throws IOException,
				InterruptedException
		{
			currentSplitOffset.setOffset(lineCount);
			currentOffsetText.setLine(line.toString());
			currentOffsetText.setOffset(lineCount);
			context.write(currentSplitOffset, currentOffsetText);
			lineCount++;
		};
	}

	public static class SequencerReducer extends
			Reducer<SplitOffsetTuple, OffsetLineTuple, LongWritable, Text>
	{
		private LongWritable sequence = new LongWritable();
		private Text line = new Text();

		protected void reduce(SplitOffsetTuple key, Iterable<OffsetLineTuple> tuples,
				Context context) throws IOException, InterruptedException
		{
			Iterator<OffsetLineTuple> iterator = tuples.iterator();
			long sequenceNumber = 0;
			OffsetLineTuple tuple = iterator.next();
			while (iterator.hasNext() && tuple.getOffset() < 0)
			{
				sequenceNumber += Long.valueOf(tuple.getLine());
				tuple = iterator.next();
			}
			sequence.set(sequenceNumber);
			line.set(tuple.getLine());
			context.write(sequence, line);
			sequenceNumber++;
			while (iterator.hasNext())
			{
				tuple = iterator.next();
				sequence.set(sequenceNumber);
				line.set(tuple.getLine());
				context.write(sequence, line);
				sequenceNumber++;
			}
		}
	}

	public static class SequencerPartitioner extends Partitioner<SplitOffsetTuple, OffsetLineTuple>
			implements Configurable
	{

		private Configuration conf;

		@Override
		public int getPartition(SplitOffsetTuple key, OffsetLineTuple value, int numPartitions)
		{
			int totalSplits = conf.getInt("splitNumber", 0);
			return ((key.getSplit() - 1) * numPartitions) / totalSplits;
		}

		@Override
		public Configuration getConf()
		{
			return conf;
		}

		@Override
		public void setConf(Configuration conf)
		{
			this.conf = conf;
		}
	}

	public static class SequencerGroupingComparator extends WritableComparator
	{
		protected SequencerGroupingComparator()
		{
			super(SplitOffsetTuple.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			SplitOffsetTuple key1 = (SplitOffsetTuple) a;
			SplitOffsetTuple key2 = (SplitOffsetTuple) b;
			return key1.getSplit() == key2.getSplit() ? 0 : (key1.getSplit() > key2.getSplit() ? 1
					: -1);
		}
	}

	public static class SequencerInputFormat extends TextInputFormat
	{

		@Override
		public List<InputSplit> getSplits(JobContext arg0) throws IOException
		{
			List<InputSplit> splits = super.getSplits(arg0);
			List<InputSplit> result = new ArrayList<InputSplit>();
			int total = splits.size();
			int counter = 0;
			for (InputSplit split : splits)
			{
				counter++;
				result.add(new SequencerFileSplit((FileSplit) split, counter, total));
			}
			return result;
		}
	}

	public static class SequencerFileSplit extends FileSplit
	{
		private int totalSplits;
		private int splitId;

		SequencerFileSplit()
		{
			super();
		}

		SequencerFileSplit(FileSplit split, int splitId, int totalSplits) throws IOException
		{
			super(split.getPath(), split.getStart(), split.getLength(), split.getLocations());
			this.splitId = splitId;
			this.totalSplits = totalSplits;
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			super.readFields(in);
			splitId = in.readInt();
			totalSplits = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			super.write(out);
			out.writeInt(splitId);
			out.writeInt(totalSplits);
		}

		public int getTotalSplits()
		{
			return totalSplits;
		}

		public void setTotalSplits(int totalSplits)
		{
			this.totalSplits = totalSplits;
		}

		public int getSplitId()
		{
			return splitId;
		}

		public void setSplitId(int splitId)
		{
			this.splitId = splitId;
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.out.println("Invalid number of arguments\n\n"
					+ "Usage: sequencer <input_path> <output_path>\n\n");
			return -1;
		}
		String input = args[0];
		String output = args[1];

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);
		Job job = Job.getInstance(getConf(), "Sequencer Hadoop");
		job.setJarByClass(SequencerJob.class);
		job.setInputFormatClass(SequencerInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(SplitOffsetTuple.class);
		job.setMapOutputValueClass(OffsetLineTuple.class);
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SequencerMap.class);
		job.setReducerClass(SequencerReducer.class);
		job.setPartitionerClass(SequencerPartitioner.class);
		job.setGroupingComparatorClass(SequencerGroupingComparator.class);
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.getConfiguration().setInt("splitNumber", new TextInputFormat().getSplits(job).size());
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new SequencerJob(), args);
	}
}
