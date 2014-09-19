
package com.thedatarealm.sequencer;

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

public class SequencerJob extends Configured implements Tool
{

	private static final String TOTAL_SPLIT_NUMBER = "totalSplitNumber";

	public static class SequencerMap extends
			Mapper<LongWritable, Text, SplitSequenceWritable, SequenceLineWritable>
	{
		private SplitSequenceWritable currentSplitSequence;
		private SequenceLineWritable currentSequenceLine;
		private int totalSplitCount;
		private int splitId;
		private long lineCount;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
			currentSequenceLine.setLine(String.valueOf(lineCount));
			for (int i = splitId; i < totalSplitCount; i++)
			{
				currentSplitSequence.setSplit(i + 1);
				currentSplitSequence.setSequence(-splitId);
				currentSequenceLine.setSequence(-splitId);
				context.write(currentSplitSequence, currentSequenceLine);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			lineCount = 0;
			totalSplitCount = ((SequencerFileSplit) context.getInputSplit()).getTotalSplits();
			splitId = ((SequencerFileSplit) context.getInputSplit()).getSplitId();
			currentSplitSequence = new SplitSequenceWritable(splitId, 0);
			currentSequenceLine = new SequenceLineWritable();
		}

		@Override
		protected void map(LongWritable offset, Text line, Context context) throws IOException,
				InterruptedException
		{
			currentSplitSequence.setSequence(lineCount);
			currentSequenceLine.setLine(line.toString());
			currentSequenceLine.setSequence(lineCount);
			context.write(currentSplitSequence, currentSequenceLine);
			lineCount++;
		};
	}

	public static class SequencerReducer extends
			Reducer<SplitSequenceWritable, SequenceLineWritable, LongWritable, Text>
	{
		private LongWritable sequence = new LongWritable();
		private Text line = new Text();

		protected void reduce(SplitSequenceWritable key, Iterable<SequenceLineWritable> tuples,
				Context context) throws IOException, InterruptedException
		{
			Iterator<SequenceLineWritable> iterator = tuples.iterator();
			long sequenceNumber = 0;
			SequenceLineWritable tuple = iterator.next();
			while (iterator.hasNext() && tuple.getSequence() < 0)
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

	public static class SequencerPartitioner extends Partitioner<SplitSequenceWritable, SequenceLineWritable>
			implements Configurable
	{

		private Configuration conf;
		private int totalSplitNumber;
		
		@Override
		public int getPartition(SplitSequenceWritable key, SequenceLineWritable value, int numPartitions)
		{	
			return ((key.getSplit() - 1) * numPartitions) / totalSplitNumber;
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
			totalSplitNumber = conf.getInt(TOTAL_SPLIT_NUMBER, 0);
		}
	}

	public static class SequencerGroupingComparator extends WritableComparator
	{
		protected SequencerGroupingComparator()
		{
			super(SplitSequenceWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b)
		{
			SplitSequenceWritable key1 = (SplitSequenceWritable) a;
			SplitSequenceWritable key2 = (SplitSequenceWritable) b;
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
			int total = splits.size();
			List<InputSplit> result = new ArrayList<InputSplit>(total);
			int counter = 1;
			for (InputSplit split : splits)
			{
				result.add(new SequencerFileSplit((FileSplit) split, counter++, total));
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
		job.setMapOutputKeyClass(SplitSequenceWritable.class);
		job.setMapOutputValueClass(SequenceLineWritable.class);
		job.setOutputKeyClass(Long.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SequencerMap.class);
		job.setReducerClass(SequencerReducer.class);
		job.setPartitionerClass(SequencerPartitioner.class);
		job.setGroupingComparatorClass(SequencerGroupingComparator.class);
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.getConfiguration().setInt(TOTAL_SPLIT_NUMBER, new TextInputFormat().getSplits(job).size());
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new SequencerJob(), args);
	}
}
