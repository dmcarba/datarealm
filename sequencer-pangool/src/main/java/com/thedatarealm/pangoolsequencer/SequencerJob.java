package com.thedatarealm.pangoolsequencer;

import static com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat.NO_ESCAPE_CHARACTER;
import static com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat.NO_QUOTE_CHARACTER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Mutator;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TaggedInputSplit;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;

public class SequencerJob extends Configured implements Tool
{

	private static final String TOTAL_SPLIT_NUMBER = "totalSplitNumber";
	private static final String TOTAL_FIELD = "totalValue";
	private static final String LINE_FIELD = "line";
	private static final String SEQ_FIELD = "sequence";
	private static final String SPLIT_FIELD = "splitid";

	private static final Schema INTERMEDIATE_SCHEMA = new Schema(
			"schema",
			Arrays.asList(new Field[] { Field.create(SPLIT_FIELD, Type.INT),
					Field.create(LINE_FIELD, Type.STRING, true),
					Field.create(SEQ_FIELD, Type.LONG), Field.create(TOTAL_FIELD, Type.LONG, true), }));

	private static final Schema RESULT_SCHEMA = Mutator.subSetOf("mutated", INTERMEDIATE_SCHEMA,
			SEQ_FIELD, LINE_FIELD);

	@SuppressWarnings("serial")
	public static class SequencerMap extends TupleMapper<LongWritable, Text>
	{
		private int totalSplitCount;
		private int splitId;
		private long lineCount;
		private transient ITuple tuple;
		private static final long TOTAL_SEQUENCE_ID = -1;

		@Override
		public void cleanup(TupleMRContext context, Collector collector) throws IOException,
				InterruptedException
		{
			tuple.set(TOTAL_FIELD, lineCount);
			tuple.set(LINE_FIELD, null);
			tuple.set(SEQ_FIELD, TOTAL_SEQUENCE_ID);
			for (int i = splitId; i < totalSplitCount; i++)
			{
				tuple.set(SPLIT_FIELD, i + 1);
				collector.write(tuple);
			}
			super.cleanup(context, collector);
		}

		@Override
		public void setup(TupleMRContext context, Collector collector) throws IOException,
				InterruptedException
		{
			super.setup(context, collector);
			lineCount = 0;
			tuple = new Tuple(INTERMEDIATE_SCHEMA);
			SequencerFileSplit split = (SequencerFileSplit) ((TaggedInputSplit) context
					.getHadoopContext().getInputSplit()).getInputSplit();
			totalSplitCount = split.getTotalSplits();
			splitId = split.getSplitId();
			tuple.set(TOTAL_FIELD, null);
			tuple.set(SPLIT_FIELD, splitId);
		}

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
				throws IOException, InterruptedException
		{
			tuple.set(SEQ_FIELD, lineCount);
			tuple.set(LINE_FIELD, value);
			collector.write(tuple);
			lineCount++;
		}
	}

	@SuppressWarnings("serial")
	public static class SequencerReducer extends TupleReducer<ITuple, NullWritable>
	{

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
				Collector collector) throws IOException, InterruptedException, TupleMRException
		{
			ITuple result = new Tuple(RESULT_SCHEMA);
			Iterator<ITuple> iterator = tuples.iterator();
			long sequenceNumber = 0;
			ITuple value = iterator.next();
			while (iterator.hasNext() && value.getLong(TOTAL_FIELD) != null)
			{
				sequenceNumber += Long.valueOf(value.getLong(TOTAL_FIELD));
				value = iterator.next();
			}
			result.set(SEQ_FIELD, sequenceNumber);
			result.set(LINE_FIELD, value.getString(LINE_FIELD));
			collector.write(result, NullWritable.get());
			sequenceNumber++;
			while (iterator.hasNext())
			{
				value = iterator.next();
				result.set(SEQ_FIELD, sequenceNumber);
				result.set(LINE_FIELD, value.getString(LINE_FIELD));
				collector.write(result, NullWritable.get());
				sequenceNumber++;
			}
		}
	}

	public static class SequencerPartitioner extends
			Partitioner<DatumWrapper<ITuple>, NullWritable> implements Configurable
	{
		private Configuration conf;
		private int totalSplitNumber;

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

		@Override
		public int getPartition(DatumWrapper<ITuple> key, NullWritable value, int numPartitions)
		{
			return ((key.datum().getInteger(SPLIT_FIELD) - 1) * numPartitions) / totalSplitNumber;
		}
	}

	// Note that Pangool will distribute the input format as a Serializable
	// object
	@SuppressWarnings("serial")
	public static class SequencerInputFormat extends TextInputFormat implements Serializable
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

		TupleMRBuilder mr = new TupleMRBuilder(getConf(), "Sequencer Pangool");

		mr.addInput(new Path(input), new SequencerInputFormat(), new SequencerMap());
		mr.addIntermediateSchema(INTERMEDIATE_SCHEMA);
		mr.setTupleReducer(new SequencerReducer());
		mr.setGroupByFields(SPLIT_FIELD);
		mr.setOrderBy(new OrderBy().add(SPLIT_FIELD, Order.ASC).add(SEQ_FIELD, Order.ASC));
		mr.setOutput(new Path(output), new TupleTextOutputFormat(RESULT_SCHEMA, false, '\t',
				NO_QUOTE_CHARACTER, NO_ESCAPE_CHARACTER), ITuple.class, NullWritable.class);
		Job job = mr.createJob();
		job.setNumReduceTasks(4);
		job.setPartitionerClass(SequencerPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(input));
		job.getConfiguration().setInt(TOTAL_SPLIT_NUMBER,
				new TextInputFormat().getSplits(job).size());
		job.waitForCompletion(true);
		mr.cleanUpInstanceFiles();
		return 0;
	}

	public static void main(String args[]) throws Exception
	{
		ToolRunner.run(new SequencerJob(), args);
	}
}
