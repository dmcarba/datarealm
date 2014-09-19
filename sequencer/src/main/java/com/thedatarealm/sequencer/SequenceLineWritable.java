package com.thedatarealm.sequencer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SequenceLineWritable implements Writable
{
	private long sequence;
	private String line;
	
	public SequenceLineWritable()
	{
		
	}
	
	public SequenceLineWritable(long sequence, String line)
	{
		this.sequence = sequence;
		this.line = line;
	}
	
	public long getSequence()
	{
		return sequence;
	}

	public void setSequence(long sequence)
	{
		this.sequence = sequence;
	}

	public String getLine()
	{
		return line;
	}

	public void setLine(String line)
	{
		this.line = line;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		sequence = WritableUtils.readVLong(in);
		line = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeVLong(out, sequence);
		Text.writeString(out, line);
	}

}
