package com.dcm.datarealm.sequencer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class OffsetTextTuple implements Writable
{
	private long offset;
	private String line;
	
	public OffsetTextTuple()
	{
		
	}
	
	public OffsetTextTuple(long offset, String line)
	{
		this.offset = offset;
		this.line = line;
	}
	
	public long getOffset()
	{
		return offset;
	}

	public void setOffset(long offset)
	{
		this.offset = offset;
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
		offset = WritableUtils.readVLong(in);
		line = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeVLong(out, offset);
		Text.writeString(out, line);
	}

}
