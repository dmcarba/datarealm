package com.dcm.datarealm.sequencer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class SplitOffsetTuple implements WritableComparable<SplitOffsetTuple>
{
	private int split;
	private long offset;

	public SplitOffsetTuple()
	{
	}

	public SplitOffsetTuple(int splitNumber, long offset)
	{
		this.split = splitNumber;
		this.offset = offset;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeVLong(out, split);
		WritableUtils.writeVLong(out, offset);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		split = WritableUtils.readVInt(in);
		offset = WritableUtils.readVLong(in);
	}

	public int getSplit()
	{
		return split;
	}

	public void setSplit(int split)
	{
		this.split = split;
	}

	public long getOffset()
	{
		return offset;
	}

	public void setOffset(long offset)
	{
		this.offset = offset;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (offset ^ (offset >>> 32));
		result = prime * result + (int) (split ^ (split >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof SplitOffsetTuple))
			return false;
		SplitOffsetTuple other = (SplitOffsetTuple) obj;
		return split == other.split && offset == other.offset;
	}

	@Override
	public int compareTo(SplitOffsetTuple o)
	{
		int result = o.split == split ? 0 : (o.split < split ? 1 : -1);
		result = (result == 0) ? (o.offset == offset ? 0 : (o.offset < offset ? 1 : -1)) : result;
		return result;
	}

	public static class Comparator extends WritableComparator
	{

		public Comparator()
		{
			super(SplitOffsetTuple.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				int result = 0;
				int pos1 = WritableUtils.decodeVIntSize(b1[s1]);
				int pos2 = WritableUtils.decodeVIntSize(b2[s2]);
				int split1 = WritableComparator.readVInt(b1, s1);
				int split2 = WritableComparator.readVInt(b2, s2);
				result = split2 == split1 ? 0 : (split2 < split1 ? 1 : -1);
				if (result != 0)
				{
					return result;
				}
				long offset1 = WritableComparator.readVLong(b1, s1 + pos1);
				long offset2 = WritableComparator.readVLong(b2, s2 + pos2);
				result = offset2 == offset1 ? 0 : (offset2 < offset1 ? 1 : -1);
				return result;
			}
			catch (Exception ex)
			{
				throw new IllegalArgumentException(ex);
			}
		}
	}

	static
	{
		WritableComparator.define(SplitOffsetTuple.class, new Comparator());
	}

}
