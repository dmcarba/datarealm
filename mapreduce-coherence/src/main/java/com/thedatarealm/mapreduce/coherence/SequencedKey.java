package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;

@SuppressWarnings("serial")
public class SequencedKey<K extends Comparable<K>> implements Serializable , Comparable<SequencedKey<K>>
{
	K key;
	long sequence;
	
	public SequencedKey(K key, long sequence)
	{
		this.key = key;
		this.sequence = sequence;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SequencedKey other = (SequencedKey) obj;
		if (key == null)
		{
			if (other.key != null)
				return false;
		}
		else if (!key.equals(other.key))
			return false;
		return true;
	}



	@Override
	public String toString()
	{
		return "SequencedKey [key=" + key + ", sequence=" + sequence + "]";
	}

	@Override
	public int compareTo(SequencedKey<K> o)
	{
		return key.compareTo(o.key);
	}
	
}
