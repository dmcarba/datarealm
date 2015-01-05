package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;

import com.tangosol.net.cache.KeyAssociation;

@SuppressWarnings("serial")
public class CompositeKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements
		KeyAssociation, Serializable, Comparable<CompositeKey<K1, K2>>
{
	protected K1 key1;
	protected K2 key2;
	protected long sequence;

	public CompositeKey()
	{
	}

	public CompositeKey(K1 key1, K2 key2, long sequence)
	{
		this.key1 = key1;
		this.key2 = key2;
		this.sequence = sequence;
	}

	public K1 getKey1()
	{
		return key1;
	}

	public K2 getKey2()
	{
		return key2;
	}

	@Override
	public Object getAssociatedKey()
	{
		return key2;
	}

	@Override
	public int compareTo(CompositeKey<K1, K2> o)
	{
		return key1.compareTo(o.key1);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key1 == null) ? 0 : key1.hashCode());
		result = prime * result + ((key2 == null) ? 0 : key2.hashCode());
		result = prime * result + (int) (sequence ^ (sequence >>> 32));
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj)
	{
		return key1.equals(((CompositeKey<K1, K2>) obj).key1);
	}

	@Override
	public String toString()
	{
		return "CompositeKey [key1=" + key1 + ", key2=" + key2 + ", sequence=" + sequence + "]";
	}

}