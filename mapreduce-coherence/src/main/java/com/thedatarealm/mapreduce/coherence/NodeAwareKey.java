package com.thedatarealm.mapreduce.coherence;

@SuppressWarnings("serial")
public class NodeAwareKey<K extends Comparable<K>> extends CompositeKey<K, Integer> 
{
	public NodeAwareKey()
	{
	}

	public NodeAwareKey(K key, int nodeId, long sequence)
	{
		super(key, nodeId, sequence);
	}

	@Override
	public Object getAssociatedKey()
	{
		return key1;
	}
}
