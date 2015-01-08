package com.thedatarealm.mapreduce.coherence;

import com.tangosol.net.BackingMapContext;

public class IntermediateContext<K extends Comparable<K>, V> extends JobContext<K,V>
{
	private Comparable<?> sourceKey;
	
	@Override
	public void setSourceKey(Comparable<?> sourceKey)
	{
		this.sourceKey = sourceKey;
	}

	public IntermediateContext(BackingMapContext bmctx,String output)
	{
		super(bmctx, output);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Object getKey(K key)
	{
		return new CompositeKey(key, sourceKey,	count++);
	}

}
