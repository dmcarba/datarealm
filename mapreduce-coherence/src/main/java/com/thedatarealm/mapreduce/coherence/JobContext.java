package com.thedatarealm.mapreduce.coherence;

import java.util.HashMap;
import java.util.Map;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;

public class JobContext<K extends Comparable<K>, V> implements Context<K, V>
{

	private final int memberId;
	protected int count;
	private final String output;
	private final static int BUFFER_SIZE = 1000;
	private Map<Object, Object> values;

	public JobContext(BackingMapContext bmctx, String output)
	{
		this.memberId = bmctx.getManagerContext().getCacheService().getCluster().getLocalMember()
				.getId();
		this.output = output;
		this.values = new HashMap<>();
	}

	public void flush()
	{
		store(output);
		values.clear();
	}

	protected Object getKey(K key)
	{
		return new NodeAwareKey<K>(key, memberId, count++);
	}

	public void write(K key, V value)
	{
		if (BUFFER_SIZE == values.size())
		{
			store(output);
			values.clear();
		}
		values.put(getKey(key), value);
	}

	private void store(String target)
	{
		CacheFactory.getCache(target).putAll(values);
	}
	
	public void setSourceKey(Comparable<?> sourceKey)
	{
	}


}
