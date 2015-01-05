package com.thedatarealm.mapreduce.coherence;

import java.util.List;

import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

@SuppressWarnings("serial")
public class WriterService<K extends Comparable<?>, V> extends AbstractInvocable
{
	protected List<OrderedKeyValue<K, V>> entryList;
	private String targetCache;

	public WriterService(List<OrderedKeyValue<K, V>> entryList, String targetCache)
	{
		this.entryList = entryList;
		this.targetCache = targetCache;
	}

	@Override
	public void run()
	{
		NamedCache cache = CacheFactory.getCache(targetCache);
		for (OrderedKeyValue<K, V> entry : entryList)
		{
			cache.put(entry.getKey(), entry.getValue());
		}
	}
}