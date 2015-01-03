package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.util.extractor.KeyExtractor;
import com.thedatarealm.mapreduce.coherence.MapReduce.DistributedKey;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;
import com.thedatarealm.mapreduce.coherence.MapReduce.WriterService;

@SuppressWarnings("serial")
public class CombinerProcessor<K extends Comparable<K>, V> extends ReducerProcessor<K, V>
{

	private long count;

	public CombinerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		super(output,reducer);
	}

	@Override
	protected void store(K key, List<V> values)
	{
		final List<OrderedKeyValue<K, V>> combinedPairs = reducer.reduce(key,
				currentValues);
		final int memberId = CacheFactory.getCache(output).getCacheService().getCluster()
				.getLocalMember().getId();
		Map<Member, List<OrderedKeyValue<DistributedKey<K>, V>>> keysToMembers = new HashMap<>();
		// For each pair get the partition it is going to be allocated
		for (OrderedKeyValue<K, V> kv : combinedPairs)
		{
			DistributedKey<K> currentKey = new DistributedKey<K>(kv.getKey(), memberId, count++);
			Member member = ((DistributedCacheService) CacheFactory.getCache(output)
					.getCacheService()).getKeyOwner(currentKey);
			if (!keysToMembers.containsKey(member))
			{
				keysToMembers.put(member, new ArrayList<OrderedKeyValue<DistributedKey<K>, V>>());
			}
			keysToMembers.get(member).add(
					new OrderedKeyValue<DistributedKey<K>, V>(currentKey, kv.getValue()));
		}
		for (Map.Entry<Member, List<OrderedKeyValue<DistributedKey<K>, V>>> entryList : keysToMembers
				.entrySet())
		{
			((InvocationService) CacheFactory.getService("Shuffle")).query(
					new WriterService<DistributedKey<K>, V>(entryList.getValue(), output),
					Collections.singleton(entryList.getKey()));
		}
	}
	
	@Override
	protected KeyExtractor getKeyExtractor()
	{
		return MapReduce.KEY_EXTRACTOR2;
	}
}
