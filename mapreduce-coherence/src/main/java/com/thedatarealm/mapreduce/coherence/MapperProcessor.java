package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.DistributedKey;
import com.thedatarealm.mapreduce.coherence.MapReduce.CompositeKey;
import com.thedatarealm.mapreduce.coherence.MapReduce.Mapper;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;
import com.thedatarealm.mapreduce.coherence.MapReduce.WriterService;

@SuppressWarnings("serial")
public class MapperProcessor<K extends Comparable<K>, V> extends AbstractProcessor
{
	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	@SuppressWarnings("rawtypes")
	private Reducer combiner;
	private String staging, output;
	private long count;
	InvocationService writerService;

	public MapperProcessor(String staging, String output, Mapper<?, ?, K, V> mapper)
	{
		this.staging = staging;
		this.output = output;
		this.mapper = mapper;
	}

	public MapperProcessor(String staging, String output, Mapper<?, ?, K, V> mapper, Reducer<K, V, K, V> combiner)
	{
		this(staging, output, mapper);
		this.combiner = combiner;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map processAll(Set arg0)
	{
		for (Iterator iter = arg0.iterator(); iter.hasNext();)
		{
			InvocableMap.Entry entry = (InvocableMap.Entry) iter.next();
			process(entry);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object process(final Entry paramEntry)
	{
		final List<OrderedKeyValue<K, V>> emittedPairs = mapper.map(
				(Comparable<?>) paramEntry.getKey(), paramEntry.getValue());
		if (combiner != null)
		{
			store(emittedPairs, (K) paramEntry.getKey());
		}
		else
		{
			shuffleAndStore(emittedPairs);
		}
		return null;
	}

	private void store(final List<OrderedKeyValue<K, V>> emittedPairs, Comparable<?> sourceKey)
	{
		List<OrderedKeyValue<CompositeKey<? extends Comparable<?>, K>, V>> values = new ArrayList<>();
		for (OrderedKeyValue<K, V> kv : emittedPairs)
		{
			@SuppressWarnings(
			{ "unchecked", "rawtypes" })
			CompositeKey<? extends Comparable<?>, K> key = new CompositeKey(kv.getKey(), sourceKey, count++);
			values.add(new OrderedKeyValue<CompositeKey<? extends Comparable<?>, K>, V>(key, kv
					.getValue()));
		}
		//Write to local node
		((InvocationService) CacheFactory.getService("Shuffle")).query(
				new WriterService<CompositeKey<? extends Comparable<?>, K>, V>(values, output),
				Collections.singleton(CacheFactory.getCache(output).getCacheService().getCluster()
						.getLocalMember()));
	}

	private void shuffleAndStore(final List<OrderedKeyValue<K, V>> emittedPairs)
	{
		final int memberId = CacheFactory.getCache(staging).getCacheService().getCluster()
				.getLocalMember().getId();
		Map<Member, List<OrderedKeyValue<DistributedKey<K>, V>>> keysToMembers = new HashMap<>();
		// For each pair get the partition it is going to be allocated
		for (OrderedKeyValue<K, V> kv : emittedPairs)
		{
			DistributedKey<K> key = new DistributedKey<K>(kv.getKey(), memberId, count++);
			Member member = ((DistributedCacheService) CacheFactory.getCache(staging)
					.getCacheService()).getKeyOwner(key);
			if (!keysToMembers.containsKey(member))
			{
				keysToMembers.put(member, new ArrayList<OrderedKeyValue<DistributedKey<K>, V>>());
			}
			keysToMembers.get(member).add(
					new OrderedKeyValue<DistributedKey<K>, V>(key, kv.getValue()));
		}
		for (Map.Entry<Member, List<OrderedKeyValue<DistributedKey<K>, V>>> entryList : keysToMembers
				.entrySet())
		{
			((InvocationService) CacheFactory.getService("Shuffle")).query(
					new WriterService<DistributedKey<K>, V>(entryList.getValue(), staging),
					Collections.singleton(entryList.getKey()));
		}
	}
}
