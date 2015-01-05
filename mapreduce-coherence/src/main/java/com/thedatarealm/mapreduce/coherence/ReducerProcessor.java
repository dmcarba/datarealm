package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class ReducerProcessor<K extends Comparable<K>, V> extends AbstractProcessor
{
	@SuppressWarnings("rawtypes")
	protected Reducer reducer;
	protected String output;
	protected List<V> currentValues = new ArrayList<V>();
	private long count;

	public ReducerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		this.reducer = reducer;
		this.output = output;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map processAll(Set arg0)
	{
		final BackingMapContext context = getContext(arg0);
		final Set<Map.Entry<K, Set<Binary>>> entries = getIndexedValues(context);
		Map<Binary, Binary> bMap = context.getBackingMap();
		Converter converter = context.getManagerContext().getKeyFromInternalConverter();
		for (Map.Entry<K, Set<Binary>> entry : entries)
		{
			currentValues = new ArrayList<>();
			for (Object o:entry.getValue())
			{
				currentValues.add((V) converter.convert(bMap.get(o)));			
			}
			store(entry.getKey(), currentValues);
			for (Object o:entry.getValue())
			{
				bMap.remove(o);			
			}
		}
		return null;
	}
	
	protected void store(K key, List<V> values)
	{
		final List<OrderedKeyValue<K, V>> combinedPairs = reducer.reduce(key,
				currentValues);
		final int memberId = CacheFactory.getCache(output).getCacheService().getCluster()
				.getLocalMember().getId();
		Map<Member, List<OrderedKeyValue<NodeAwareKey<K>, V>>> keysToMembers = new HashMap<>();
		// For each pair get the partition it is going to be allocated
		for (OrderedKeyValue<K, V> kv : combinedPairs)
		{
			NodeAwareKey<K> currentKey = new NodeAwareKey<K>(kv.getKey(), memberId, count++);
			Member member = ((DistributedCacheService) CacheFactory.getCache(output)
					.getCacheService()).getKeyOwner(currentKey);
			if (!keysToMembers.containsKey(member))
			{
				keysToMembers.put(member, new ArrayList<OrderedKeyValue<NodeAwareKey<K>, V>>());
			}
			keysToMembers.get(member).add(
					new OrderedKeyValue<NodeAwareKey<K>, V>(currentKey, kv.getValue()));
		}
		for (Map.Entry<Member, List<OrderedKeyValue<NodeAwareKey<K>, V>>> entryList : keysToMembers
				.entrySet())
		{
			((InvocationService) CacheFactory.getService("Shuffle")).query(
					new WriterService<NodeAwareKey<K>, V>(entryList.getValue(), output),
					Collections.singleton(entryList.getKey()));
		}
	}

	private BackingMapContext getContext(Set<BinaryEntry> set)
	{
		return set.iterator().next().getBackingMapContext();
	}

	private Set<Map.Entry<K, Set<Binary>>> getIndexedValues(BackingMapContext context)
	{
		final MapIndex index = (MapIndex) context.getIndexMap().get(MapReduce.KEY_EXTRACTOR);
		final Map<K,Set<Binary>> contents = index.getIndexContents();
		return contents.entrySet();
	}

	@Override
	public Object process(final Entry paramEntry)
	{
		return null;
	}
}
