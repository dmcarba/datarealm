package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.CompositeKey;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.DistributedKey;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;
import com.thedatarealm.mapreduce.coherence.MapReduce.WriterService;

@SuppressWarnings("serial")
public class CombinerProcessor<K extends Comparable<K>, V> extends AbstractProcessor
{
	@SuppressWarnings("rawtypes")
	private Reducer reducer;
	private String output;
	private CompositeKey currentKey, oldKey;
	private List<V> currentValues = new ArrayList<V>();
	private long count;

	public CombinerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		this.reducer = reducer;
		this.output = output;
	}

	@SuppressWarnings(
	{ "rawtypes", "unchecked" })
	@Override
	public Map processAll(Set arg0)
	{
		SortedSet ss = new TreeSet(arg0);
		for (Iterator iter = ss.iterator(); iter.hasNext();)
		{
			InvocableMap.Entry entry = (InvocableMap.Entry) iter.next();
			process(entry);
		}
		shuffleAndStore();
		return null;
	}
	
	private void shuffleAndStore()
	{
		final List<OrderedKeyValue<K, V>> combinedPairs = reducer.reduce(((SequencedKey)oldKey.getSecondaryKey()).key,
				currentValues);
		final int memberId = CacheFactory.getCache(output).getCacheService().getCluster()
				.getLocalMember().getId();
		Map<Member, List<OrderedKeyValue<DistributedKey<K>, V>>> keysToMembers = new HashMap<>();
		// For each pair get the partition it is going to be allocated
		for (OrderedKeyValue<K, V> kv : combinedPairs)
		{
			DistributedKey<K> key = new DistributedKey<K>(kv.getKey(), memberId, count++);
			Member member = ((DistributedCacheService) CacheFactory.getCache(output)
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
					new WriterService<DistributedKey<K>, V>(entryList.getValue(), output),
					Collections.singleton(entryList.getKey()));
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public Object process(final Entry paramEntry)
	{
		BackingMapManagerContext currentContext = ((BinaryEntry) paramEntry)
				.getBackingMapContext().getManagerContext();
		final int memberId = currentContext.getCacheService().getCluster().getLocalMember()
				.getId();
		currentKey = (CompositeKey) paramEntry.getKey();
		if (oldKey != null && !currentKey.equals(oldKey))
		{
			shuffleAndStore();
			currentValues.clear();
		}
		currentValues.add((V) paramEntry.getValue());
		oldKey = currentKey;
		System.out.println("Member: " + memberId + ",Entry: " + paramEntry.getKey());
		return null;
	}
	
	private static BinaryKeyComparator BKC = new BinaryKeyComparator();
	
	private static class BinaryKeyComparator implements Comparator<BinaryEntry> {

		@Override
		public int compare(BinaryEntry o1, BinaryEntry o2) {
			return o1.getBinaryKey().compareTo(o2.getBinaryKey());
		}		
	}
}
