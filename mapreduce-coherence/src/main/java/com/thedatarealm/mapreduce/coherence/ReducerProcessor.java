package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.DistributedKey;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class ReducerProcessor<K extends Comparable<K>, V> extends AbstractProcessor
{
	@SuppressWarnings("rawtypes")
	private Reducer reducer;
	private String output;
	private DistributedKey<K> currentKey, oldKey;
	private List<V> currentValues = new ArrayList<V>();

	public ReducerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		this.reducer = reducer;
		this.output = output;
	}

	@SuppressWarnings(
	{ "rawtypes", "unchecked" })
	@Override
	public Map processAll(Set arg0)
	{
		final BackingMapContext context = getContext(arg0);
		final Set<Map.Entry<K, Set>> entries = getIndexedValues(context);
		Map bMap = context.getBackingMap();
		for (Map.Entry<K, Set> entry : entries)
		{
			currentValues = new ArrayList<>();
			for (Object o:entry.getValue())
			{
				currentValues.add((V) ExternalizableHelper.CONVERTER_FROM_BINARY.convert(bMap.get(o)));			
			}
			store(entry.getKey(), currentValues);
		}
		return null;
	}

	private void store()
	{
		final List<OrderedKeyValue<K, V>> reducedPairs = reducer.reduce(oldKey.getAssociatedKey(),
				currentValues);
		for (OrderedKeyValue<K, V> pair : reducedPairs)
		{
			CacheFactory.getCache(output).put(pair.getKey(), pair.getValue());
		}
	}
	
	private void store(K key, List<V> values)
	{
		final List<OrderedKeyValue<K, V>> reducedPairs = reducer.reduce(key,
				values);
		for (OrderedKeyValue<K, V> pair : reducedPairs)
		{
			CacheFactory.getCache(output).put(pair.getKey(), pair.getValue());
		}
	}

	private static BackingMapContext getContext(Set set)
	{
		final Object entry = set.iterator().next();
		if (!(entry instanceof BinaryEntry))
		{
			throw new UnsupportedOperationException("Only supports binary caches");
		}

		final BinaryEntry binaryEntry = (BinaryEntry) entry;
		return binaryEntry.getBackingMapContext();
	}

	private Set getIndexedValues(BackingMapContext context)
	{
		final MapIndex index = (MapIndex) context.getIndexMap().get(MapReduce.KEY_EXTRACTOR);
		final Map contents = index.getIndexContents();
		return contents == null ? Collections.emptySet() : contents.entrySet();
	}
	
	public void process(final V value)
	{
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object process(final Entry paramEntry)
	{
		currentKey = (DistributedKey<K>) paramEntry.getKey();
		if (oldKey != null && !currentKey.equals(oldKey))
		{
			store();
			currentValues.clear();
		}
		currentValues.add((V) paramEntry.getValue());
		oldKey = currentKey;
		return null;
	}
}
