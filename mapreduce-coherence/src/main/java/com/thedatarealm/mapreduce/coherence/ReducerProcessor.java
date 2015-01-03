package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.extractor.KeyExtractor;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class ReducerProcessor<K extends Comparable<K>, V> extends AbstractProcessor
{
	@SuppressWarnings("rawtypes")
	protected Reducer reducer;
	protected String output;
	protected List<V> currentValues = new ArrayList<V>();

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
		for (Map.Entry<K, Set<Binary>> entry : entries)
		{
			currentValues = new ArrayList<>();
			for (Object o:entry.getValue())
			{
				currentValues.add((V) ExternalizableHelper.CONVERTER_FROM_BINARY.convert(bMap.get(o)));			
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
		final List<OrderedKeyValue<K, V>> reducedPairs = reducer.reduce(key,
				values);
		for (OrderedKeyValue<K, V> pair : reducedPairs)
		{
			CacheFactory.getCache(output).put(pair.getKey(), pair.getValue());
		}
	}

	private BackingMapContext getContext(Set<BinaryEntry> set)
	{
		return set.iterator().next().getBackingMapContext();
	}

	private Set<Map.Entry<K, Set<Binary>>> getIndexedValues(BackingMapContext context)
	{
		final MapIndex index = (MapIndex) context.getIndexMap().get(getKeyExtractor());
		final Map<K,Set<Binary>> contents = index.getIndexContents();
		return contents.entrySet();
	}
	
	protected KeyExtractor getKeyExtractor()
	{
		return MapReduce.KEY_EXTRACTOR;
	}

	@Override
	public Object process(final Entry paramEntry)
	{
		return null;
	}
}
