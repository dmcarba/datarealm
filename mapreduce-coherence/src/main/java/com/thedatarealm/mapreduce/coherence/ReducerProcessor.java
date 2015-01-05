package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tangosol.net.BackingMapContext;
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
	private Context<K,V> context;

	public ReducerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		this.reducer = reducer;
		this.output = output;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map processAll(Set arg0)
	{
		if (arg0.size() == 0)
		{
			return null;
		}
		final BackingMapContext bmctx = getContext(arg0);
		this.context = new Context<>(bmctx, output, output, false);
		final Set<Map.Entry<K, Set<Binary>>> entries = getIndexedValues(bmctx);
		Map<Binary, Binary> bMap = bmctx.getBackingMap();
		Converter converter = bmctx.getManagerContext().getKeyFromInternalConverter();
		for (Map.Entry<K, Set<Binary>> entry : entries)
		{
			currentValues = new ArrayList<>();
			for (Object o:entry.getValue())
			{
				currentValues.add((V) converter.convert(bMap.get(o)));			
			}
			reducer.reduce(entry.getKey(), currentValues, context);
			for (Object o:entry.getValue())
			{
				bMap.remove(o);			
			}
		}
		context.flush();
		return null;
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
