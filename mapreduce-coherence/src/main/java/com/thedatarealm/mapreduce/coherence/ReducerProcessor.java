package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Converter;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.MapIndex;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class ReducerProcessor<K extends Comparable<K>, V> extends AbstractProcessor implements
		PortableObject
{
	@SuppressWarnings("rawtypes")
	protected Reducer reducer;
	protected String output;
	private Context<K, V> context;

	public ReducerProcessor()
	{
	}

	public ReducerProcessor(String output, Reducer<K, V, ?, ?> reducer)
	{
		this.reducer = reducer;
		this.output = output;
	}

	@Override
	public Map processAll(Set arg0)
	{
		if (arg0.size() == 0)
		{
			return null;
		}
		final BackingMapContext bmctx = ((BinaryEntry) arg0.iterator().next())
				.getBackingMapContext();
		this.context = new Context<>(bmctx, output, output, false);
		final Set<Map.Entry<K, Set<Binary>>> entries = getIndexedValues(bmctx);
		Map<Binary, Binary> bMap = bmctx.getBackingMap();
		Converter converter = bmctx.getManagerContext().getKeyFromInternalConverter();
		for (Map.Entry<K, Set<Binary>> entry : entries)
		{
			reducer.reduce(entry.getKey(), new ValuesIterator<>(entry.getValue(), converter, bMap),
					context);
		}
		context.flush();
		return null;
	}

	private Set<Map.Entry<K, Set<Binary>>> getIndexedValues(BackingMapContext context)
	{
		return ((MapIndex) context.getIndexMap().get(MapReduce.KEY_EXTRACTOR)).getIndexContents()
				.entrySet();
	}

	@Override
	public Object process(final Entry paramEntry)
	{
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(PofReader paramPofReader) throws IOException
	{
		output = paramPofReader.readString(0);
		reducer = (Reducer<K, V, ?, ?>) paramPofReader.readObject(1);
	}

	@Override
	public void writeExternal(PofWriter paramPofWriter) throws IOException
	{
		paramPofWriter.writeString(0, output);
		paramPofWriter.writeObject(1, reducer);
	}
}
