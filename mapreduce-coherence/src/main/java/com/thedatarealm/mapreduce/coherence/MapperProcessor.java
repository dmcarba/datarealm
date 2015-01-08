package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.BackingMapContext;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.Mapper;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class MapperProcessor<K extends Comparable<K>, V> extends AbstractProcessor implements PortableObject
{
	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	@SuppressWarnings("rawtypes")
	private Reducer combiner;
	private String staging, output;
	private Context<K,V> context;
	
	public MapperProcessor()
	{
		
	}

	public MapperProcessor(String staging, String output, Mapper<?, ?, K, V> mapper)
	{
		this.staging = staging;
		this.output = output;
		this.mapper = mapper;
	}

	public MapperProcessor(String staging, String output, Mapper<?, ?, K, V> mapper,
			Reducer<K, V, K, V> combiner)
	{
		this(staging, output, mapper);
		this.combiner = combiner;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map processAll(Set arg0)
	{
		if (arg0.size() == 0)
		{
			return null;
		}
		final BackingMapContext bmctx = ((BinaryEntry) arg0.iterator().next())
				.getBackingMapContext();
		this.context = new Context<>(bmctx,
				staging, output, combiner != null);
		for (Iterator iter = arg0.iterator(); iter.hasNext();)
		{
			InvocableMap.Entry entry = (InvocableMap.Entry) iter.next();
			process(entry);
		}
		context.flush();
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object process(final Entry paramEntry)
	{
		context.setSourceKey((K) paramEntry.getKey());
		mapper.map((Comparable<?>) paramEntry.getKey(), paramEntry.getValue(), context);
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(PofReader paramPofReader) throws IOException
	{
		staging = paramPofReader.readString(0);
		output  =  paramPofReader.readString(1);
		mapper = (Mapper<?, ?, K, V>) paramPofReader.readObject(2);
		combiner = (Reducer<K, V, K, V>) paramPofReader.readObject(3);
	}

	@Override
	public void writeExternal(PofWriter paramPofWriter) throws IOException
	{
		paramPofWriter.writeString(0, staging);
		paramPofWriter.writeString(1, output);
		paramPofWriter.writeObject(2, mapper);
		paramPofWriter.writeObject(3, combiner);
	}
}
