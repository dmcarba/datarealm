/* 
 * Copyright (C) 2015 by David Carballo (http://datalocus.blogspot.com.es/)
 *
 **********************************
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */
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
public class MapperProcessor<K extends Comparable<K>, V> extends AbstractProcessor implements
		PortableObject
{
	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	@SuppressWarnings("rawtypes")
	private Reducer combiner;
	private String staging, output;
	private JobContext<K, V> context;

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
		if (combiner != null)
		{
			this.context = new IntermediateContext<K, V>(bmctx, output);
		}
		else
		{
			this.context = new JobContext<>(bmctx, staging);
		}
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
		output = paramPofReader.readString(1);
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
