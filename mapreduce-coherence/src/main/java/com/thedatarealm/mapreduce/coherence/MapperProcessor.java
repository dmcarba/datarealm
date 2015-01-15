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
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;
import com.thedatarealm.mapreduce.coherence.MapReduce.Mapper;

@SuppressWarnings("serial")
public class MapperProcessor<K extends Comparable<K>, V> extends AbstractProcessor implements
		PortableObject
{
	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	private boolean combineOutput;
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
			boolean combineOutput)
	{
		this(staging, output, mapper);
		this.combineOutput = combineOutput;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map processAll(Set arg0)
	{
		if (arg0.size() == 0)
		{
			return null;
		}
		final BackingMapManagerContext bmctx = ((BinaryEntry) arg0.iterator().next())
				.getBackingMapContext().getManagerContext();
		final int id = bmctx.getCacheService().getCluster().getLocalMember().getId();
		if (combineOutput)
		{
			this.context = new IntermediateContext<K, V>(id, output,
					((DistributedCacheService) CacheFactory.getCache(output).getCacheService())
							.getOwnedPartitions(
									bmctx.getCacheService().getCluster().getLocalMember())
							.toArray());
		}
		else
		{
			this.context = new JobContext<>(id, staging);
		}
		for (Iterator iter = arg0.iterator(); iter.hasNext();)
		{
			InvocableMap.Entry entry = (InvocableMap.Entry) iter.next();
			process(entry);
		}
		context.flush();
		return null;
	}

	@Override
	public Object process(final Entry paramEntry)
	{
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
		combineOutput = paramPofReader.readBoolean(3);
	}

	@Override
	public void writeExternal(PofWriter paramPofWriter) throws IOException
	{
		paramPofWriter.writeString(0, staging);
		paramPofWriter.writeString(1, output);
		paramPofWriter.writeObject(2, mapper);
		paramPofWriter.writeBoolean(3, combineOutput);
	}
}
