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

import java.io.Serializable;
import java.util.Iterator;

import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.KeyExtractor;
import com.tangosol.util.filter.AlwaysFilter;

public class MapReduce<K extends Comparable<K>, V>
{
	private String input, output, staging;

	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	@SuppressWarnings("rawtypes")
	private Reducer reducer;
	@SuppressWarnings("rawtypes")
	private Reducer combiner;

	public static final KeyExtractor KEY_EXTRACTOR = new KeyExtractor("getKey1");

	public static interface Mapper<MKI extends Comparable<MKI>, MVI, K extends Comparable<K>, V>
			extends Serializable, PortableObject
	{
		public void map(MKI key, MVI value, Context<K, V> context);
	}

	public static interface Reducer<K, V, RKO extends Comparable<RKO>, RVO> extends Serializable, PortableObject
	{
		public void reduce(K key, Iterator<V> values, Context<RKO, RVO> context);
	}

	public MapReduce(String input, String staging, String output, Mapper<?, ?, K, V> mapper,
			Reducer<K, V, ?, ?> reducer)
	{
		this.input = input;
		this.staging = staging;
		this.output = output;
		this.mapper = mapper;
		this.reducer = reducer;
	}

	public MapReduce(String input, String staging, String output, Mapper<?, ?, K, V> mapper,
			Reducer<K, V, ?, ?> reducer, Reducer<K, V, K, V> combiner)
	{
		this.input = input;
		this.staging = staging;
		this.output = output;
		this.mapper = mapper;
		this.reducer = reducer;
		this.combiner = combiner;
	}

	public void mapReduce()
	{
		NamedCache inputCache = CacheFactory.getCache(input);
		NamedCache stagingCache = CacheFactory.getCache(staging);
		NamedCache outputCache = CacheFactory.getCache(output);
		stagingCache.clear();
		outputCache.clear();

		inputCache.invokeAll(AlwaysFilter.INSTANCE, new MapperProcessor<K, V>(staging, output,
				this.mapper, this.combiner));

		stagingCache.addIndex(KEY_EXTRACTOR, true, null);
		outputCache.addIndex(KEY_EXTRACTOR, true, null);
		
		Filter filter = AlwaysFilter.INSTANCE;
		if (this.combiner != null)
		{
			outputCache.invokeAll(filter, new ReducerProcessor<K, V>(staging, this.combiner));
			outputCache.clear();
			outputCache.removeIndex(KEY_EXTRACTOR);
		}
		stagingCache.invokeAll(filter, new ReducerProcessor<K, V>(output, this.reducer));
	}
	
}
