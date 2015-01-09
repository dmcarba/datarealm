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

import java.util.HashMap;
import java.util.Map;

import com.tangosol.net.CacheFactory;

public class JobContext<K extends Comparable<K>, V> implements Context<K, V>
{

	private final int memberId;
	protected int count;
	private final String output;
	private final static int BUFFER_SIZE = 1000;
	private Map<Object, Object> values;

	public JobContext(int memberId, String output)
	{
		this.memberId = memberId;
		this.output = output;
		this.values = new HashMap<>();
	}

	public void flush()
	{
		store(output);
		values.clear();
	}

	protected Object getKey(K key)
	{
		return new NodeAwareKey<K>(key, memberId, count++);
	}

	public void write(K key, V value)
	{
		if (BUFFER_SIZE == values.size())
		{
			store(output);
			values.clear();
		}
		values.put(getKey(key), value);
	}

	private void store(String target)
	{
		CacheFactory.getCache(target).putAll(values);
	}
	
	public void setSourceKey(Comparable<?> sourceKey)
	{
	}


}
