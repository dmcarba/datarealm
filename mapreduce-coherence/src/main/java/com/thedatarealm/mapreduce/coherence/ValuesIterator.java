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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.tangosol.util.Binary;
import com.tangosol.util.Converter;

public class ValuesIterator<V> implements Iterator<V>
{

	private Iterator<Binary> currentValues;
	private Converter converter;
	Map<Binary, Binary> bMap;
	
	public ValuesIterator(Set<Binary> currentValues, Converter converter, Map<Binary, Binary> bMap)
	{
		this.currentValues = currentValues.iterator();
		this.converter = converter;
		this.bMap = bMap;
	}

	@Override
	public boolean hasNext()
	{
		return currentValues.hasNext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V next()
	{
		return (V) converter.convert(bMap.remove(currentValues.next()));
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}

}
