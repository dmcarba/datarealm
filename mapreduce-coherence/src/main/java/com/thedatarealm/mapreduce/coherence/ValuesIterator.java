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
