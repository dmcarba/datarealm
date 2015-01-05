package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;

@SuppressWarnings("serial")
public class OrderedKeyValue<K extends Comparable<?>, V> implements Serializable
{
	private K key;
	private V value;

	public OrderedKeyValue(K key, V value)
	{
		this.key = key;
		this.value = value;
	}

	public K getKey()
	{
		return key;
	}

	public V getValue()
	{
		return value;
	}

	@Override
	public String toString()
	{
		return "OrderedKeyValue [key=" + key + ", value=" + value + "]";
	}
	
	
}