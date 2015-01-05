package com.thedatarealm.mapreduce.coherence;

public interface MapContext<K extends Comparable<K>, V>
{
	public void write(K key, V value);
}
