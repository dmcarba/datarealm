package com.thedatarealm.mapreduce.coherence;

public interface Context<K extends Comparable<K>, V>
{
	public void write(K key, V value);
}
