package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;
import java.util.List;

import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.KeyAssociation;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.KeyExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;

public class MapReduce<K extends Comparable<K>, V>
{
	private String input, output, staging;

	@SuppressWarnings("rawtypes")
	private Mapper mapper;
	@SuppressWarnings("rawtypes")
	private Reducer reducer;
	@SuppressWarnings("rawtypes")
	private Reducer combiner;

	public static final KeyExtractor KEY_EXTRACTOR = new KeyExtractor("getKey");
	public static final KeyExtractor KEY_EXTRACTOR2 = new KeyExtractor("getKey2");

	public static interface Mapper<MKI extends Comparable<MKI>, MVI, K extends Comparable<K>, V>
			extends Serializable
	{
		public List<OrderedKeyValue<K, V>> map(MKI key, MVI value);
	}

	public static interface Reducer<K, V, RKO extends Comparable<RKO>, RVO> extends Serializable
	{
		public List<OrderedKeyValue<RKO, RVO>> reduce(K key, List<V> value);
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

	@SuppressWarnings("serial")
	public static class WriterService<K extends Comparable<K>, V> extends AbstractInvocable
	{
		protected List<OrderedKeyValue<K, V>> entryList;
		private String targetCache;

		public WriterService(List<OrderedKeyValue<K, V>> entryList, String targetCache)
		{
			this.entryList = entryList;
			this.targetCache = targetCache;
		}

		@Override
		public void run()
		{
			NamedCache cache = CacheFactory.getCache(targetCache);
			for (OrderedKeyValue<K, V> entry : entryList)
			{
				cache.put(entry.getKey(), entry.getValue());
			}
		}
	}

	private interface Distributed
	{
		public boolean isDistributed();
	}

	@SuppressWarnings("serial")
	public static class LocalKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements
			Distributed, KeyAssociation, Serializable, Comparable<LocalKey<K1, K2>>
	{
		private K1 key1;
		private K2 key2;
		long sequence;

		public LocalKey()
		{
		}

		public LocalKey(K1 key1, K2 key2, long sequence)
		{
			this.key1 = key1;
			this.key2 = key2;
			this.sequence = sequence;
		}	

		public K1 getKey1()
		{
			return key1;
		}

		public K2 getKey2()
		{
			return key2;
		}

		@Override
		public boolean isDistributed()
		{
			return false;
		}

		@Override
		public Object getAssociatedKey()
		{
			return key1;
		}

		@Override
		public int compareTo(LocalKey<K1, K2> o)
		{
			return key2.compareTo(o.key2);
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key1 == null) ? 0 : key1.hashCode());
			result = prime * result + ((key2 == null) ? 0 : key2.hashCode());
			result = prime * result + (int) (sequence ^ (sequence >>> 32));
			return result;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj)
		{
			return key2.equals(((LocalKey<K1,K2>) obj).key2);
		}

		@Override
		public String toString()
		{
			return "LocalKey [key1=" + key1 + ", key2=" + key2 + ", sequence=" + sequence + "]";
		}
				
	}

	@SuppressWarnings("serial")
	public static class DistributedKey<K extends Comparable<K>> implements
			Comparable<DistributedKey<K>>, Distributed, KeyAssociation, Serializable
	{

		private K key;
		private int nodeId;
		private long sequence;

		public DistributedKey()
		{
		}

		public DistributedKey(K key, int nodeId, long sequence)
		{
			this.key = key;
			this.nodeId = nodeId;
			this.sequence = sequence;
		}

		public K getKey()
		{
			return key;
		}

		@Override
		public int compareTo(DistributedKey<K> o)
		{
			return key.compareTo(o.key);
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + nodeId;
			result = prime * result + (int) (sequence ^ (sequence >>> 32));
			return result;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj)
		{
			return key.equals(((DistributedKey<K>) obj).key);
		}

		@Override
		public String toString()
		{
			return "DistributedKey [key=" + key + ", nodeId=" + nodeId + ", sequence=" + sequence
					+ "]";
		}

		@Override
		public boolean isDistributed()
		{
			return true;
		}

		@Override
		public Object getAssociatedKey()
		{
			return key;
		}

	}

	public void mapReduce()
	{
		NamedCache inputCache = CacheFactory.getCache(input);

		inputCache.invokeAll(AlwaysFilter.INSTANCE, new MapperProcessor<K, V>(staging, this.mapper,
				this.combiner));

		NamedCache stagingCache = CacheFactory.getCache(staging);

		stagingCache.addIndex(KEY_EXTRACTOR, true, null);
		stagingCache.addIndex(KEY_EXTRACTOR2, true, null);

		Filter filter = AlwaysFilter.INSTANCE;
		if (this.combiner != null)
		{
			stagingCache.invokeAll(new EqualsFilter(new KeyExtractor("isDistributed"), false),
					new CombinerProcessor<K, V>(staging, this.combiner));
			filter = new EqualsFilter(new KeyExtractor("isDistributed"), true);
		}

		stagingCache.invokeAll(filter, new ReducerProcessor<K, V>(output, this.reducer));
	}

}
