package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.net.cache.KeyAssociation;
import com.tangosol.util.Filter;
import com.tangosol.util.extractor.KeyExtractor;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.KeyAssociatedFilter;

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
	public static final KeyExtractor KEY_EXTRACTOR2 = new KeyExtractor("getKey1");

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
				if (entry.getKey() == null)
				{
					throw new RuntimeException("Null Key " + entry);
				}
				cache.put(entry.getKey(), entry.getValue());
			}
		}
	}

	@SuppressWarnings("serial")
	public static class CompositeKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements
			KeyAssociation, Serializable, Comparable<CompositeKey<K1, K2>>
	{
		private K1 key1;
		private K2 key2;
		long sequence;

		public CompositeKey()
		{
		}

		public CompositeKey(K1 key1, K2 key2, long sequence)
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
		public Object getAssociatedKey()
		{
			return key2;
		}

		public Object getKey()
		{
			return null;
		}

		@Override
		public int compareTo(CompositeKey<K1, K2> o)
		{
			return key1.compareTo(o.key1);
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
			return key1.equals(((CompositeKey<K1, K2>) obj).key1);
		}

		@Override
		public String toString()
		{
			return "CompositeKey [key1=" + key1 + ", key2=" + key2 + ", sequence=" + sequence + "]";
		}

	}

	@SuppressWarnings("serial")
	public static class DistributedKey<K extends Comparable<K>> implements
			Comparable<DistributedKey<K>>, KeyAssociation, Serializable
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

		public Object getKey2()
		{
			return null;
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
		public Object getAssociatedKey()
		{
			return key;
		}

	}

	public void mapReduce()
	{
		NamedCache inputCache = CacheFactory.getCache(input);

		inputCache.invokeAll(AlwaysFilter.INSTANCE, new MapperProcessor<K, V>(staging, output,this.mapper,
				this.combiner));

		NamedCache stagingCache = CacheFactory.getCache(staging);
		NamedCache outputCache = CacheFactory.getCache(output);
		
		stagingCache.addIndex(KEY_EXTRACTOR, true, null);
		outputCache.addIndex(KEY_EXTRACTOR2, true, null);

		Filter filter = AlwaysFilter.INSTANCE;
		if (this.combiner != null)
		{
			checkAssociation();
			outputCache.invokeAll(filter,
					new CombinerProcessor<K, V>(staging, this.combiner));
			outputCache.clear();
			outputCache.removeIndex(KEY_EXTRACTOR2);
		}

		stagingCache.invokeAll(filter, new ReducerProcessor<K, V>(output, this.reducer));
	}

	private void checkAssociation()
	{
	    NamedCache inputCache = CacheFactory.getCache(input);
	    NamedCache outputCache = CacheFactory.getCache(output);
	    System.out.println("input Cache.size() = " + inputCache.size());
	    PartitionedService ps1 = (PartitionedService) inputCache.getCacheService();
	    Set<Long> inputKeySet = (Set<Long>) inputCache.keySet();
	    for (Long key: inputKeySet)
	    {
	      Member member = ps1.getKeyOwner(key);
	      System.out.println("Coherence member:" + member.getId() + "; input key:" + key );
	      Filter filterAsc = new KeyAssociatedFilter(AlwaysFilter.INSTANCE, key);
	      Set<CompositeKey> ONKeySet = (Set<CompositeKey>) outputCache.keySet(filterAsc);
	      PartitionedService ps2 = (PartitionedService) outputCache.getCacheService();
	      for (CompositeKey key1: ONKeySet)
	      {
	        Member member1 = ps2.getKeyOwner(key1);
	        System.out.println("              Coherence member:" + member1.getId() + "; output key:" + key1 );
	      }
	    }
	}
}
