package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;
import java.util.List;

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
			extends Serializable
	{
		public void map(MKI key, MVI value, MapContext<K,V> context);
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

	public void mapReduce()
	{
		NamedCache inputCache = CacheFactory.getCache(input);

		inputCache.invokeAll(AlwaysFilter.INSTANCE, new MapperProcessor<K, V>(staging, output,this.mapper,
				this.combiner));

		NamedCache stagingCache = CacheFactory.getCache(staging);
		NamedCache outputCache = CacheFactory.getCache(output);
		
		stagingCache.addIndex(KEY_EXTRACTOR, true, null);
		outputCache.addIndex(KEY_EXTRACTOR, true, null);

		Filter filter = AlwaysFilter.INSTANCE;
		if (this.combiner != null)
		{
			outputCache.invokeAll(filter,
					new ReducerProcessor<K, V>(staging, this.combiner));
			outputCache.clear();
			outputCache.removeIndex(KEY_EXTRACTOR);
		}
		stagingCache.invokeAll(filter, new ReducerProcessor<K, V>(output, this.reducer));
	}

//	private void checkAssociation()
//	{
//	    NamedCache inputCache = CacheFactory.getCache(input);
//	    NamedCache outputCache = CacheFactory.getCache(output);
//	    System.out.println("input Cache.size() = " + inputCache.size());
//	    PartitionedService ps1 = (PartitionedService) inputCache.getCacheService();
//	    Set<Long> inputKeySet = (Set<Long>) inputCache.keySet();
//	    for (Long key: inputKeySet)
//	    {
//	      Member member = ps1.getKeyOwner(key);
//	      System.out.println("Coherence member:" + member.getId() + "; input key:" + key );
//	      Filter filterAsc = new KeyAssociatedFilter(AlwaysFilter.INSTANCE, key);
//	      Set<CompositeKey> ONKeySet = (Set<CompositeKey>) outputCache.keySet(filterAsc);
//	      PartitionedService ps2 = (PartitionedService) outputCache.getCacheService();
//	      for (CompositeKey key1: ONKeySet)
//	      {
//	        Member member1 = ps2.getKeyOwner(key1);
//	        System.out.println("              Coherence member:" + member1.getId() + "; output key:" + key1 );
//	      }
//	    }
//	}
}
