package com.thedatarealm.mapreduce.coherence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroupUtils;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.extractor.KeyExtractor;
import com.tangosol.util.filter.EqualsFilter;
import com.thedatarealm.mapreduce.coherence.MapReduce.Mapper;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class MapReduceTest implements Serializable
{
	private final static String INPUT = "INPUT_CACHE";
	private final static String STAGING = "STAGING_CACHE";
	private final static String OUTPUT = "OUTPUT_CACHE";


	@BeforeClass
	public static void setCacheSystemProperties()
	{
		System.setProperty("tangosol.coherence.wka", "localhost");
	}

	@Before
	public void setUp()
	{

		ClusterMemberGroupUtils.newBuilder().setStorageEnabledCount(3)
				.setLogLevel(3)
				.setFastStartJoinTimeoutMilliseconds(100)
				.buildAndConfigureForStorageDisabledClient();

		NamedCache cache = CacheFactory.getCache(INPUT);
		for (long i = 1;i< 13;i++)
		{
		cache.put(i++, "How many");
		cache.put(i++, "different words can");
		cache.put(i++, "you count in");
		cache.put(i++, "this text");
		}
	}

	@After
	public void release()
	{
		CacheFactory.shutdown();
	}

	@Test
	public void testWordCount()
	{
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT,
				new Mapper<Long, String, String, Long>()
				{
					List<OrderedKeyValue<String, Long>> result = new ArrayList<>();

					@Override
					public List<OrderedKeyValue<String, Long>> map(Long key, String value)
					{
						result.clear();
						for (String word : value.split(" ", -1))
						{
							result.add(new OrderedKeyValue<String, Long>(word, 1L));
						}
						return result;
					}
				}, new Reducer<String, Long, String, Long>()
				{
					List<OrderedKeyValue<String, Long>> result = new ArrayList<>();

					@Override
					public List<OrderedKeyValue<String, Long>> reduce(String key, List<Long> values)
					{
						result.clear();
						long total = 0;
						for (Long value : values)
						{
							total += value;
						}
						result.add(new OrderedKeyValue<String, Long>(key, total));
						return result;
					}
				}).mapReduce();

		NamedCache stagingCache = CacheFactory.getCache(STAGING);

		int count = 0;
		for (Iterator iter = stagingCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		
		NamedCache outputCache = CacheFactory.getCache(OUTPUT);
		count = 0;
		for (Iterator iter = outputCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
	}

	public void testWordCountWithCombiner()
	{
		Reducer<String, Long, String, Long> reducer;
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT,
				new Mapper<Long, String, String, Long>()
				{
					List<OrderedKeyValue<String, Long>> result = new ArrayList<>();

					@Override
					public List<OrderedKeyValue<String, Long>> map(Long key, String value)
					{
						result.clear();
						for (String word : value.split(" ", -1))
						{
							result.add(new OrderedKeyValue<String, Long>(word, 1L));
						}
						return result;
					}
				}, reducer = new Reducer<String, Long, String, Long>()
				{
					List<OrderedKeyValue<String, Long>> result = new ArrayList<>();

					@Override
					public List<OrderedKeyValue<String, Long>> reduce(String key, List<Long> values)
					{
						result.clear();
						long total = 0;
						for (Long value : values)
						{
							total += value;
						}
						result.add(new OrderedKeyValue<String, Long>(key, total));
						return result;
					}
				}, reducer).mapReduce();

		NamedCache stagingCache = CacheFactory.getCache(STAGING);

		int count = 0;
		for (Iterator iter = stagingCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		
		NamedCache outputCache = CacheFactory.getCache(OUTPUT);
		count = 0;
		for (Iterator iter = outputCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		
		System.out.println("------------------");
		count = 0;
		for (Iterator iter = stagingCache.entrySet(new EqualsFilter(new KeyExtractor("isDistributed"),false)).iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		System.out.println("------------------");
		count = 0;
		for (Iterator iter = stagingCache.entrySet(new EqualsFilter(new KeyExtractor("isDistributed"),true)).iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			System.out.println("key=" + entry.getKey() + " value=" + entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		
	}
}
