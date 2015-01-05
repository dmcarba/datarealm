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

		ClusterMemberGroupUtils.newBuilder().setStorageEnabledCount(3).setLogLevel(3)
				.setFastStartJoinTimeoutMilliseconds(100)
				.buildAndConfigureForStorageDisabledClient();

		NamedCache cache = CacheFactory.getCache(INPUT);
		long j = 0;
		for (long i = 0; i < 1000; i++)
		{
			cache.put(j++, "How many");
			cache.put(j++, "different words can");
			cache.put(j++, "you count in");
			cache.put(j++, "this text");
		}
	}

	@After
	public void release()
	{
		CacheFactory.shutdown();
	}

	public void testWordCount()
	{
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT,
				new Mapper<Long, String, String, Long>()
				{
					@Override
					public void map(Long key, String value, MapContext<String, Long> context)
					{
						for (String word : value.split(" ", -1))
						{
							context.write(word, 1L);
						}
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

	@Test
	public void testWordCountWithCombiner()
	{
		Reducer<String, Long, String, Long> reducer;
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT,
				new Mapper<Long, String, String, Long>()
				{
					@Override
					public void map(Long key, String value, MapContext<String, Long> context)
					{
						for (String word : value.split(" ", -1))
						{
							context.write(word, 1L);
						}
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
		NamedCache inputCache = CacheFactory.getCache(INPUT);

		int count = 0;
		for (Iterator iter = inputCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			// System.out.println("key=" + entry.getKey() + " value=" +
			// entry.getValue());
			count++;
		}
		System.out.println("input count=" + count);

		count = 0;
		for (Iterator iter = stagingCache.entrySet().iterator(); iter.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			// System.out.println("key=" + entry.getKey() + " value=" +
			// entry.getValue());
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
		for (Iterator iter = stagingCache.entrySet(
				new EqualsFilter(new KeyExtractor("isDistributed"), false)).iterator(); iter
				.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			// System.out.println("key=" + entry.getKey() + " value=" +
			// entry.getValue());
			count++;
		}
		System.out.println("count=" + count);
		System.out.println("------------------");
		count = 0;
		for (Iterator iter = stagingCache.entrySet(
				new EqualsFilter(new KeyExtractor("isDistributed"), true)).iterator(); iter
				.hasNext();)
		{
			Map.Entry entry = (Map.Entry) iter.next();
			// System.out.println("key=" + entry.getKey() + " value=" +
			// entry.getValue());
			count++;
		}
		System.out.println("count=" + count);

	}

}