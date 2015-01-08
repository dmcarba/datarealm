package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroupUtils;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
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
		ClusterMemberGroupUtils.newBuilder().setStorageEnabledCount(3).setLogLevel(3)
				.setFastStartJoinTimeoutMilliseconds(100)
				.buildAndConfigureForStorageDisabledClient();
	}

	@Before
	public void setUp()
	{
		NamedCache cache = CacheFactory.getCache(INPUT);
		long j = 0;
		for (long i = 0; i < 10; i++)
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
		CacheFactory.getCache(INPUT).clear();
		CacheFactory.getCache(STAGING).clear();
		CacheFactory.getCache(OUTPUT).clear();
		CacheFactory.shutdown();
	}

//	@Test
	public void testWordCount()
	{
		long start = System.currentTimeMillis();
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT, new WordCountMapper(),
				new WordCountReducer()).mapReduce();
		System.out.println("TIME TAKEN:" + (System.currentTimeMillis() - start));

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
		long start = System.currentTimeMillis();
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT, new WordCountMapper(),
				reducer = new WordCountReducer(), reducer).mapReduce();
		System.out.println("TIME TAKEN:" + (System.currentTimeMillis() - start));
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

	public static class WordCountMapper implements Mapper<Long, String, String, Long>
	{
		@Override
		public void map(Long key, String value, Context<String, Long> context)
		{
			for (String word : value.split(" ", -1))
			{
				context.write(word, 1L);
			}
		}

		@Override
		public void readExternal(PofReader paramPofReader) throws IOException
		{
		}

		@Override
		public void writeExternal(PofWriter paramPofWriter) throws IOException
		{
		}
	}

	public static class WordCountReducer implements Reducer<String, Long, String, Long>
	{
		@Override
		public void reduce(String key, Iterator<Long> values, Context<String, Long> context)
		{
			long total = 0;
			while (values.hasNext())
			{
				total += values.next();
			}
			context.write(key, total);
		}

		@Override
		public void readExternal(PofReader paramPofReader) throws IOException
		{
		}

		@Override
		public void writeExternal(PofWriter paramPofWriter) throws IOException
		{
		}
	}

}
