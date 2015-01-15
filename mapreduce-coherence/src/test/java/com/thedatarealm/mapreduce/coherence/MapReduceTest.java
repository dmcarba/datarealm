package com.thedatarealm.mapreduce.coherence;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.littlegrid.ClusterMemberGroupUtils;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.PartitionedService;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.filter.EqualsFilter;
import com.tangosol.util.filter.KeyAssociatedFilter;
import com.thedatarealm.mapreduce.coherence.MapReduce.Mapper;
import com.thedatarealm.mapreduce.coherence.MapReduce.Reducer;

@SuppressWarnings("serial")
public class MapReduceTest implements Serializable
{
	private final static String INPUT = "INPUT_CACHE";
	private final static String STAGING = "STAGING_CACHE";
	private final static String OUTPUT = "OUTPUT_CACHE";

	private static final String[] COUNT_SAMPLE =
	{ "How many", "different words can", "you count in", "this text", "keep the count", "of each different one" };
	
	private static final int SAMPLE_SIZE = 2;

	private static final String[] INVERTED_SAMPLE =
	{
			"When a distinguished but elderly scientist states that something is possible, he is almost certainly right. When he states that something is impossible, he is very probably wrong",
			"The only way of discovering the limits of the possible is to venture a little way past them into the impossible",
			"Any sufficiently advanced technology is indistinguishable from magic" };

	@BeforeClass
	public static void setCacheSystemProperties()
	{
		System.setProperty("tangosol.coherence.wka", "localhost");
		ClusterMemberGroupUtils.newBuilder().setStorageEnabledCount(3).setLogLevel(3)
				.setFastStartJoinTimeoutMilliseconds(100)
				.buildAndConfigureForStorageDisabledClient();
	}

	public void setUpCountData()
	{
		NamedCache cache = CacheFactory.getCache(INPUT);
		long j = 0;
		for (long i = 0; i < SAMPLE_SIZE; i++)
		{
			for (int z = 0; z < COUNT_SAMPLE.length; z++)
			{
				cache.put(j++, COUNT_SAMPLE[z]);
			}
		}
	}

	public void setUpIndexData()
	{
		NamedCache cache = CacheFactory.getCache(INPUT);
		for (long i = 0; i < INVERTED_SAMPLE.length; i++)
		{
			cache.put(i, INVERTED_SAMPLE[(int) i]);
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

	@Test
	public void testWordCount()
	{
		setUpCountData();
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT, new WordCountMapper(),
				new WordCountReducer()).mapReduce();
		NamedCache outputCache = CacheFactory.getCache(OUTPUT);
		assertEquals(getCountValue(outputCache, "count"),SAMPLE_SIZE*2);
		assertEquals(getCountValue(outputCache, "different"),SAMPLE_SIZE*2);
		assertEquals(getCountValue(outputCache, "many"),SAMPLE_SIZE);
	}

	@Test
	public void testWordCountWithCombiner()
	{
		setUpCountData();
		Reducer<String, Long, String, Long> reducer;
		new MapReduce<String, Long>(INPUT, STAGING, OUTPUT, new WordCountMapper(),
				reducer = new WordCountReducer(), reducer).mapReduce();
		NamedCache outputCache = CacheFactory.getCache(OUTPUT);
		assertEquals(getCountValue(outputCache, "count"),SAMPLE_SIZE*2);
		assertEquals(getCountValue(outputCache, "different"),SAMPLE_SIZE*2);
		assertEquals(getCountValue(outputCache, "many"),SAMPLE_SIZE);
		printCache(OUTPUT);
	}

	@Test
	public void testInvertedIndex()
	{
		setUpIndexData();
		new MapReduce<String, LongPair>(INPUT, STAGING, OUTPUT, new InvertedIndexMapper(),
				new InvertedIndexReducer()).mapReduce();
		NamedCache outputCache = CacheFactory.getCache(OUTPUT);
		assertTrue(getIndexValue(outputCache, "is").contains("f:4|l:0"));
		assertEquals(getIndexValue(outputCache, "states"),"f:2|l:0");
		assertEquals(getIndexValue(outputCache, "the"),"f:4|l:1");
	}
	
	@Test
	public void testCombinerDataLocality()
	{
		setUpCountData();
		NamedCache inputCache = CacheFactory.getCache(INPUT);
		inputCache.invokeAll(AlwaysFilter.INSTANCE, new MapperProcessor<String, Long>(STAGING, OUTPUT,
				new WordCountMapper(), true));
		//Check that all combined keys generated from a mapper node are stored in that node
		checkAssociation(INPUT, OUTPUT);
//		printCache(OUTPUT);
	}
	
	private void printCache(String cache)
	{
		System.out.println("Cache " + cache);
		for (Map.Entry<Object,Object> entry : (Set<Entry<Object,Object>>) CacheFactory.getCache(cache).entrySet())
		{
			System.out.println(entry);
		}
	}

	private String getIndexValue(NamedCache cache, String word)
	{
		return (String) ((Entry<?, ?>) cache
				.entrySet(new EqualsFilter(MapReduce.KEY_EXTRACTOR, word)).iterator().next())
				.getValue();
	}

	private long getCountValue(NamedCache cache, String word)
	{
		return (long) ((Entry<?, ?>) cache
				.entrySet(new EqualsFilter(MapReduce.KEY_EXTRACTOR, word)).iterator().next())
				.getValue();
	}
	
	public static class LongPair implements PortableObject
	{

		private long frequency;
		private long line;

		public LongPair()
		{
		}

		public LongPair(long frequency, long line)
		{
			this.frequency = frequency;
			this.line = line;
		}

		@Override
		public void readExternal(PofReader paramPofReader) throws IOException
		{
			frequency = paramPofReader.readLong(0);
			line = paramPofReader.readLong(1);
		}

		@Override
		public void writeExternal(PofWriter paramPofWriter) throws IOException
		{
			paramPofWriter.writeLong(0, frequency);
			paramPofWriter.writeLong(1, line);
		}

		@Override
		public String toString()
		{
			return "f:" + frequency + "|l:" + line;
		}

	}

	public static class InvertedIndexMapper implements Mapper<Long, String, String, LongPair>
	{

		@Override
		public void readExternal(PofReader paramPofReader) throws IOException
		{
		}

		@Override
		public void writeExternal(PofWriter paramPofWriter) throws IOException
		{
		}

		@Override
		public void map(Long key, String value, Context<String, LongPair> context)
		{
			Map<String, long[]> wordToFreq = new HashMap<>();
			for (String word : value.split("[ .,]", -1))
			{
				word = word.toLowerCase();
				long[] l = wordToFreq.get(word);
				if (l == null)
				{
					l = new long[]
					{ 0 };
					wordToFreq.put(word, l);
				}
				l[0]++;
			}
			for (Map.Entry<String, long[]> entry : wordToFreq.entrySet())
			{
				context.write(entry.getKey(), new LongPair(entry.getValue()[0], key));
			}

		}

	}

	public static class InvertedIndexReducer implements Reducer<String, LongPair, String, String>
	{

		@Override
		public void readExternal(PofReader arg0) throws IOException
		{
			// TODO Auto-generated method stub

		}

		@Override
		public void writeExternal(PofWriter arg0) throws IOException
		{
			// TODO Auto-generated method stub

		}

		@Override
		public void reduce(String key, Iterator<LongPair> values, Context<String, String> context)
		{
			StringBuilder out = new StringBuilder();
			while (values.hasNext())
			{
				out.append(values.next().toString());
			}
			context.write(key, out.toString());
		}

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

	private void checkAssociation(String input, String output)
	{
		NamedCache inputCache = CacheFactory.getCache(input);
		NamedCache outputCache = CacheFactory.getCache(output);
		PartitionedService ps1 = (PartitionedService) inputCache.getCacheService();
		PartitionedService ps2 = (PartitionedService) outputCache.getCacheService();
		Set<Object> inputKeySet = (Set<Object>) inputCache.keySet();		
		for (Object key : inputKeySet)
		{
			Member member = ps1.getKeyOwner(key);
			Filter filterAsc = new KeyAssociatedFilter(AlwaysFilter.INSTANCE, key);
			@SuppressWarnings("rawtypes")
			Set<CompositeKey> keyset = (Set<CompositeKey>) outputCache.keySet(filterAsc);
			
			for (CompositeKey<?, ?> key1 : keyset)
			{
				Member member1 = ps2.getKeyOwner(key1);
				assertEquals(member, member1);
			}
		}
		
		Set<Map.Entry<Object,Object>> inputEntrySet = (Set<Map.Entry<Object,Object>>) inputCache.entrySet();
		Set<Map.Entry<Object,Object>> outputEntrySet = (Set<Map.Entry<Object,Object>>) outputCache.entrySet();
		
		for (Entry<Object,Object> entry:inputEntrySet)
		{
			System.out.println(ps1.getKeyOwner(entry.getKey()).getId()+" "+ entry);
		}
		System.out.println("===========");
		for (Entry<Object,Object> entry:outputEntrySet)
		{
			System.out.println(ps2.getKeyOwner(entry.getKey()).getId()+" "+ entry);
		}
		
	}
}
