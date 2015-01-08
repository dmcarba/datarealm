package com.thedatarealm.mapreduce.coherence;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;

public class Context<K extends Comparable<K>, V> implements MapContext<K, V>
{
	private static final String WRITER_SERVICE = "Writer";
	private final Member localMember;
	private final int memberId;
	private int count;
	private final String staging, output;
	private final static int BUFFER_SIZE = 1000;
	private Map<Object, Object> values;
	private Map<Member, Map<Object,Object>> keysToMembers;
	private int mapCount;
	private boolean isCombinerPresent;
	private Comparable<?> sourceKey;
	private DistributedCacheService service;

	public Context(BackingMapContext bmctx, String staging, String output, boolean isCombinerPresent)
	{
		this.localMember = bmctx.getManagerContext().getCacheService().getCluster()
				.getLocalMember();
		this.memberId = localMember.getId();
		this.staging = staging;
		this.output = output;
		this.isCombinerPresent = isCombinerPresent;
		this.values = new HashMap<>();
		this.keysToMembers = new HashMap<>();
		this.service = (DistributedCacheService) bmctx.getManagerContext().getCacheService();
	}

	public void setSourceKey(Comparable<?> sourceKey)
	{
		this.sourceKey = sourceKey;
	}

	public void flush()
	{
		if (isCombinerPresent)
		{
			store();
			values.clear();
		}
		else
		{
			shuffleAndStore();
			keysToMembers.clear();
		}
	}

	public void write(K key, V value)
	{
		
		if (isCombinerPresent)
		{
			if (BUFFER_SIZE == values.size())
			{
				store();
				values.clear();
			}
			@SuppressWarnings(
			{ "unchecked", "rawtypes" })
			CompositeKey<? extends Comparable<?>, K> ckey = new CompositeKey(key, sourceKey,
					count++);
			System.out.println("member " + memberId + " Count " +count);
			if (values.containsKey(ckey))
			{
				throw new RuntimeException("member "+ memberId+ " key already exists: " + ckey);
			}
			values.put(ckey, value);
		}
		else
		{
			if (BUFFER_SIZE == mapCount)
			{
				shuffleAndStore();
				keysToMembers.clear();
			}
			NodeAwareKey<K> nkey = new NodeAwareKey<K>(key, memberId, count++);
			Member member = service.getKeyOwner(nkey);
			if (!keysToMembers.containsKey(member))
			{
				keysToMembers.put(member, new HashMap<Object, Object>());
			}
			keysToMembers.get(member).put(nkey, value);
			mapCount++;
		}
	}

	private void store()
	{
		// Write to local node
		((InvocationService) CacheFactory.getService(WRITER_SERVICE)).query(
				new Writer(values, output),
				Collections.singleton(localMember));
	}

	private void shuffleAndStore()
	{
		Writer.Observer observer = new Writer.Observer(keysToMembers.entrySet().size());
		for (final Entry<Member, Map<Object, Object>> entryList : keysToMembers
				.entrySet())
		{
			((InvocationService) CacheFactory.getService(WRITER_SERVICE)).execute(
					new Writer(entryList.getValue(), staging),
					Collections.singleton(entryList.getKey()), observer);
		}
		observer.waitForExecution();
	}
}
