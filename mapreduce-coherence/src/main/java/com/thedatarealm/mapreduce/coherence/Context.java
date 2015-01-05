package com.thedatarealm.mapreduce.coherence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tangosol.net.BackingMapContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.DistributedCacheService;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;

public class Context<K extends Comparable<K>,V> implements MapContext<K, V>
{
	private final Member localMember;
	private final int memberId;
	private int count;
	private final String staging, output;
	private final static int BUFFER_SIZE = 100;
	private List<OrderedKeyValue<CompositeKey<? extends Comparable<?>, K>, V>> values;
	private Map<Member, List<OrderedKeyValue<NodeAwareKey<K>, V>>> keysToMembers;
	private int mapCount;
	private boolean isCombinerPresent;
	private Comparable<?> sourceKey;
	private BackingMapContext bmctx;

	public Context(BackingMapContext bmctx, String staging, String output, boolean isCombinerPresent)
	{
		this.bmctx = bmctx;
		this.localMember = bmctx.getManagerContext().getCacheService().getCluster().getLocalMember();
		this.memberId = localMember.getId();
		this.staging = staging;
		this.output = output;
		this.isCombinerPresent = isCombinerPresent;
		this.values = new ArrayList<>();
		this.keysToMembers = new HashMap<>();
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
			values.add(new OrderedKeyValue<CompositeKey<? extends Comparable<?>, K>, V>(ckey, value));
		}
		else
		{
			if (BUFFER_SIZE == mapCount)
			{
				shuffleAndStore();
				keysToMembers.clear();
			}
			NodeAwareKey<K> nkey = new NodeAwareKey<K>(key, memberId, count++);
			Member member = ((DistributedCacheService) bmctx.getManagerContext().getCacheService())
					.getKeyOwner(nkey);
			if (!keysToMembers.containsKey(member))
			{
				keysToMembers.put(member, new ArrayList<OrderedKeyValue<NodeAwareKey<K>, V>>());
			}
			keysToMembers.get(member).add(
					new OrderedKeyValue<NodeAwareKey<K>, V>(nkey, value));
			mapCount++;
		}
	}

	private void store()
	{
		// Write to local node
		((InvocationService) CacheFactory.getService("Shuffle")).query(
				new WriterService<CompositeKey<? extends Comparable<?>, K>, V>(values, output),
				Collections.singleton(localMember));
	}

	private void shuffleAndStore()
	{
		for (Map.Entry<Member, List<OrderedKeyValue<NodeAwareKey<K>, V>>> entryList : keysToMembers
				.entrySet())
		{
			((InvocationService) CacheFactory.getService("Shuffle")).query(
					new WriterService<NodeAwareKey<K>, V>(entryList.getValue(), staging),
					Collections.singleton(entryList.getKey()));
		}
	}
}
