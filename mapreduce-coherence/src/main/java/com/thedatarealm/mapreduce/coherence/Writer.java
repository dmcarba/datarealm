package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationObserver;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

@SuppressWarnings("serial")
public class Writer extends AbstractInvocable implements PortableObject
{
	protected Map<Object, Object> entryMap;
	private int size;
	private String targetCache;

	public Writer()
	{

	}

	public Writer(Map<Object, Object> entryMap, String targetCache)
	{
		this.entryMap = entryMap;
		this.targetCache = targetCache;
		this.size = entryMap.size();
	}

	@Override
	public void run()
	{
		NamedCache cache = CacheFactory.getCache(targetCache);
		
		if (entryMap.values().iterator().next() instanceof Binary)
		{
			for (Map.Entry<Object, Object> entry : entryMap.entrySet())
			{
				if (cache.containsKey(entry.getKey()))
				{
					throw new RuntimeException("Entry exists: " + entry);
				}
				cache.invoke(entry.getKey(), new BinaryValueUpdater((Binary) entry.getValue()));
			}
		}
		else
		{
			for (Map.Entry<Object, Object> entry : entryMap.entrySet())
			{
				if (cache.containsKey(entry.getKey()))
				{
					throw new RuntimeException("Entry exists: " + entry);
				}
				cache.put(entry.getKey(), entry.getValue());
			}
		}
	}

	public static class Observer implements InvocationObserver
	{
		private int numberOfInvocations;
		private int memberCount;
		private Throwable lastError;

		public Observer(int numberOfInvocations)
		{
			this.numberOfInvocations = numberOfInvocations;
		}

		@Override
		public synchronized void memberCompleted(Member paramMember, Object paramObject)
		{
			memberCount++;
			notifyAll();
		}

		@Override
		public synchronized void memberFailed(Member paramMember, Throwable paramThrowable)
		{
			lastError = paramThrowable;
			memberCount++;
			notifyAll();
		}

		@Override
		public synchronized void memberLeft(Member paramMember)
		{
			lastError = new IllegalStateException("Member left the cluster");
			memberCount++;
			notifyAll();
		}

		@Override
		public void invocationCompleted()
		{
		}

		public synchronized void waitForExecution()
		{
			while (memberCount < numberOfInvocations)
			{
				try
				{
					this.wait();
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
				}
			}
			if (lastError != null)
			{
				throw new RuntimeException(lastError);
			}
		}
	}

	@Override
	public void readExternal(PofReader paramPofReader) throws IOException
	{
		targetCache = paramPofReader.readString(0);
		size = paramPofReader.readInt(1);
		entryMap = new HashMap<>(size);
		int id = 2;
		for (int i = 0; i < size; i++)
		{
			entryMap.put(paramPofReader.readObject(id++), paramPofReader.readBinary(id++));
		}
	}

	@Override
	public void writeExternal(PofWriter paramPofWriter) throws IOException
	{
		paramPofWriter.writeString(0, targetCache);
		paramPofWriter.writeInt(1, entryMap.size());
		int id = 2;

		for (Map.Entry<Object, Object> entry : entryMap.entrySet())
		{
			paramPofWriter.writeObject(id++, entry.getKey());
			paramPofWriter
					.writeBinary(
							id++,
							ExternalizableHelper.toBinary(entry.getValue(),
									paramPofWriter.getPofContext()));
		}
	}
}