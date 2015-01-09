/* 
 * Copyright (C) 2015 by David Carballo (http://datalocus.blogspot.com.es/)
 *
 **********************************
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */

package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;
import java.io.Serializable;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.net.cache.KeyAssociation;

@SuppressWarnings("serial")
public class CompositeKey<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements
		KeyAssociation, Serializable, PortableObject
{
	protected K1 key1;
	protected K2 key2;
	protected long sequence;

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

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeKey<?, ?> other = (CompositeKey<?, ?>) obj;
		if (key1 == null)
		{
			if (other.key1 != null)
				return false;
		}
		else if (!key1.equals(other.key1))
			return false;
		if (key2 == null)
		{
			if (other.key2 != null)
				return false;
		}
		else if (!key2.equals(other.key2))
			return false;
		if (sequence != other.sequence)
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "CompositeKey [key1=" + key1 + ", key2=" + key2 + ", sequence=" + sequence + "]";
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(PofReader paramPofReader) throws IOException
	{
		key1 = (K1) paramPofReader.readObject(1);
		key2 = (K2) paramPofReader.readObject(2);
		sequence = paramPofReader.readLong(3);
	}

	@Override
	public void writeExternal(PofWriter paramPofWriter) throws IOException
	{
		paramPofWriter.writeObject(1, key1);
		paramPofWriter.writeObject(2, key2);
		paramPofWriter.writeLong(3, sequence);
	}

}