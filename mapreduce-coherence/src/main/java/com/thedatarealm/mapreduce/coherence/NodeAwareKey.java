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


@SuppressWarnings("serial")
public class NodeAwareKey<K extends Comparable<K>> extends CompositeKey<K, Integer> 
{
	public NodeAwareKey()
	{
	}

	public NodeAwareKey(K key, int nodeId, long sequence)
	{
		super(key, nodeId, sequence);
	}

	@Override
	public Object getAssociatedKey()
	{
		return key1;
	}
}
