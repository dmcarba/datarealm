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

import com.tangosol.net.partition.SimplePartitionKey;

public class IntermediateContext<K extends Comparable<K>, V> extends JobContext<K, V>
{
	private int[] targetPartitions;

	public IntermediateContext(int memberId, String output, int[] targetPartitions)
	{
		super(memberId, output);
		this.targetPartitions = targetPartitions;
	}

	@SuppressWarnings(
	{ "unchecked", "rawtypes" })
	@Override
	protected Object getKey(K key)
	{
		//RoundRobin
		return new CompositeKey(key, SimplePartitionKey.getPartitionKey(targetPartitions[count++
				% targetPartitions.length]), count++);
	}

}
