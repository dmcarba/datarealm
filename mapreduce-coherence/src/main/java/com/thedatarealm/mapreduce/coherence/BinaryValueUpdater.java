package com.thedatarealm.mapreduce.coherence;

import java.io.IOException;

import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableObject;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap;
import com.tangosol.util.processor.AbstractProcessor;

@SuppressWarnings("serial")
public class BinaryValueUpdater extends AbstractProcessor implements PortableObject
{

	private Binary binaryValue;

	public BinaryValueUpdater()
	{
	}

	public BinaryValueUpdater(Binary binaryValue)
	{
		this.binaryValue = binaryValue;
	}

	@Override
	public Object process(InvocableMap.Entry entry)
	{
		((BinaryEntry) entry).updateBinaryValue(binaryValue);
		return null;
	}

	@Override
	public void readExternal(PofReader pofReader) throws IOException
	{
		binaryValue = pofReader.readBinary(0);
	}

	@Override
	public void writeExternal(PofWriter pofWriter) throws IOException
	{
		pofWriter.writeBinary(0, binaryValue);
	}

}