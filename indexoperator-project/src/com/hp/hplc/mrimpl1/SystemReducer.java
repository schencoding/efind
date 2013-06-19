package com.hp.hplc.mrimpl1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import com.hp.hplc.indexoperator.util.K2V2Writable;



public class SystemReducer extends IdentityReducer {

	@Override
	public void reduce(Object key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
		while(values.hasNext()){
			/*
			Object obj = values.next();
			//System.out.println("Key: " + key.toString());
			//System.out.println("Value: " + obj.toString());
			output.collect(key, obj);
			*/
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			((K2V2Writable) values.next()).write(dos);
			
			byte[] buf = baos.toByteArray();
			
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			DataInputStream dis = new DataInputStream(bais);
			K2V2Writable got = K2V2Writable.read(dis);
			
			output.collect(key, got);
		}
	}
	

}
