package com.hp.hplc.translator;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.indexoperator.SimpleIndexOperator;

public class Mapper4Mapper extends MapReduceBase implements Mapper<Writable, Writable, Writable, Writable> {
	
	private Mapper mapper;

	@Override
	public void map(Writable key, Writable value,
			OutputCollector<Writable, Writable> output, Reporter reporter)
			throws IOException {
		mapper.map(key, value, output, reporter);		
	}

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		
		String mapperClassName = job.get("mapper.mapperclassName");
		if(mapperClassName != null){
			Class c;
			try {
				c = Class.forName(mapperClassName);
				if (c != null) {
					Constructor constructor = c.getConstructor();
					if(constructor != null){
						this.mapper = (Mapper)(constructor.newInstance());
						mapper.configure(job);
					}
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	

}
