package com.hp.hplc.indexopimpl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class OrdersIndexOperator extends __IndexOperator {

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		Text text = (Text)value;
		String str = text.toString();
		String[] strs = str.split("\\|");
		String shipDateStr = strs[10];
		if(shipDateStr.compareToIgnoreCase("1995-03-15")<=0){
			return false;
		}
		//keys.put(0, new Text(strs[0])); //0 for orderkey
		keys.put(0, new Text(strs[2])); //2 for suppkey
		return true;
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys, IndexOutput values,
			OutputCollector<Writable, Writable> output) {
		Text line = (Text)value;
		int numOfKeys = keys.size(0);
		for(int i=0; i<numOfKeys; i++){
			Writable indexkey = keys.get(0, i);
			Iterator<Writable> it = values.iterator(0, i);
			while (it.hasNext()) {
				Writable v = it.next();
				Text tv = (Text) v;
				try {
					line.set(line.toString() + "|" + tv.toString());
					output.collect(indexkey, line);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public Class<? extends Writable> getInputKeyClass() {
		// TODO Auto-generated method stub
		return LongWritable.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		// TODO Auto-generated method stub
		return super.getInputValueClass();
	}
	
	

}
