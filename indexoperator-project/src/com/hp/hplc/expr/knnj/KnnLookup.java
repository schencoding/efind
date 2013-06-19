package com.hp.hplc.expr.knnj;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class KnnLookup extends __IndexOperator{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7013316638614435274L;

	

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\t");
		String id = fields[0];
		String xStr = fields[1];
		String yStr = fields[2];
		
		keys.put(0, new Text(xStr + "\t" + yStr));
		//((Text) value).set(id);

		return (true);
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys, IndexOutput values,
			OutputCollector<Writable, Writable> output) {
		String v = value.toString();
		Vector<Writable>[][] __values = values.getInternal();
		assert(__values.length == 1);
		assert(__values[0].length == 1);
		assert(__values[0][0].size() == 1);
		
		try {
			String knns = ((Text) __values[0][0].get(0)).toString();
			output.collect(key, new Text(v + "\t" + knns));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
