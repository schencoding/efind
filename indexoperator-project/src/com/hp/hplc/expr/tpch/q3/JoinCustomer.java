package com.hp.hplc.expr.tpch.q3;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinCustomer extends __IndexOperator {
	private static final long serialVersionUID = -4407529369393248010L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 5);
		keys.put(0, new Text(fields[2]));
		
		// l_orderkey | revenue | o_orderdate | o_shippriority
		((Text) value).set(fields[0] + "|" + fields[1] + "|" + fields[3] + "|" + fields[4]);
		System.out.println("JoinCustomer pre: " + ((Text) value).toString());

		return (true);
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys,
		IndexOutput values, OutputCollector<Writable, Writable> output) {
		Vector<Writable>[][] __values = values.getInternal();
		assert(__values.length == 1);
		assert(__values[0].length == 1);
		assert(__values[0][0].size() == 1);
		String row = ((Text) __values[0][0].get(0)).toString();
		String[] fields = row.split("\\|");

		assert(fields.length == 8);
		if (! fields[6].equals("BUILDING"))
			return;
		
		try {
			output.collect(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
}
