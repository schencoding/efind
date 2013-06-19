package com.hp.hplc.expr.tpch.q3;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinOrders extends __IndexOperator {
	private static final long serialVersionUID = -4670257654878540013L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 16);
		assert(fields[10].length() == 10);
		if (fields[10].compareTo("1995-03-15") <= 0)
			return (false);
		
		double extendedprice = Double.valueOf(fields[5]);
		double discount = Double.valueOf(fields[6]);
		double revenue = extendedprice * (1 - discount);
		
		// l_orderkey | revenue
		((Text) value).set(fields[0] + "|" + revenue);
		System.out.println("JoinOrders pre: " + ((Text) value).toString());
		
		keys.put(0, new Text(fields[0]));

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
		
		assert(fields.length == 9);
		assert(fields[4].length() == 10);
		if (fields[4].compareTo("1995-03-15") >= 0)
			return;
		
		try {
			// l_orderkey | revenue | o_custkey | o_orderdate | o_shippriority
			Text text = new Text(((Text) value).toString() + "|" + fields[1] + "|" + fields[4] + "|" + fields[7]);
			System.out.println("JoinOrders post: " + ((Text) text).toString());
			output.collect(key, text);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
