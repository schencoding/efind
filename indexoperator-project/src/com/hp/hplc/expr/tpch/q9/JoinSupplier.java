package com.hp.hplc.expr.tpch.q9;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinSupplier extends __IndexOperator {
	private static final long serialVersionUID = 5902240368859043040L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 16);
		
		// l_extendedprice | l_discount | l_quantity | l_partkey | l_suppkey | l_orderkey
		((Text) value).set(fields[5] + "|" + fields[6] + "|" + fields[4] + "|" +
			fields[1] + "|" + fields[2] + "|" + fields[0]);
		
		keys.put(0, new Text(fields[2]));

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
		
		assert(fields.length == 7);
		
		try {
			// l_extendedprice | l_discount | l_quantity | l_partkey | l_suppkey | l_orderkey | s_nationkey
			Text text = new Text(((Text) value).toString() + "|" + fields[3]);
			output.collect(key, text);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
