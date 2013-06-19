package com.hp.hplc.expr.tpch.q9;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinPartsupp extends __IndexOperator {
	private static final long serialVersionUID = 4684147835303380619L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 7);
		
		// l_extendedprice | l_discount | l_quantity | l_orderkey | s_nationkey
		((Text) value).set(fields[0] + "|" + fields[1] + "|" + fields[2] + "|" + fields[5] + "|" + fields[6]);

		keys.put(0, new Text(fields[3] + "|" + fields[4]));

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
		
		assert(fields.length == 5);
		
		double supplycost = Double.valueOf(fields[3]);
		
		row = ((Text) value).toString();
		fields = row.split("\\|");
		assert(fields.length == 5);
		
		double extendedprice = Double.valueOf(fields[0]);
		double discount = Double.valueOf(fields[1]);
		double quantity = Double.valueOf(fields[2]);
		
		double amount = extendedprice * (1 - discount) - supplycost * quantity;
		
		try {
			// l_orderkey | s_nationkey | amount
			Text text = new Text(fields[3] + "|" + fields[4] + "|" + amount);
			output.collect(key, text);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
