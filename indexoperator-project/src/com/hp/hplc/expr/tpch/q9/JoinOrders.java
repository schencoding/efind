package com.hp.hplc.expr.tpch.q9;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinOrders extends __IndexOperator {
	private static final long serialVersionUID = -6710035319942059699L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 3);

		// s_nationkey | amount
		((Text) value).set(fields[1] + "|" + fields[2]);
		
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
		
		try {
			// s_nationkey | amount | o_year
			Text text = new Text(((Text) value).toString() + "|" + fields[4].substring(0, 4));
			output.collect(key, text);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
