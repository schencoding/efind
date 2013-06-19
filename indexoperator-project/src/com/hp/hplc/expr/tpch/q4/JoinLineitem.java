package com.hp.hplc.expr.tpch.q4;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinLineitem extends __IndexOperator {
	private static final long serialVersionUID = -2375155088198992815L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 9);
		assert(fields[4].length() == 10);
		if (fields[4].compareTo("1993-07-01") < 0 ||
			fields[4].compareTo("1993-11-01") >= 0)
			return (false);

		// o_orderpriority
		((Text) value).set(fields[5]);

		keys.put(0, new Text(fields[0]));

		return (true);
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys,
		IndexOutput values, OutputCollector<Writable, Writable> output) {
		Vector<Writable>[][] __values = values.getInternal();
		assert(__values.length == 1);
		assert(__values[0].length == 1);
		
		boolean flag = false;
		for (int i = 0; i < __values[0][0].size(); i++) {
			String row = ((Text) __values[0][0].get(0)).toString();
			String[] fields = row.split("\\|");
		
			assert(fields.length == 16);
			assert(fields[11].length() == 10);
			assert(fields[12].length() == 10);
			
			if (fields[11].compareTo(fields[12]) < 0) {
				flag = true;
				break;
			}
		}
		
		if (! flag)
			return;
		
		try {
			output.collect(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
