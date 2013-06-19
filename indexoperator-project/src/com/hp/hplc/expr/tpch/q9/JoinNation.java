package com.hp.hplc.expr.tpch.q9;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class JoinNation extends __IndexOperator {
	private static final long serialVersionUID = 3096698000918324885L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\|");
		
		assert(fields.length == 3);
		
		// amount | o_year
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
		
		assert(fields.length == 4);
		
		try {
			// amount | o_year | nation
			Text text = new Text(((Text) value).toString() + "|" + fields[1]);
			output.collect(key, text);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
