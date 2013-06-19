package com.hp.hplc.expr.chinacache;

import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class IPLookup extends __IndexOperator {
	private static final long serialVersionUID = 6298928186537298915L;

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split("\\s+");
		String ip = fields[2];
		String url = fields[10];
		
		keys.put(0, new Text(ip));
		((Text) value).set(url);

		return (true);
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys,
		IndexOutput values, OutputCollector<Writable, Writable> output) {
		Vector<Writable>[][] __values = values.getInternal();
		assert(__values.length == 1);
		assert(__values[0].length == 1);
		assert(__values[0][0].size() == 1);
		
		try {
			String area = ((Text) __values[0][0].get(0)).toString();
			String url = ((Text) value).toString();
			output.collect(key, new Text(area + "," + url));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}

