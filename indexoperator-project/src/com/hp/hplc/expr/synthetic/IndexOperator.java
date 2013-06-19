package com.hp.hplc.expr.synthetic;

import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexoperator.util.IndexInput;
import com.hp.hplc.indexoperator.util.IndexOutput;

public class IndexOperator extends __IndexOperator {
	private static final long serialVersionUID = -483340110211434364L;
	private static final Random rand = new Random();

	@Override
	public boolean preprocess(Writable key, Writable value, IndexInput keys) {
		String row = ((Text) value).toString();
		String[] fields = row.split(",");
		
		assert(fields.length == 2);

		keys.put(0, new Text(fields[0]));

		//if (rand.nextInt(10) < 8) {
			((Text) value).set(fields[1]);
			return (true);
		//} else {
		//	return false;
		//}
	}

	@Override
	public void postprocess(Writable key, Writable value, IndexInput keys,
		IndexOutput values, OutputCollector<Writable, Writable> output) {
		Vector<Writable>[][] __values = values.getInternal();
		assert(__values.length == 1);
		assert(__values[0].length == 1);
		assert(__values[0][0].size() == 1);
		
		/*
		if (__values[0][0].size() == 0) {
			String[] fields = ((Text) value).toString().split(",");
			try {
				throw new Exception("No value found for key #" + fields[0] + "#, " +
					"The input record is #" + ((Text) value).toString() + "#");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		*/

		String inputValue = ((Text) value).toString();
		String indexValue = ((Text) __values[0][0].get(0)).toString();

		try {
			// output.collect(key, new Text(inputValue + "," + indexValue));
			
			int[] cnt = new int [10];
			int i, j;
			
			for (i = 0; i < 10; i++)
				cnt[i] = 0;
			for (i = 0; i < inputValue.length(); i++) {
				char c = inputValue.charAt(i);
				if ('0' <= c && c <= '9')
					cnt[c - '0']++;
			}
			for (i = 0; i < indexValue.length(); i++) {
				char c = indexValue.charAt(i);
				if ('0' <= c && c <= '9')
					cnt[c - '0']++;
			}
			for (j = i = 0; i < 10; i++)
				if (cnt[j] < cnt[i])
					j = i;
			
			output.collect(key, new Text("" + ('0' + j)));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
