package osm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class RecordConverter {
	
	private FileWriter fw = null;
	private BufferedWriter bw = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RecordConverter conv = new RecordConverter();
		conv.run();
	}

	public RecordConverter(){
		try {
			fw = new FileWriter("outer");
			bw = new BufferedWriter(fw);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void run() {
		String fileName = "outer-1";
		int counter = 0;
		try {
			FileReader fr = new FileReader(fileName);
			
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\t");
				counter ++;
				String res = "" + counter;
				res += "\t";
				res += fields[1]+"\t";
				res += fields[2];
				bw.write(res);
				bw.newLine();
				if(counter == 40000000)
					break;
			}
			br.close();
			fr.close();
			
			bw.flush();
			fw.flush();
			bw.close();
			fw.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		System.out.println(counter);
	}

}
