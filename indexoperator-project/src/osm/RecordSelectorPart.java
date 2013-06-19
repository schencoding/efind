package osm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RecordSelectorPart {
	
	public static String[] fileNames = {"0.csv", "1.csv", "2.csv", "3.csv", "4.csv",
		"5.csv", "6.csv", "7.csv", "8.csv", "9.csv",
		"10.csv", "11.csv", "12.csv", "13.csv", "14.csv",
		"15.csv", "16.csv", "17.csv", "18.csv", "19.csv",
		"20.csv", "21.csv", "22.csv", "23.csv", "24.csv",
		"25.csv", "26.csv", "27.csv", "28.csv", "29.csv",
		"30.csv", "31.csv", "32.csv"};
	
	
	public static String INPUT_FILE_NAME_PREFIX = "/home/hplcchina/work/EFind_Exp/us_osm_repart/";
	public static String OUTPUT_FILE_NAME_PREFIX = "/home/hplcchina/work/EFind_Exp/us_osm_sel_repart/";
	
	public RecordSelectorPart(){
		
	}
	
	public void run(){
		int counter = 0;
		Random rand = new Random();
		for(int f = 0; f < fileNames.length; f++){
			String fileName = fileNames[f];
			try {
				String fullFileName = INPUT_FILE_NAME_PREFIX + fileName;
				System.out.println(fullFileName);
				FileReader fr = new FileReader(fullFileName);				
				BufferedReader br = new BufferedReader(fr);
				
				String outputFullFileName = OUTPUT_FILE_NAME_PREFIX + fileName;
				FileWriter fw = new FileWriter(outputFullFileName);
				BufferedWriter bw = new BufferedWriter(fw);
				
				String line = null;
				while ((line = br.readLine()) != null) {
					if (rand.nextDouble() < 0.1) {
						bw.write(line);
						bw.newLine();
						counter ++;
					}
				}
				br.close();
				fr.close();
				
				bw.flush();
				fw.flush();
				bw.close();
				fw.close();
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("# of records: " + counter);

	}

		/**
		 * @param args
		 */
		public static void main(String[] args) {
			RecordSelectorPart sel = new RecordSelectorPart();
			sel.run();
		}
}
