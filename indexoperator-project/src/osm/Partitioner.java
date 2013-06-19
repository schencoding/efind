package osm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Partitioner {
	
	public String[] fileNames = {"alabama", "alaska", "arizona", "arkansas", "california", 
			"colorado", "connecticut", "delaware", "district-of-columbia", "florida",
			"georgia", "hawaii", "idaho", "illinois", "indiana", 
			"iowa", "kansas", "kentucky", "louisiana", "maine", 
			"maryland", "massachusetts", "michigan", "minnesota", "mississippi", 
			"missouri", "montana", "nebraska", "nevada", "new-hampshire", 
			"new-jersey", "new-mexico", "new-york", "north-carolina", "north-dakota", 
			"ohio", "oklahoma", "oregon", "pennsylvania", "rhode-island", 
			"south-carolina", "south-dakota", "tennessee", "texas", "utah", 
			"vermont", "virginia", "washington", "west-virginia", "wisconsin", 
			"wyoming"};
	
	//public String[] fileNames = {"alabama", "alaska"};

	public static int NEG = -1;
	public static int AMP = 10000000;
	public static String IP_PREFIX = "16.158.159.";
	public static String INPUT_FILE_NAME_PREFIX = "/home/hplcchina/work/EFind_Exp/us_osm_node/";
	public static String INPUT_FILE_NAME_SURFIX = ".osm.pbf.csv";
	public static String OUTPUT_FILE_NAME_PREFIX = "/home/hplcchina/work/EFind_Exp/us_osm_repart/";
	public static int NUM_PARTITIONS = 33;
	public static int SERVICE_PORT_PRE = 12345;
	
	FileWriter fws[] = new FileWriter[NUM_PARTITIONS];
	BufferedWriter bws[] = new BufferedWriter[NUM_PARTITIONS];
	
	
	
	public Partitioner() {
		this.init();
	}

	public static int getPartitionId(int lo, int lat){
		int x = ((lo + 60) * NEG)/9;
		int y = (lat - 23)/9;
		int res = y*8+x;
		if(x <0){
			return NUM_PARTITIONS-1;
		}
		if(res >= NUM_PARTITIONS-1){
			return NUM_PARTITIONS-1;
		}
		return res;
	}
	
	
	public static String getIPAddress(int partId) {
		int nodeId = partId % 11;
		return "" + (161 + nodeId);
	}
	
	public static int getPort(int partId){
		return SERVICE_PORT_PRE + (partId%3);
	}
	

	public void run(){
		for(int f = 0; f < fileNames.length; f++){
			String fileName = fileNames[f];
			try {
				String fullFileName = INPUT_FILE_NAME_PREFIX + fileName + INPUT_FILE_NAME_SURFIX;
				System.out.println(fullFileName);
				FileReader fr = new FileReader(fullFileName);
				
				BufferedReader br = new BufferedReader(fr);
				String line = null;
				while((line = br.readLine()) != null){
					String[] fields = line.split("\\t");
					long id = Long.parseLong(fields[0]);
					float lo = Float.parseFloat(fields[1]);
					float la = Float.parseFloat(fields[2]);
					
					int loi = (int) (lo );
					int lai = (int) (la );
					
					int partId = this.getPartitionId(loi, lai);
					bws[partId].write(line);	
					bws[partId].newLine();
				}
				br.close();
				fr.close();
				
				for(int i=0; i<NUM_PARTITIONS; i++){
					try {
						bws[i].flush();
						fws[i].flush();
					} catch (IOException e) {
						e.printStackTrace();
					}			
				}
				
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void init(){
		
		for(int i=0; i<NUM_PARTITIONS; i++){
			try {
				fws[i] = new FileWriter(OUTPUT_FILE_NAME_PREFIX + i + ".csv");
				bws[i] = new BufferedWriter(fws[i]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void close(){
		for(int i=0; i<NUM_PARTITIONS; i++){
			try {
				bws[i].flush();				
				fws[i].flush();
				bws[i].close();
				fws[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Partitioner partitioner = new Partitioner();
		partitioner.run();
		partitioner.close();

	}

}
