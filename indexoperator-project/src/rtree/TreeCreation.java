package rtree;
/* Modified from Test.java created by Nikos
*/

import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TreeCreation {
   

	
	
	 public TreeCreation (String filename, String inputFileName, int dimension, int blockLength, int cacheSize) {
	        this.dimension = dimension;
	        this.blockLength = blockLength;
	        this.cacheSize = cacheSize;
	        
	        // initialize tree
	        rt = new RTree(filename, blockLength, cacheSize, dimension);

	        // insert random data into the tree
	        Data d;
	        FileReader fr = null;
	        BufferedReader br = null; 
			try {
				fr = new FileReader(inputFileName);
				br = new BufferedReader(fr);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
	       
			String line = null;
			int counter = 0; 
			try {
				while((line = br.readLine()) != null){
					counter++;
					
					String[] fields = line.split("\\t");
					if(counter%1000==0){
						System.out.println("[" + counter + "]");
					}
				    // create a new Data with dim=dimension
					long id = Long.parseLong(fields[0]);
				    d = new Data(dimension, id);
				    // create a new rectangle
				    float fx = Float.parseFloat(fields[1]);
				    float fy = Float.parseFloat(fields[2]);
				    int x = (int) (fx*10000000.0);
				    int y = (int) (fy*10000000.0);
				    rectangle r = new rectangle(id, x, y);
				    // copy the rectangle's coords into d's data
				    d.data = new float[dimension*2];
				    d.data[0] = (float)r.LX;
				    d.data[1] = (float)r.UX;
				    d.data[2] = (float)r.LY;
				    d.data[3] = (float)r.UY;
				    d.id = id;
				    
				    rt.insert(d);
				   
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("" + counter + " records loaded.");
	    }
	 
	 
    public TreeCreation (String filename, int numRects, int dimension, int blockLength, int cacheSize) {
        this.numRects = numRects;
        this.dimension = dimension;
        this.blockLength = blockLength;
        this.cacheSize = cacheSize;
        
        // initialize tree
        rt = new RTree(filename, blockLength, cacheSize, dimension);

        // insert random data into the tree
        Data d;
        for (int i=0; i<numRects; i++)
        {
        	if(i%1000==0){
        		System.out.println("[" + i + "]");
        	}
            // create a new Data with dim=dimension
            d = new Data(dimension, i);
            // create a new rectangle
            rectangle r = new rectangle(i);
            // copy the rectangle's coords into d's data
            d.data = new float[dimension*2];
            d.data[0] = (float)r.LX;
            d.data[1] = (float)r.UX;
            d.data[2] = (float)r.LY;
            d.data[3] = (float)r.UY;
            d.id = i;
           // d.data[0] = (float) r.LX;
          //  d.data[1] = (float) r.LY;
            //d.print();
            rt.insert(d);
        }
        
        // Create the Query Result Window
        //qf = new QueryFrame(rt);
        //qf.show();
        //qf.move(400, 0);

        // Create the Rectangle Display Window
       // f = new RectFrame(this);
       // f.pack();
        //f.show();

    }
    
    TreeCreation (String filename, int cacheSize) {
        //this.numRects = numRects;
        //this.dimension = dimension;
        //this.blockLength = blockLength;
        this.cacheSize = cacheSize;
        
        // initialize tree
        rt = new RTree(filename, cacheSize);

        // Create the Rectangle Display Window
       // f = new RectFrame(this);
        //f.pack();
       // f.show();

    }
    
    public void exit(int exitcode)
    {
        if ((rt != null) && (exitcode == 0))
            rt.delete();
        System.exit(0);
    }

    public RTree rt;
    public RectFrame f;
    //public QueryFrame qf;
    public int displaylevel = 199;
    private int numRects, dimension, blockLength, cacheSize;
}