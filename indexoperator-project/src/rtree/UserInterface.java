package rtree;
/* R* Tree Implementation - COMP630C Project
   Implementing User Interface of the program
   Written by Josephine Wong 23 November 1997
*/

import java.awt.*;
import java.awt.event.*;

import com.hp.hplc.rtree.service.QueryProcessor;

public class UserInterface extends Frame implements ActionListener {

    public UserInterface () {
        setTitle("R* Tree Creation");

        Panel textPanel = new Panel();
        setFont(new Font("Times", Font.BOLD, 16));
        textPanel.add(new Label("Please enter details of the R* Tree:"));
        add("North", textPanel);

        Panel inputPanel = new Panel();
        setFont(new Font("Times", Font.PLAIN, 16));
        inputPanel.setLayout(new GridLayout(7, 2));
        inputPanel.add(new Label("Tree filename:"));
        inputPanel.add(filename = new TextField("myTree", 10));
        inputPanel.add(new Label("Maximum coordinates:"));
        inputPanel.add(maxCoord = new TextField("300", 10));
        inputPanel.add(new Label("Maximum width:"));
        inputPanel.add(maxWidth = new TextField("60", 10));
        inputPanel.add(new Label("Number of Rectangles:"));
        inputPanel.add(numRects = new TextField("200", 10));
        inputPanel.add(new Label("Dimension:"));
        inputPanel.add(dimension = new TextField("2", 10));
        inputPanel.add(new Label("BlockLength:"));
        inputPanel.add(blockLength = new TextField("256", 10));
        inputPanel.add(new Label("Cache Size:"));
        inputPanel.add(cacheSize = new TextField("128", 10));
        add("Center", inputPanel);

        Panel buttonPanel = new Panel();
        Button createButton = new Button("Create");
        createButton.addActionListener(this);
        buttonPanel.add(createButton);
        Button loadButton = new Button("Load");
        loadButton.addActionListener(this);
        buttonPanel.add(loadButton);
        Button exitButton = new Button("Exit");
        exitButton.addActionListener(this);
        buttonPanel.add(exitButton);
        add("South", buttonPanel);

        setSize(400, 300);
        setLocation(100, 100);
    }

    public static int getMAXCOORD () {
        return MAXCOORD;
    }

    public static int getMAXWIDTH () {
        return MAXWIDTH;
    }

    public static int getNUMRECTS () {
        return NUMRECTS;
    }

    public static int getDIMENSION () {
        return DIMENSION;
    }

    public static int getBLOCKLENGTH () {
        return BLOCKLENGTH;
    }

    public static int getCACHESIZE () {
        return CACHESIZE;
    }

    public boolean handleEvent (Event e) {
        if (e.id == Event.WINDOW_DESTROY && e.target == this)
            System.exit(0);
        return super.handleEvent(e);
    }

    public void actionPerformed (ActionEvent e) {
        if (e.getActionCommand().equals("Create")) {
            if (!filename.getText().equals(""))
            {
                processInput();
                hide();
                TreeCreation tc = new TreeCreation(filename.getText()+".rtr",NUMRECTS, DIMENSION, BLOCKLENGTH, CACHESIZE);
            }
        }
        else if (e.getActionCommand().equals("Load")) {
            FileDialog fd = new FileDialog(this, "Choose an rtree");
            fd.show();
            String fname = fd.getFile();
            if ((fname != null) && (!fname.equals("")))
            {
                hide();
                TreeCreation tc = new TreeCreation(fname,CACHESIZE);
            }
        }
        else if (e.getActionCommand().equals("Exit"))
            System.exit(0);
    }
            
    private void processInput () {
        try {
            MAXCOORD = Integer.parseInt(maxCoord.getText());
            MAXWIDTH = Integer.parseInt(maxWidth.getText());
            NUMRECTS = Integer.parseInt(numRects.getText());
            DIMENSION = Integer.parseInt(dimension.getText());
            BLOCKLENGTH = Integer.parseInt(blockLength.getText());
            CACHESIZE = Integer.parseInt(cacheSize.getText());
        }
        catch (NumberFormatException e) {
            requestFocus ();
        }
    }

    public static int MAXCOORD = 300*1000;
    public static int MAXWIDTH = 600000;
	public static int NUMRECTS = 2 * 100 * 1000;
    public static int DIMENSION = 2;
    public static int BLOCKLENGTH = 512;//256;
    public static int CACHESIZE = 1 * 1024 * 1024;
    private Object controller;
    private TextField filename,maxCoord, maxWidth, numRects, dimension, blockLength, cacheSize;

 /*   public static void main (String [] args) {
        UserInterface ui = new UserInterface();
        ui.show();
    }*/
    
	public static void main(String[] args) {
		//System.out.println("Loading " + args[0] + "...");
		//TreeCreation tc = new TreeCreation("test.rtr", args[0], DIMENSION, BLOCKLENGTH, CACHESIZE);
		TreeCreation tc = new TreeCreation("test.rtr", NUMRECTS, DIMENSION, BLOCKLENGTH, CACHESIZE);
		System.out.println("Finished loading data.");
		for (int i = 0; i < 100; i++) {
			PPoint p = new PPoint(2);
			p.data[0] = 0 + i * 100;
			p.data[1] = 0 + i * 100;

			System.out.println("Query point:" + p.data[0] + " " + p.data[1]);
			SortedLinList res = new SortedLinList();
			long start = System.currentTimeMillis();
			//tc.rt.point_query(p, res);
			//tc.rt.rangeQuery(p, 200, res);
			tc.rt.k_NearestNeighborQuery(p, 10, res);
			long end = System.currentTimeMillis();
			System.out.println("Time used: " + (end - start));
			System.out.println("# of data: " + res.get_num());
			System.out.println(res);
		}
		
		QueryProcessor qp = new QueryProcessor(tc.rt);
		qp.start();
		
	}

}
