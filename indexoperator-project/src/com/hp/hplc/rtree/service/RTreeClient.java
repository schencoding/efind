/*
  PROGRAMME:
  PROGRAMMER:
  DATE:
  PROJECT:
  MODIFIED DATE:

*/
package com.hp.hplc.rtree.service;

import java.io.*;
import java.util.Vector;
import java.net.*;

import rtree.PPoint;

public class RTreeClient 
{
  private Socket socket;
  public RTreeClient(String host,int port)
  {
    try
      {
        socket=new Socket(host,port);
      }
    catch(Exception e)
      {
        e.printStackTrace();
      }
  }
 
  public String[] nearestSearch(PPoint pt,int in) 
  {
    try
      {
        String command="nearest";
        Object obj[]={pt.dimension, pt.data[0], pt.data[1], new Integer(in)};
        Object[] response=sendRequest(command,obj);
        if(response!=null)
          {
            String abl[]=new String[response.length];
            for(int i=0;i<response.length;i++)
              {
                abl[i]=(String)response[i];
              }
            return abl;
          }     
        else
          return null;

      }
    catch(Exception e)
      {
        e.printStackTrace();
      }
    return null;
  }
 
  private Object[] sendRequest(String command,Object[] param) 
  {
    try
      {
        /*ByteArrayOutputStream bout=new ByteArrayOutputStream ();
        ObjectOutputStream out=new ObjectOutputStream (bout);
        //                      ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
        OutputStream fout=socket.getOutputStream ();
        out.writeObject(command);
        if(param!=null)
          {
            out.writeObject(new Integer (param.length));
            for(int i=0;i<param.length;i++)
              {
                out.writeObject(param[i]);
              }
          }
        else
          {
            out.writeObject(new Integer(0));
          }
        out.flush();
        out.close();

        //long time = System.currentTimeMillis();
        fout.write(bout.toByteArray());
        fout.flush();
        */
    	
    	//-------------------------begin John----------------
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(command);
			if (param != null) {
				out.writeObject(new Integer(param.length));
				for (int i = 0; i < param.length; i++) {
					out.writeObject(param[i]);
				}
			} else {
				out.writeObject(new Integer(0));
			}
			out.flush();
			//out.close();
    	//-------------------------end John----------------

        //                      InputStream in=socket.getInputStream();
        /*
          while(in.available()==0)
          {
          Thread.sleep(10);
          }
          int rTotal=in.available();
          System.out.println("RTotal..........."+rTotal);
          byte[] bb=new byte[in.available()];
          in.read(bb,0,in.available());
          byte ln[]=new byte[10];
          System.arraycopy(bb,0,ln,0,10);
          ByteArrayInputStream lin=new ByteArrayInputStream(ln);
          ObjectInputStream loin=new ObjectInputStream(lin);
          int total=loin.readInt();
          byte bbf[]=new byte[rTotal];
          System.out.println(total+"..."+bbf.length);
          System.arraycopy(bb,10,bbf,0,rTotal-10);
          int pos=bbf.length;
          System.out.println("Postion................."+pos);
          while(rTotal<total)
          {
          int avail=in.available();
          while(avail==0)
          {
          avail=in.available();
          }
          byte temp[]=new byte[avail];
          in.read(temp,0,avail);
          byte temp1[]=new byte[avail+bbf.length];
          System.arraycopy(bbf,0,temp1,0,bbf.length);
          System.arraycopy(temp,0,temp1,bbf.length,temp.length);
          bbf=temp1;
          rTotal+=avail;
          System.out.println(bbf.length+"______________________");
          System.arraycopy(temp,0,bbf,pos,temp.length);
          pos+=avail;
          Thread.sleep(10);
          }

          
        */
        
        
        InputStream in=socket.getInputStream();
        //System.out.println("RTreeClient.sendRequest : time to receive response " 
        //+ (System.currentTimeMillis() - time));
        //time = System.currentTimeMillis();
//---------------------begin John-----------------
        
			ObjectInputStream ois = new ObjectInputStream(in);
			Boolean bool = (Boolean) ois.readObject();
			//System.out.println(bool);
			if (bool.booleanValue()) {
				Integer count = (Integer) ois.readObject();
				//System.out.println(count.intValue());
				Object[] objects = new Object[count.intValue()];
				for (int j = 0; j < count.intValue(); j++) {
					objects[j] = ois.readObject();
				}
				return objects;
			} else {
				return null;
			}
//----------------------end John------------------
     
/*        byte bbf[]=new byte[100];

        int i=0;
        int total=-1;
        while(true)
          {
            int val=in.read(bbf,i,1);
            if(val==-1)
              break;
            i++;
            if(i==10)
              {
                byte ln[]=new byte[10];
                System.arraycopy(bbf,0,ln,0,10);
                ByteArrayInputStream lin=new ByteArrayInputStream(ln);
                ObjectInputStream loin=new ObjectInputStream(lin);
                total=loin.readInt();
                byte temp[]=new byte[total];
                System.arraycopy(bbf,0,temp,0,bbf.length);
                bbf=temp;
              }
            if(i>=total && total!=-1)
              {
                //System.out.println("read completed..."+bbf.length);
                break;
              }
          }

        byte tby[]=new byte[bbf.length-10];
        System.arraycopy(bbf,10,tby,0,tby.length);
        ByteArrayInputStream bin=new ByteArrayInputStream(tby);
        ObjectInputStream oIn=new ObjectInputStream(bin);
        Boolean bool=(Boolean) oIn.readObject();
        //System.out.println("boolean.........."+bool);
        Integer count=(Integer) oIn.readObject();
        //System.out.println("count.........."+count);
        if(bool.booleanValue())
          {
            Object[] obj=new Object[count.intValue()];
            for(int j=0;j<count.intValue();j++)
              {
                obj[j]=oIn.readObject();
              }
            //                              oIn.close();
            //System.out.println("RTreeClient.sendRequest : time to process input data " 
            //         + (System.currentTimeMillis() - time));
            
            return obj;
          }
        else
          {
            throw (Exception) oIn.readObject();
          }*/
      }
    catch(Exception e)
      {
        e.printStackTrace();
      }
	return null;
  }
  /**
     public static void main(String args[])
     {

     try
     {
     RTreeClient cln=new RTreeClient("localhost",7001);
     System.out.println("Connection established1...");
     Rect rect=cln.getTreeMBR();
     System.out.println("........."+rect);
     Vector v=cln.getAllElements();//contains(new Rect(0,0,1000,1000));
     System.out.println(v.size());
     System.out.println(v.get(0));
     }
     catch(Exception e)
     {
     e.printStackTrace();
     }
     } */

public void stop() {
	try {
		socket.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}
}
