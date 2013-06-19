package rtree;

import java.io.Serializable;

public class PPoint implements Serializable
{
    /**
	 * 
	 */
	private static final long serialVersionUID = -7249178535555975815L;
	public int dimension;
    public float data[];
    public float distanz;

   /* public PPoint()
    {
        dimension = Constants.RTDataNode__dimension;
        data = new float[dimension];
        distanz = 0.0f;
    }*/
    
    public PPoint(int dimension)
    {
        this.dimension = dimension;
        data = new float[dimension];
        distanz = 0.0f;
    }
}