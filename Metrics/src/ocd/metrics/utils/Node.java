package ocd.metrics.utils;

import java.util.HashMap;

public class Node {
	int index;
	HashMap<Integer, Double> ownCommunities = new HashMap<Integer, Double>();
	int outDegree=0;
	int inDegree=0;
	
	public int getOutDegree() {
		return outDegree;
	}

	public void setOutDegree(int outDegree) {
		this.outDegree = outDegree;
	}

	public int getInDegree() {
		return inDegree;
	}

	public void setInDegree(int inDegree) {
		this.inDegree = inDegree;
	}

	public Node(int index) {
		super();
		this.index = index;
		this.ownCommunities=new HashMap<Integer, Double>();
	}
	
	public HashMap<Integer, Double> getOwnCommunities() {
		return ownCommunities;
	}

	public void addCommunity(Integer key, Double value) {
		this.ownCommunities.put(key, value);
	}
	
	public void removeCommunity(Integer key) {
		this.ownCommunities.remove(key);
	}
	
	public void clearCommunity() {
		this.ownCommunities.clear();
	}
	
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
	
	@Override
	public boolean equals(Object obj) {
		boolean res=false;
		if (obj != null && getClass() == obj.getClass()) {
			final Node other = (Node) obj;
	        if(this.getIndex()==other.getIndex()){
	        	res=true;
	        }
	    }
		
		return res;
	}
	@Override
    public int hashCode() {
         String result = ""+this.index ;
        return result.hashCode();
    }

	public void setOwnCommunities(HashMap<Integer, Double> ownCommunities) {
		this.ownCommunities=ownCommunities;
		
	}
}
