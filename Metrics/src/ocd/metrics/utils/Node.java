package ocd.metrics.utils;

public class Node {
	int index;

	public Node(int index) {
		super();
		this.index = index;
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
        int result = 0;
        result = (int) (this.index / 11);
        return result;
    }
}
