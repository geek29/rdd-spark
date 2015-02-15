package beyondthewall.store;

public class SegmentKey implements Comparable<SegmentKey>{
	
	private String name;
	private long ts;
	
	public SegmentKey(String name,long ts){
		this.name = name;
		this.ts = ts;
	}

	@Override
	public int compareTo(SegmentKey o) {
		int diff = this.name.compareTo(o.name);
		if(diff==0){
			return (int)(this.ts - o.ts);
		}else 
			return diff;
	}
	
	static final int prime = 31;
	
	public int hashCode(){
		return (int)(name.hashCode()*prime + ts);
	}
	
	public boolean equals(Object other){
		if(this==other)
			return true;
		if(other instanceof SegmentKey){
			SegmentKey otherKey = (SegmentKey) other;
			return (this.name.equals(otherKey.name) && this.ts == otherKey.ts);
		}
		else return false;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}
	
	

}
