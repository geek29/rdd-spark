package beyondthewall.store;

public interface Collector {
	
	//public void startSegment(String name);
	
	//public void endSegment(String name);
	
	public void collect(SegmentKey key, Object object);

}
