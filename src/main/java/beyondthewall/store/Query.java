package beyondthewall.store;

import java.io.Serializable;
import java.util.Set;

import com.jayway.jsonpath.Predicate;

public class Query implements Serializable{
	
	private long from, to;
	private String jsonPath=null;
	private String segment=null;
	private Set<String> segmentFilter=null;
	private int numThreads=1;
	
	public Query(long t1,long t2){
		this.from = t1;
		this.to = t2;
	}
	
	public static Query range(long ts1, long ts2){
		return new Query(ts1,ts2);
	}
	
	public Query jsonPath(String jsonPath){
		this.jsonPath = jsonPath;
		return this;
	}
	
	public Query segment(String seg){
		this.segment = seg;
		return this;
	}
	
	public Query forSegments(Set<String> segments){
		this.segmentFilter = segments;
		return this;
	}

	public long getFrom() {
		return from;
	}

	public long getTo() {
		return to;
	}

	public String getJsonPath() {
		return jsonPath;
	}

	public String getSegment() {
		return segment;
	}

	public Set<String> getSegmentFilter() {
		return segmentFilter;
	}

	public Predicate getPredicates() {		
		return null;
	}	

}
