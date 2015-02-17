package beyondthewall.store;

import java.util.Collection;

@SuppressWarnings("rawtypes")
public class TestResultCollector implements Collector{		
	private Collection list =null;
	private boolean DEBUG = false;
	
	public TestResultCollector(Collection resultList, boolean flag){
		this.list = resultList;
		this.DEBUG = flag;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collect(SegmentKey key, Object object) {
		if(DEBUG)
			System.out.println("Read " + object + " segment=" + key.getName() + " slice="+ key.getTs());
		list.add(object);				
	}

	public Collection getList() {
		return list;
	}	
	
}
