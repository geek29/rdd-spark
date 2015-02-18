package beyondthewall.store;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import com.jayway.jsonpath.InvalidPathException;

public class FileReader {

	private DataStoreConf conf=null;
	private String path=null;
	private ObjectInputStream ois = null;	
	private String jsonPath = null;
	private JsonPathEvaluator evaluator=null;
	public static final String EMPTY_JSON_PATH_RESULT="EMPTY_JSON_PATH_RESULT";
	
	public FileReader(DataStoreConf conf, String path, Collector collector, Query query){
		this.conf = conf;
		this.path = path;		
		this.jsonPath = query.getJsonPath();
		if(jsonPath!=null){
			try {
				this.evaluator = new JsonPathEvaluator(jsonPath, query);
			} catch (InvalidPathException e) {
				throw new StoreException(e);
			}
		}
		open();
	}
	
	public void open() {
		try {
			ois = new ObjectInputStream(conf.getFileHelper().openInputStream(path));
		} catch (FileNotFoundException e) {
			throw new StoreException(e);
		} catch (IOException e) {
			throw new StoreException(e);
		}		
	}
	
	public Object readNext(){
		Object obj;
		try {
			obj = apply(jsonPath,ois.readObject());
			if (obj != null)
				return obj;
			else {
				ois.close();
				return null;
			}
		} catch (ClassNotFoundException e) {
			throw new StoreException(e);
		} catch(EOFException e){			
			return null;
		}catch (IOException e) {
			throw new StoreException(e);
		}		
	}

	private Object apply(String jsonPath, Object readObject) {
		if(jsonPath!=null){
			assert readObject instanceof String;
			String json = (String)readObject;
			Object evalResult = evaluator.eval(json);
			if(evalResult instanceof List){
				List list = (List) evalResult;
				if(list.size()==1)
					return list.get(0);
				else if(list.size()==0)
					return EMPTY_JSON_PATH_RESULT;
				else return list;
			} else
				return evalResult;
		} else {
			return readObject;
		}
	}
	
	/*
	public void start() {
		try {
			while (true) {
				Object obj = ois.readObject();
				if (obj != null)
					collector.collect(obj);
				else
					break;
			}
			ois.close();
		} catch (IOException ioe) {
			throw new StoreException(ioe);
		} catch (ClassNotFoundException e) {
			throw new StoreException(e);
		}
	}*/
	

}
