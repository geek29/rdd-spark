package beyondthewall.store;

import static com.jayway.jsonpath.JsonPath.parse;

import java.util.EnumSet;
import java.util.Set;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.Predicate;
import com.jayway.jsonpath.internal.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.internal.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class JsonPathEvaluator {
	
	static{
		Configuration.setDefaults(new Configuration.Defaults() {

		    private final JsonProvider jsonProvider = new JacksonJsonProvider();

		    @Override
		    public JsonProvider jsonProvider() {
		        return jsonProvider;
		    }

		    @Override
		    public MappingProvider mappingProvider() {
		        return new JacksonMappingProvider();
		    }

		    @Override
		    public Set<Option> options() {
		        return EnumSet.noneOf(Option.class);
		    }
		});
	}
	
	private static JsonProvider jsonProvider=Configuration.defaultConfiguration().jsonProvider();
	private JsonPath jsonPathCompiled=null;
	
	public JsonPathEvaluator(String jsonPath,Predicate... predicates){
		jsonPathCompiled = JsonPath.compile(jsonPath, predicates);
	}
	
	public JsonPathEvaluator(String jsonPath){		
		jsonPathCompiled = JsonPath.compile(jsonPath);
	}
	
	
	public JsonPathEvaluator(String jsonPath, Query query) {
		jsonPathCompiled = JsonPath.compile(jsonPath, query.getPredicates());
	}

	public Object eval(String json){
		Object document = jsonProvider.parse(json);
		Object result = jsonPathCompiled.read(document);
		return result;
	}	

}
