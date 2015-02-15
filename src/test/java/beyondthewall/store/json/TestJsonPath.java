package beyondthewall.store.json;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.internal.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.internal.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

public class TestJsonPath {
	
	public static void main(String[] args) throws IOException {		
		File file = new File("/work/scalaSDK/workspace/rdd-spark/src/test/java/test.json");		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		char[] bytes = new char[(int) file.length()];
		reader.read(bytes,0,bytes.length);		
		reader.close();
		String json = new String(bytes);
		
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
		
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);

		String author0 = JsonPath.read(document, "$.store.book[0].author");
		String author1 = JsonPath.read(document, "$.store.book[1].author");
		System.out.println("Author0 =" + author0);
		System.out.println("Author1 =" + author1);
		
		Filter cheapFictionFilter = filter(where("category").is("fiction").and("price").lte(15D));
		List<Map<String, Object>> books =  parse(json).read("$.store.book[?]", cheapFictionFilter);
		System.out.println("Filtered books " + books);
	}

}
