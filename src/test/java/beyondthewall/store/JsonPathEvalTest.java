package beyondthewall.store;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import com.jayway.jsonpath.Filter;

public class JsonPathEvalTest extends TestCase{

	@Test
	public void testSimpleJson() throws IOException {
		JsonPathEvaluator eval = new JsonPathEvaluator("$.store.book[0].author");
		Object result = eval.eval(readJson());
		assertEquals("Nigel Rees", result);
		eval = new JsonPathEvaluator("$.store.book[1].author");
		result = eval.eval(readJson());
		assertEquals("Evelyn Waugh", result);
	}
	
	@Test
	public void testPredicate() throws IOException {
		Filter cheapFictionFilter = filter(where("category").is("fiction").and("price").lte(10D));
		JsonPathEvaluator eval = new JsonPathEvaluator("$.store.book[?]", cheapFictionFilter);		
		Object result = eval.eval(readJson());
		assertEquals(1,((List)result).size());
		
		String json = "{ \"id\": 2 , \"name\" : \"Name0\" }";
		eval = new JsonPathEvaluator("[?(@.id < 0)].id");		
		result = eval.eval(json);
		System.out.println("Result1 : " + result);
		assertEquals(0,((List)result).size());
		
		eval = new JsonPathEvaluator("[?(@.id < 10)].id");		
		result = eval.eval(json);
		System.out.println("Result2 : " + result);
		assertEquals(1,((List)result).size());
		
	}

	private String readJson() throws IOException{
		File file = new File("/work/scalaSDK/workspace/rdd-spark/src/test/java/test.json");		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		char[] bytes = new char[(int) file.length()];
		reader.read(bytes,0,bytes.length);		
		reader.close();
		String json = new String(bytes);
		return json;
	}

}
