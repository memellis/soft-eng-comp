package org.soft.eng.com;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.*;

import au.com.bytecode.opencsv.CSVReader;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

@SuppressWarnings("serial")
public class Soft_eng_compServlet extends HttpServlet {
	private static final String APP_ENTITIES_AND_ENTITIES_DATA = "WEB-INF/data/app_entities_and_entity_data.csv";
	private static final String ENTITY_CSV_NAME = "entity";
	private static final String ENTTITY_FIELDS_CSV_NAME = "entity_fields";
	private static final String ENTITY_DATA_CSV_NAME="entity_data";
	private HashMap<String,  Entity> entities = new HashMap<String, Entity>();
	private HashMap<String, List<String>> entities_fieldnames = new HashMap<String, List<String>>();
	private int entityCount = 0; 
	public void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws IOException {
		
		DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
		CSVReader reader = new CSVReader(new FileReader(APP_ENTITIES_AND_ENTITIES_DATA));

		String [] nextLine;
	    while ((nextLine = reader.readNext()) != null) {
	        // nextLine[] is an array of values from the line
	    	int numLines = nextLine.length;
	    	System.out.println("Number of Data Items: " + numLines);
	    	for (int i = 0; i < numLines; i++)
            {
                System.out.println("     nextLine[" + i + "]:  " + nextLine[i]);
            }
	    	System.out.println("nextLine[0]"+nextLine[0]);
            parseItem(nextLine);
	    }
    	List<Entity> entities_list = new ArrayList<Entity>(entities.values());
    	
    	datastore.put(entities_list);
    	
    	System.out.println("Number of entities created =" +entities.size());
    	
    	Query q = new Query("member");
    	PreparedQuery pq = datastore.prepare(q);
    	
    	for (Entity result : pq.asIterable()) {
    		  String firstName = (String) result.getProperty("first_name");
    		  String lastName = (String) result.getProperty("last_name");
    		  String grade = (String) result.getProperty("grade");

    		  System.out.println(firstName + " " + lastName + ", " + grade);
    	}
    	
		resp.setContentType("text/plain");
		resp.getWriter().println("Hello, world");
	}
	private boolean parseItem(String [] line) {
		String cleanedString;
		cleanedString = removeWhiteSpace(line[0]);
        if (cleanedString.equals("entity")) {
        	boolean success = createEntity(line[1]);
        }
        else if (cleanedString.equals("entity_fields")) {
        	boolean success = loadEntityFields(line);
        }
        else if (cleanedString.equals("entity_data")) {
        	boolean success = loadEntityData(line);
        }
		return false;
	}
	private boolean createEntity(String entityName) {
		if (entityName != null) {
			if (! entities.containsKey(entityName)) {
				entities.put(entityName, new Entity(entityName));
			};
		}
		return false;
	}
	private boolean loadEntityFields(String [] line) {
		List<String> entity_fields = new ArrayList<String>();
		for (int i=2;i<line.length;i++) {
			entity_fields.add(removeWhiteSpace(line[i]));
		}
		entities_fieldnames.put(removeWhiteSpace(line[1]), entity_fields);
		return false;
	}
	private boolean loadEntityData(String [] line) {
		String entityName = removeWhiteSpace(line[1]);
		Entity currentEntity = entities.get(entityName);
		ArrayList<String> fieldnames = (ArrayList<String>) entities_fieldnames.get(entityName);
		ArrayList<String> fields = new ArrayList<String>();
		String key = new String("");
		for (int i=2; i<line.length; i++) {
			if(fieldnames.get(i-2).startsWith("key:")) {
				key = key + line[i];
			}
			fields.add(line[i]);
		}
		Key entityKey = KeyFactory.createKey(entityName, key);
		String currentFieldName;
		Entity myEntity = new Entity(entityName, entityKey);
		for (int i=0; i<fields.size();i++) {
			currentFieldName = fieldnames.get(i);
			if(currentFieldName.startsWith("key:")) {
				currentFieldName = currentFieldName.split("key:")[1];
			}
			myEntity.setProperty(currentFieldName, fields.get(i));
		}
		System.out.println("Putting entity"+entityName);
		entities.put(entityName+key, myEntity);
		return false;
	}
	private String removeWhiteSpace(String item) {
		return new String(item.replaceAll("\\s+", ""));
	}
}
