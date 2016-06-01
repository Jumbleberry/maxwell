package com.jumbleberry.kinesis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.json.JSONArray;
import org.json.JSONObject;

public class Serializer {
	// TODO: fix this
	private static final String homeDirectory = "/home/dave/JB-Maxwell/src/main/resources/schemas/";
	
	public static HashMap<String, HashMap<String, Object>> sortData(HashMap<String, String> tableSchema, LinkedHashMap<String, Object> data) {
		HashMap<String, Object> strings = new HashMap<String, Object>();
		HashMap<String, Object> integers = new HashMap<String, Object>();
		HashMap<String, Object> longs = new HashMap<String, Object>();
		HashMap<String, Object> bytes = new HashMap<String, Object>();
		
		for ( String key: data.keySet() ) {
			// TODO: Update this list
			switch (tableSchema.get(key).toString()) {
				case "varchar":
				case "datetime":
				case "mediumblob":
					strings.put(key, data.get(key));
					break;
				case "int":
					integers.put(key, data.get(key));
					break;
			}
			
		}			
		
		HashMap<String, HashMap<String, Object>> typeContainer = new HashMap<String, HashMap<String, Object>>();
		typeContainer.put("strings", strings);
		typeContainer.put("integers", integers);
		typeContainer.put("longs", longs);
		typeContainer.put("bytes", bytes);		
		
		return typeContainer;
	}
	
	public static byte[] serializeAvro(String rowType, JSONObject json) throws IOException {			
		Schema schema = getSchema(rowType);		
		GenericRecord record = new GenericData.Record(schema);			
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BinaryEncoder bencoder = EncoderFactory.get().binaryEncoder(baos, null);
		
		// Create the writer
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);								
		
		// Write the table name (so the reader knows what schema to use) and the data
		bencoder.writeString(ucfirst(rowType));
		datumWriter.write(serializeJSON(schema, json, record), bencoder);						
		
		// Write to the output stream
		bencoder.flush();		
						
		return baos.toByteArray();
	}	
	
	private static GenericRecord serializeJSON(Schema schema, JSONObject jsonObject, GenericRecord record) {						
		for (Iterator<String> i = jsonObject.keys(); i.hasNext();) {					
			String key = i.next();

			// If the class of the current value is a json object we need to do this again
			if (jsonObject.get(key).getClass() == JSONObject.class) {
				GenericRecord subRecord = new GenericData.Record(schema.getField(key).schema());				
				record.put(key, serializeJSON(schema, jsonObject.getJSONObject(key), subRecord));
			// Handle arrays/maps
			} else if (jsonObject.get(key).getClass() == JSONArray.class) {											
				Map<String, Object> map = new HashMap<String, Object>();							
				JSONArray jsonArray = (JSONArray) jsonObject.get(key);
				
				for (int j = 0; j < jsonArray.length(); j++) {
					JSONObject mapObject = jsonArray.getJSONObject(j);
					
					for (Iterator<String> k = mapObject.keys(); k.hasNext();) {
						String mapObjectKey = k.next();																	
						
						map.put(mapObjectKey, mapObject.get(mapObjectKey));
					}
				}
								
				record.put(key, map);
			// Or else we can just add it to the record				
			} else {								
				if (record.getSchema().getField(key).schema().getName().equals("string"))
					record.put(key, jsonObject.get(key));
				else if (record.getSchema().getField(key).schema().getName().equals("long"))
					record.put(key, new Long(jsonObject.get(key).toString()));
				
			}			
		}
		
		return record;
	}		
	
	private static Schema getSchema(String rowType) throws IOException {				
		String schemaFile = homeDirectory + ucfirst(rowType) + "Mutation.avsc";			

		return new Schema.Parser().parse(new File(schemaFile));						
	}	
	
	final public static String ucfirst(String subject) {
	    return Character.toUpperCase(subject.charAt(0)) + subject.substring(1);
	}	
}
