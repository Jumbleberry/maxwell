package com.jumbleberry.kinesis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.json.JSONObject;

public class AvroData {
	private final String schemaDirectory = "/schemas/";
	private final String schemaSuffix = "Mutation.avsc";
	private final Schema schema;
	private static final String[] schemaDataTypes = {"strings", "integers", "longs", "bytes"};
	
	private GenericRecord record;
	
	public AvroData(String rowType) throws IOException {
		schema = getSchema(ucfirst(rowType) + schemaSuffix);
		record = new GenericData.Record(schema);
	}
	
	/**
	 * Sort data into buckets for the schema
	 * 
	 * @param HashMap<String, String> tableSchema 
	 * @param LinkedHashMap<String, Object> data
	 * @return HashMap<String, HashMap<String, Object>>
	 */
	public static HashMap<String, HashMap<String, Object>> sortData(HashMap<String, String> tableSchema, LinkedHashMap<String, Object> data) {
		// Create buckets
		HashMap<String, Object> strings = new HashMap<String, Object>();
		HashMap<String, Object> integers = new HashMap<String, Object>();
		HashMap<String, Object> longs = new HashMap<String, Object>();
		HashMap<String, Object> bytes = new HashMap<String, Object>();
		
		// Sort mySQL data types into buckets
		for ( String key: data.keySet() ) {
			switch (tableSchema.get(key).toString()) {
				case "decimal":
				case "float":
				case "double":
				case "null":
				case "date":
				case "time":
				case "year":
				case "varchar":
				case "timestamp":
				case "datetime":
				case "enum":
				case "set":
				case "tinyblob":
				case "mediumblob":
				case "longblob":
				case "blob":
				case "text":				
					strings.put(key, data.get(key).toString());
					break;
				case "tinyint":
				case "smallint":
				case "int":
					integers.put(key, data.get(key));
					break;
				case "bigint":
					longs.put(key, data.get(key));
					break;
				default:
					strings.put(key, data.get(key).toString());
					break;
			}
			
		}			
		
		// Create a master bucket
		HashMap<String, HashMap<String, Object>> typeContainer = new HashMap<String, HashMap<String, Object>>();
		typeContainer.put("strings", strings);
		typeContainer.put("integers", integers);
		typeContainer.put("longs", longs);
		typeContainer.put("bytes", bytes);		
		
		return typeContainer;
	}
	
	/**
	 * Transform the record to a byte array
	 * 
	 * @return byte[]
	 * @throws IOException
	 */
	public byte[] toByteArray() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BinaryEncoder bencoder = EncoderFactory.get().binaryEncoder(baos, null);
		
		// Create the writer
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);								
		
		// Write the table name (so the reader knows what schema to use) and the data		
		JSONObject jsonObject = new JSONObject(schema.toString()); // Apparently a schema cannot access it's own properties
		bencoder.writeString(jsonObject.getString("name"));
		datumWriter.write(record, bencoder);						
		
		// Write to the output stream
		bencoder.flush();		
						
		return baos.toByteArray();		
	}			
	
	/**
	 * Get a schema for the type of mySQL action
	 * 
	 * @param String rowType
	 * @return 
	 * @throws IOException
	 */
	private Schema getSchema(String schemaFile) throws IOException {				
		return new Schema.Parser().parse(AvroData.class.getResourceAsStream(schemaDirectory + schemaFile));								
	}	
	
	/**
	 * Set the value for the field of the record
	 * 
	 * @param String key
	 * @param Object value
	 */
	public void put(String field, Object value) {
		record.put(field, value);
	}
		
	/**
	 * Return the GenericRecord
	 * 
	 * @return GenericRecord
	 */
	public GenericRecord getRecord() {
		return this.record;
	}
	
	/**
	 * Return the Schema
	 * 
	 * @return Schema
	 */
	public Schema getSchema() {
		return this.schema;
	}
	
	/**
	 * Get the list of data types for the schema
	 * 
	 * @return String[]
	 */
	public static String[] getSchemaDataTypes() {
		return schemaDataTypes;
	}
	
	/**
	 * Java implementation of PHP ucfirst because Dave misses it
	 * 
	 * @param String subject
	 * @return String
	 */
	final public static String ucfirst(String subject) {
	    return Character.toUpperCase(subject.charAt(0)) + subject.substring(1);
	}		
}
