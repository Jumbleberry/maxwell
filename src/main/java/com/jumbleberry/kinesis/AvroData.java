package com.jumbleberry.kinesis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class AvroData {
	private static final String schemaDirectory = "/schemas/";
	private static final String schemaSuffix = "Mutation.avsc";
	private final Schema schema;
	private static final String[] schemaDataTypes = {"strings", "integers", "longs", "bytes"};	

	private GenericRecord record;
	private HashMap<String, String> header;
	private static ConcurrentHashMap<String, Schema> avroSchemas = new ConcurrentHashMap<String, Schema>(8);

	public AvroData(String rowType) throws IOException {
		String schemaName = ucfirst(rowType) + schemaSuffix;

		this.schema = getSchema(schemaName);
		this.record = new GenericData.Record(schema);				

		this.header = new HashMap<String, String>();
		this.header.put("schema", schemaName);
		this.header.put("action", rowType);
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
			Object val = data.get(key);

			switch (tableSchema.get(key).toString()) {
				case "tinyint":
				case "smallint":
				case "int":
					integers.put(key, val == null? null: Integer.parseInt(val.toString()));
					break;
				case "bigint":
					longs.put(key, val == null? null: Long.parseLong(val.toString()));
					break;
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
				default:
					strings.put(key, val == null? null: val.toString());
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

		// Write header
		bencoder.setItemCount(header.size());
		bencoder.writeMapStart();
		for (String key : header.keySet() ) {
			bencoder.startItem();
			bencoder.writeString(key);
			bencoder.writeString(header.get(key));
		}
		bencoder.writeMapEnd();				

		// Write record
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
	private static Schema getSchema(String schemaFile) throws IOException {
		String path = schemaDirectory + schemaFile;
		
		if (!avroSchemas.containsKey(path))
			avroSchemas.putIfAbsent(path, new Schema.Parser().parse(AvroData.class.getResourceAsStream(path)));
		
		return avroSchemas.get(path);
	}	

	/**
	 * Set the value for the field of the record
	 * 
	 * @param String key
	 * @param Object value
	 */
	public void put(String field, Object value) {
		this.record.put(field, value);
	}

	/**
	 * Set the value for the file of a specific record
	 * 
	 * @param record
	 * @param field
	 * @param value
	 */
	public void put(GenericRecord record, String field, Object value) {
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
	 * Return the record for a field
	 * 
	 * @param field
	 * @return
	 */
	public GenericRecord getSubRecord(String field) {
		GenericRecord subRecord = new GenericData.Record(this.schema.getField(field).schema());

		return subRecord;
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

	/**
	 * Deserialize an Avro byte array
	 * 
	 * @param byte[] data
	 * @return String
	 * @throws IOException
	 */
	public static String deserializeByteArray(byte[] data) throws IOException {
		String response = "";						

		// Create a binary decoder to read the byte array
		BinaryDecoder bdecoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);									

		// While not EOF
		while (!bdecoder.isEnd()) {
			// Read header
			Map<String, String> header = new HashMap<String, String>();												
			for(long i = bdecoder.readMapStart(); i != 0; i = bdecoder.mapNext()) {							
				for (long j = 0; j < i; j++) {							
					String key = bdecoder.readString();
					String value = bdecoder.readString();

					header.put(key, value);
				}
			}

			Schema writeSchema = getSchema(header.get("schema"));
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(writeSchema);			

			// Read the entry
			response += datumReader.read(null, bdecoder);
		}

		return response;		
	}
}