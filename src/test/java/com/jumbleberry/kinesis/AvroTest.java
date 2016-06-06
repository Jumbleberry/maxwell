package com.jumbleberry.kinesis;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.junit.Test;

import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class AvroTest {	
	private final Table table;
		
	private List<ColumnDef> columnList;	
	
	public AvroTest() {		
		table = new Table();
		table.setDatabase("test");		
		table.rename("test");
		createTableSchema(columnList);				
	}
	
	@Test
	public void TestSerializeData() throws IOException {		
		RowMap r1 = new RowMap("insert", table, 1L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1"));
		createData(r1);
		AvroData avroData1 = r1.toAvro();		
		JSONObject s1 = new JSONObject(avroData1.getRecord().toString());		
		JSONObject d1 = new JSONObject(AvroData.deserializeByteArray(avroData1.toByteArray()));
		assertThat(d1.toString(), is(s1.toString()));
		
		RowMap r2 = new RowMap("update", table, 1L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1"));
		createData(r2);
		createOldData(r2);
		AvroData avroData2 = r2.toAvro();
		JSONObject s2 = new JSONObject(avroData2.getRecord().toString());
		JSONObject d2 = new JSONObject(AvroData.deserializeByteArray(avroData2.toByteArray()));									
		assertThat(d2.toString(), is(s2.toString()));
		
		RowMap r3 = new RowMap("delete", table, 1L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1"));
		createData(r3);
		AvroData avroData3 = r3.toAvro();						
		JSONObject s3 = new JSONObject(avroData3.getRecord().toString());
		JSONObject d3 = new JSONObject(AvroData.deserializeByteArray(avroData3.toByteArray()));		
		assertThat(d3.toString(), is(s3.toString()));		
	}	
	
	protected void createTableSchema(List<ColumnDef> columnList) {
		String[] empty = {};		
		ColumnDef[] colList = {
			ColumnDef.build("id", "", "int", 0, false, empty), 
			ColumnDef.build("int_field", "", "int", 1, false, empty),
			ColumnDef.build("varchar_field", "", "varchar", 2, false, empty),
			ColumnDef.build("float_field", "", "float", 3, false, empty),	
			ColumnDef.build("double_field", "", "double", 4, false, empty),											
			ColumnDef.build("bigint_field", "", "bigint", 5, false, empty),
			ColumnDef.build("datetime_field", "", "datetime", 6, false, empty)
		};
		
		table.setColumnList(Arrays.asList(colList));
	}
	
	protected void createData(RowMap r) {
		r.putData("id", 1);
		r.putData("int_field", 10);
		r.putData("varchar_field", "var char test");		
		r.putData("float_field", 1.11);
		r.putData("double_field", 22.222);
		r.putData("bigint_field", 123456789);
		r.putData("datetime_field", "2016-06-06 15:06:00");
	}
	
	protected void createOldData(RowMap r) {
		r.putOldData("id", 2);
		r.putOldData("int_field", 20);
		r.putOldData("varchar_field", "test var char");		
		r.putOldData("float_field", 2.22);
		r.putOldData("double_field", 33.333);
		r.putOldData("bigint_field", 11019876);
		r.putOldData("datetime_field", "2015-05-05 14:01:00");
	}
}
