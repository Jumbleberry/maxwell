package com.zendesk.maxwell;

import org.junit.Test;

import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class RowMapBufferTest {
	@Test
	public void TestOverflowToDisk() throws Exception {
		Table table = new Table();
		table.setDatabase("foo");		
		table.rename("bar");
		String[] empty = {};		
		ColumnDef[] colList = {
			ColumnDef.build("id", "", "int", 0, false, empty), 
		};		
		table.setColumnList(Arrays.asList(colList));
		
		RowMapBuffer buffer = new RowMapBuffer(2);

		RowMap r;
		buffer.add(new RowMap("insert", table, 1L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));
		buffer.add(new RowMap("insert", table, 2L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));
		buffer.add(new RowMap("insert", table, 3L, new ArrayList<String>(), new BinlogPosition(3, "mysql.1")));

		assertThat(buffer.size(), is(3L));
		assertThat(buffer.inMemorySize(), is(2L));

		assertThat(buffer.removeFirst().getTimestamp(), is(1L));
		assertThat(buffer.removeFirst().getTimestamp(), is(2L));
		assertThat(buffer.removeFirst().getTimestamp(), is(3L));
	}
}
