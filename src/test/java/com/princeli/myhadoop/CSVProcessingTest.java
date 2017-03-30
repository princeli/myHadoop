package com.princeli.myhadoop;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

import au.com.bytecode.opencsv.CSVParser;

public class CSVProcessingTest {

	private final String LINE = "ci,14897012,2,\"Monday, December 13, 2010 " + "14:10:32 UTC\",33.0290,-115."
			+ "5388,1.9,15.70,41,\"Southern California\"";

	@Test
	public void testReadingOneLine() throws Exception {
		String[] lines = new CSVParser().parseLine(LINE);

		assertEquals("should be Monday, December 13, 2010 14:10:32 UTC", "Monday, December 13, 2010 14:10:32 UTC",
				lines[3]);

		assertEquals("should be Southern California", "Southern California", lines[9]);

		assertEquals("should be 1.9", "1.9", lines[6]);
	}

	@Test
	public void testParsingDate() throws Exception {
		String datest = "Monday, December 13, 2010 14:10:32 UTC";
		SimpleDateFormat formatter = new SimpleDateFormat("EEEEE, MMMMM dd, yyyy HH:mm:ss Z",Locale.US);
		Date dt = formatter.parse(datest);

		formatter.applyPattern("dd-MM-yyyy");
		String dtstr = formatter.format(dt);
		assertEquals("should be 13-12-2010", "13-12-2010", dtstr);
	}

}
