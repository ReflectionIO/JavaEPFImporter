//  
//  IngesterBase.java
//  epfimporter
//
//  Created by William Shakour on 28 Aug 2013.
//  Copyrights © 2013 SPACEHOPPER STUDIOS LTD. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import java.io.File;
import java.util.Date;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.spacehopperstudios.epf.parse.V3Parser;

/**
 * @author billy1380
 * 
 */
public abstract class IngesterBase implements Ingester {

	protected String tableName;

	protected JsonObject statusDict;
	protected String filePath;
	protected String fileName;

	protected Date startTime;
	protected Date endTime;

	protected Date abortTime;
	protected boolean didAbort;

	protected long lastRecordIngested;
	protected V3Parser parser;

	protected long lastRecordCheck = 0;
	protected Date lastTimeCheck;

	public static final String STATUS_FILENAME = "fileName";
	public static final String STATUS_FILEPATH = "filePath";
	public static final String STATUS_LASTRECORD_INGESTED = "lastRecordIngested";
	public static final String STATUS_STARTTIME = "startTime";
	public static final String STATUS_ENDTIME = "endTime";
	public static final String STATUS_ABORTTIME = "abortTime";
	public static final String STATUS_DIDABORT = "didAbort";

	protected void updateStatusDict () {
		this.statusDict.add(STATUS_FILENAME, new JsonPrimitive(this.fileName));
		this.statusDict.add(STATUS_FILEPATH, new JsonPrimitive(this.filePath));
		this.statusDict.add(STATUS_LASTRECORD_INGESTED,
				new JsonPrimitive(this.lastRecordIngested));

		if (this.startTime != null) {
			this.statusDict.add(STATUS_STARTTIME,
					new JsonPrimitive(this.startTime.getTime()));
		}

		if (endTime != null) {
			this.statusDict.add(STATUS_ENDTIME,
					new JsonPrimitive(this.endTime.getTime()));
		}

		if (abortTime != null) {
			this.statusDict.add(STATUS_ABORTTIME,
					new JsonPrimitive(this.abortTime.getTime()));
		}

		this.statusDict.add(STATUS_DIDABORT, new JsonPrimitive(this.didAbort));
	}

	@Override
	public void ingest (boolean skipKeyViolators /* =False */) {

		if ("INCREMENTAL".equals(this.parser.getExportMode())) {
			this.ingestIncremental(0, skipKeyViolators);
		} else {
			if (lastRecordIngested > 0 && didAbort) {
				this.ingestFullResume(lastRecordIngested, skipKeyViolators);
			} else {
				this.ingestFull(skipKeyViolators);
			}
		}
	}

	/**
	 * Checks whether recordGap or more records have been ingested since the last check; if so, checks whether timeGap seconds have elapsed since the last
	 * check.
	 * 
	 * If both checks pass, returns this.lastRecordIngested; otherwise returns null.
	 */
	protected long checkProgress (int recordGap/* =5000 */,
			long timeGap/* =datetime.timedelta(0, 120, 0) */) {

		if (this.lastRecordIngested - this.lastRecordCheck >= recordGap) {
			Date t = new Date();
			if (t.getTime() - this.lastTimeCheck.getTime() >= timeGap) {
				this.lastTimeCheck = t;
				this.lastRecordCheck = this.lastRecordIngested;

				return this.lastRecordCheck;
			}
		}

		return 0;
	}

	protected void initTableName (String filePath, String tablePrefix) {

		this.filePath = filePath;
		this.fileName = (new File(filePath)).getName();
		String pref = tablePrefix == null || tablePrefix.length() == 0 ? ""
				: String.format("%s_", tablePrefix);
		this.tableName = (pref + this.fileName).replace("-", "_"); // hyphens aren't allowed in table names

		if (this.tableName.contains(".")) {
			this.tableName = this.tableName.split(".", -1)[0];
		}
	}

	protected void initVariables (V3Parser parser, JsonObject statusDict) {
		if (statusDict.has(STATUS_LASTRECORD_INGESTED)) {
			this.lastRecordIngested = statusDict.get(STATUS_LASTRECORD_INGESTED)
					.getAsLong();
		} else {
			this.lastRecordIngested = -1;
		}

		this.parser = (V3Parser) parser;

		if (statusDict.has(STATUS_STARTTIME)) {
			this.startTime = new Date(
					statusDict.get(STATUS_STARTTIME).getAsLong());
		} else {
			this.startTime = null;
		}

		if (statusDict.has(STATUS_ENDTIME)) {
			this.endTime = new Date(statusDict.get(STATUS_ENDTIME).getAsLong());
		} else {
			this.endTime = null;
		}

		if (statusDict.has(STATUS_ABORTTIME)) {
			this.abortTime = new Date(
					statusDict.get(STATUS_ABORTTIME).getAsLong());
		} else {
			this.abortTime = null;
		}

		if (statusDict.has(STATUS_DIDABORT)) {
			this.didAbort = statusDict.get(STATUS_DIDABORT).getAsBoolean();
		} else {
			this.didAbort = false;
		}

		this.lastRecordCheck = 0;
		this.lastTimeCheck = new Date();

		this.statusDict = statusDict;
	}
}
