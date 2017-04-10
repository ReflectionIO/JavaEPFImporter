//
//  ObjectifyClassCreatorIngestor.java
//  epfimporter
//
//  Created by William Shakour (billy1380) on 11 Feb 2016.
//  Copyright © 2016 WillShex Limited. All rights reserved.
//
package com.spacehopperstudios.epf.ingest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.spacehopperstudios.epf.SubstringNotFoundException;
import com.spacehopperstudios.epf.parse.V3Parser;
import com.willshex.codegen.core.Code;
import com.willshex.codegen.core.EnvironmentProvider;
import com.willshex.codegen.core.all.CatchAllKeywordProcessor;
import com.willshex.codegen.core.datatypes.MappableType;
import com.willshex.codegen.core.datatypes.MappableTypeType;
import com.willshex.codegen.core.datatypes.Member;
import com.willshex.codegen.core.helpers.CommentHelper;
import com.willshex.codegen.core.helpers.TypeCreatorHelper;
import com.willshex.codegen.core.java.JavaTypeCreator;
import com.willshex.codegen.core.java.gson.FromJsonGenerator;
import com.willshex.codegen.core.java.gson.ToJsonGenerator;
import com.willshex.utility.StringUtils;

/**
 * @author William Shakour (billy1380)
 *
 */
public class ObjectifyClassCreatorIngestor extends JavaTypeCreator
		implements Ingester {

	private static final Logger LOGGER = Logger
			.getLogger(ObjectifyClassCreatorIngestor.class);

	List<MappableType> supportingTypes = new ArrayList<>();

	/* (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#init(java.lang.String,
	 * com.spacehopperstudios.epf.parse.V3Parser, java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 * java.lang.String, java.lang.String) */
	@Override
	public void init (String filePath, V3Parser parser, String tablePrefix,
			String dbHost, String dbUser, String dbPassword, String dbName,
			String recordDelim, String fieldDelim)
			throws IOException, SubstringNotFoundException {
		CatchAllKeywordProcessor.register();

		String fileName = (new File(filePath)).getName();
		String pref = tablePrefix == null || tablePrefix.length() == 0 ? ""
				: String.format("%s_", tablePrefix);
		String tableName = (pref + fileName).replace("-", "_"); // hyphens aren't allowed in table names

		if (tableName.contains(".")) {
			tableName = tableName.split(".", -1)[0];
		}

		String typeName = StringUtils.pascalCase(tableName.replace(pref, ""));
		String codeFileName = typeName + ".java";

		Code code = new Code();
		code.append(CommentHelper.getFileComment(codeFileName));
		code.appendLine("package epf.datatype;");

		code.appendLine("import com.google.gson.*;");
		code.appendLine("import com.googlecode.objectify.annotation.*;");
		code.appendLine("import com.willshex.gson.shared.Jsonable;");
		code.appendLine("import com.googlecode.objectify.*;");

		code.appendLine("@Cache");
		code.append("@Entity(name=\"");
		code.append(tableName);
		code.append("\")");
		code.appendNewLine();
		code.append("public class ");
		code.append(typeName);
		code.appendLine(" extends Jsonable {");

		Map<String, String> dataTypeLookup = new HashMap<String, String>();
		List<String> dataTypes = parser.getDataTypes();
		List<String> columnNames = parser.getColumnNames();
		List<String> primaryKey = parser.getPrimaryKey();

		for (int i = 0; i < columnNames.size(); i++) {
			dataTypeLookup.put(columnNames.get(i), dataTypes.get(i));
		}

		code.appendLine("@Id");
		switch (primaryKey.size()) {
		case 0:
			code.appendLine("public Long id;");
		case 1:
			if (dataTypeLookup.get(primaryKey.get(0)).equals("INTEGER")) {
				code.appendLine("public Long id;");
			} else {
				code.appendLine("public String name;");
			}
			break;
		default:
			code.appendLine("public String name;");
			break;
		}

		MappableType type = new MappableType();
		type.name(typeName.toString());
		type.namespace("com.willshex.epf.datatype");
		type.type(MappableTypeType.MappableTypeTypeClass);

		Member member;
		String columnName, dataType, codeColumnName;
		boolean index, reference;
		for (int i = 0; i < columnNames.size(); i++) {
			index = false;
			columnName = columnNames.get(i);
			dataType = dataTypeLookup.get(columnNames.get(i));

			if (primaryKey.size() == 1 && primaryKey.contains(columnName)) {
				continue;
			}

			member = new Member();
			index = false;
			reference = false;

			if (primaryKey.contains(columnName)) {
				index = true;
			}

			if (columnName.endsWith("_id") && dataType.equals("INTEGER")) {
				index = reference = true;
			}

			codeColumnName = StringUtils.camelCase(columnName);
			if (reference) {
				codeColumnName = codeColumnName.replace("Id", "");
			}
			member.name(codeColumnName);
			member.type(convertType(columnName, dataType, tableName, pref));

			code.append("@AlsoLoad(\"");
			code.append(columnName);
			code.appendLine("\")");

			if (index) {
				code.appendLine("@Index");
			}

			code.append("public ");
			if (reference) {
				code.append("Key<");
				code.append(getJavaTypeForType(member.type));
				code.append(">");
			} else {
				code.append(getJavaTypeForType(member.type));
			}
			code.append(" ");
			if (reference) {
				code.append(member.name);
				code.append("Key");
			} else {
				code.append(member.name);
			}
			code.appendLine(";");

			if (reference) {
				code.appendLine("@Ignore");
				code.append("public ");
				code.append(getJavaTypeForType(member.type));
				code.append(" ");
				code.append(member.name);
				code.appendLine(";");
			}

			member.collection(false);
			member.enumeration(false);
			member.typeNamespace("");

			TypeCreatorHelper.addVariable(type, member);
		}

		supportingTypes.add(type);

		code.appendNewLine();

		code.appendLine(
				ToJsonGenerator.generateJson(type.variables, supportingTypes));

		code.appendLine(FromJsonGenerator.generateJson(type.variables,
				supportingTypes));

		code.append("}");

		File generateFolder = new File("generate/src/epf/datatype");

		if (generateFolder.exists()) {
			generateFolder.delete();
		}

		generateFolder.mkdirs();

		write(code.toString(),
				generateFolder.getPath()
						+ EnvironmentProvider.provide().getFileSeparator()
						+ codeFileName);
	}

	/**
	 * @param code
	 * @param fileName
	 */
	private void write (String code, String fileName) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(fileName));
			writer.write(code);
		} catch (Exception e) {
			LOGGER.error("Error writing to file " + fileName, e);
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					LOGGER.error("Error closing file " + fileName, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param columnName
	 * @param dataType
	 * @param tableName
	 * @param tablePrefix
	 * @return
	 */
	private String convertType (String columnName, String dataType,
			String tableName, String tablePrefix) {
		String type;

		if (columnName.endsWith("_date")) {
			if (dataType.equals("BIGINT")) {
				type = "date";
			} else {
				type = convertType("", dataType, tableName, tablePrefix);
			}
		} else if (columnName.endsWith("_id")) {
			if (dataType.equals("INTEGER")) {
				type = StringUtils.pascalCase(
						foreignTableName(columnName, tableName, tablePrefix)
								.replace(tablePrefix, ""));
			} else {
				type = convertType("", dataType, tableName, tablePrefix);
			}
		} else {
			if (dataType.equals("INTEGER")) {
				type = "int";
			} else if (dataType.contains("CHAR") || dataType.contains("TEXT")) {
				type = "string";
			} else if (dataType.equals("BIGINT")
					|| dataType.startsWith("DECIMAL")) {
				type = "long";
			} else if (dataType.equals("BOOLEAN")) {
				type = "bool";
			} else if (dataType.equals("DATETIME")) {
				type = "Date";
			} else {
				type = dataType;
			}
		}

		return type;
	}

	/**
	 * @return
	 */
	private String foreignTableName (String columnName, String tableName,
			String tablePrefix) {
		String foreignTableName;
		switch (columnName) {
		case "parent_id":
			// AFAIK this only applies to genres
			foreignTableName = tableName;
			break;
		case "primary_media_type_id":
			foreignTableName = tablePrefix + "media_type";
			break;
		default:
			foreignTableName = tablePrefix + columnName.replace("_id", "");
			break;
		}

		return foreignTableName;
	}

	/* (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingest(boolean) */
	@Override
	public void ingest (boolean skipViolators) {
		// do nothing
	}

	/* (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestFull(boolean) */
	@Override
	public void ingestFull (boolean skipKeyViolators) {
		// do nothing
	}

	/* (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestFullResume(long,
	 * boolean) */
	@Override
	public void ingestFullResume (long fromRecord, boolean skipKeyViolators) {
		// do nothing
	}

	/* (non-Javadoc)
	 * 
	 * @see com.spacehopperstudios.epf.ingest.Ingester#ingestIncremental(long,
	 * boolean) */
	@Override
	public void ingestIncremental (long fromRecord, boolean skipKeyViolators) {
		// do nothing
	}

}
