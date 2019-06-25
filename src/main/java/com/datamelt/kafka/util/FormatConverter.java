/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datamelt.kafka.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;
import org.json.JSONObject; 

import com.datamelt.util.RowField;
import com.datamelt.util.RowFieldCollection;

/**
 * Formats RowFieldCollection to JSON or Avro and vice versa.
 * 
 * 
 * @author uwe.geercken@web.de - 2019-06-25
 *
 */
public class FormatConverter
{
	final static Logger logger = Logger.getLogger(FormatConverter.class);
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record	a record of data 
	 * @return			a rowfield collection
	 */
	public static RowFieldCollection convertFromJson(String record)
	{
		// create a JSON object from the record
		JSONObject obj = new JSONObject(record);
		
        // create a new rowfield collection using keys and values
		return new RowFieldCollection(obj.toMap());  

	}
	
	/**
	 * creates a JSONObject from the fields available in the rowfield collection.
	 * 
	 * if fields to be excluded from the output are defined, then these will be
	 * excluded here.
	 * 
	 * @param rowFieldCollection	rowfield collection
	 * @param excludedFields		rowfield to exclude from the output
	 * @return						JSONObject representing json formated key/value pairs
	 */
	public static JSONObject convertToJson(RowFieldCollection rowFieldCollection, ArrayList<String> excludedFields)
	{
		JSONObject jsonObject = new JSONObject();
		for(int i=0;i<rowFieldCollection.getNumberOfFields();i++)
		{
			try
			{
				RowField field = rowFieldCollection.getField(i);
				// don't add fields that should be excluded
				if(excludedFields==null || !excludedFields.contains(field.getName()))
				{
					if(field.getValue()==null)
					{
						jsonObject.put(field.getName(), JSONObject.NULL);
					}
					else
					{
						jsonObject.put(field.getName(), field.getValue());
					}
				}
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception when creating json object from rowfield collection");
			}
		}
		return jsonObject;
	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record			the kafka message value in CSV format 
	 * @param recordFieldNames	the CSV fieldnames as specified in the properties file
	 * @param separator			the separator used between the individual fields in the kafka message
	 * @return					a rowfield collection
	 */
	public static RowFieldCollection convertFromCsv(String record, String recordFieldNames, String separator)
	{
		String fieldNames[] = recordFieldNames.split(Constants.PROPERTY_VALUES_SEPARATOR);
		String fields[] = record.split(separator);
		
        // create a new rowfield collection using keys and values
		return new RowFieldCollection(fieldNames,fields);  

	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param schema 			the AVRO schema corresponding to the message
	 * @param recordValue		the avro generic record 
	 * @return					a rowfield collection
	 */
	public static RowFieldCollection convertFromAvro(Schema schema,GenericRecord recordValue)
	{
		GenericRecord record = recordValue;
		RowFieldCollection rowFieldCollection = null;
		try
		{
			rowFieldCollection = new RowFieldCollection(record, schema);
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception when converting avro generic record to rowfield collection");
        	return null;
		}
        
		return rowFieldCollection;  

	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param record			the kafka message value as GenericRecord 
	 * @return					a rowfield collection or null in case of an exception
	 */
	public static RowFieldCollection convertFromAvroGenericRecord(GenericRecord record)
	{
		RowFieldCollection rowFieldCollection = null;
		try
		{
			rowFieldCollection = new RowFieldCollection(record, record.getSchema());
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception when converting GenericRecord to rowfield collection");
        	return null;
		}
        
		return rowFieldCollection;  

	}
	
	/**
	 * returns a collection of fields containing field names and their values from the record
	 * 
	 * @param recordValue		the kafka message value as byte array (in AVRO format) 
	 * @return					a rowfield collection or null in case of an exception
	 */
	public static RowFieldCollection convertFromAvroByteArray(byte[] recordValue)
	{
		GenericRecord record = byteArrayToDatum(recordValue);
		RowFieldCollection rowFieldCollection = null;
		try
		{
			rowFieldCollection = new RowFieldCollection(record, record.getSchema());
		}
		catch(Exception ex)
		{
			logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception when converting byte array to rowfield collection");
        	return null;
		}
        
		return rowFieldCollection;  

	}
	
	/**
	 * converts the byte array into an avro generic record
	 * 
	 * @param schema	the avro schema 
	 * @param byteData	the byte array
	 * @return			Avro GenericRecord or null in case of an IO exception
	 */
	private static GenericRecord byteArrayToDatum(byte[] byteData) 
	{
		GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        ByteArrayInputStream byteArrayInputStream = null;
        try 
        {
            byteArrayInputStream = new ByteArrayInputStream(byteData);
            Decoder decoder = DecoderFactory.get().binaryDecoder(byteArrayInputStream, null);
            return reader.read(null, decoder);
        }
        catch (IOException e) 
        {
        	logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "IO exception when converting byte array to avro generic record");
        	return null;
        } 
        finally
        {
            try 
            {
                byteArrayInputStream.close();
            }
            catch (IOException e) 
            {
            	logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "error closing ByteArrayInputStream when converting byte array to avro generic record");
            }
        }
    }
	
	/**
	 * converts a generic avro record into a byte array
	 * 
	 * @param schema		the avro schema
	 * @param record		an avro generic record
	 * @return				the byte array or null in case of an IO exception
	 */
	private static byte[] recordToByteArray(Schema schema, GenericRecord record) 
	{
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try 
        {
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
            writer.write(record, binaryEncoder);
            binaryEncoder.flush();
            return byteArrayOutputStream.toByteArray();
        }
        catch (IOException e) 
        {
        	logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "IO exception when converting avro generic record to byte array");
        	return null;
        } 
        finally 
        {
        	try
        	{
        		byteArrayOutputStream.close();
        	}
        	catch (IOException e) 
            {
        		logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception closing ByteArrayOutputStream when converting avro generic record to byte array");
            }
        }
	}

	/**
	 * converts a rowfield collection into an avro generic record
	 * 
	 * @param rowFieldCollection	collection of row fields
	 * @param excludedFields		fields to exclude from the process
	 * @param schema				the avro schema
	 * @return						the avro generic record
	 */
	public static GenericRecord convertToAvro(RowFieldCollection rowFieldCollection, ArrayList<String> excludedFields, Schema schema)
	{
		//GenericRecord record = new GenericData.Record(schema);
		GenericRecordBuilder recordBuilder =new GenericRecordBuilder(schema);
		for(int i=0;i<rowFieldCollection.getNumberOfFields();i++)
		{
			try
			{
				RowField field = rowFieldCollection.getField(i);
				// don't add fields that should be excluded
				if(excludedFields==null || !excludedFields.contains(field.getName()))
				{
					if(field.getValue()==null)
					{
						recordBuilder.set(field.getName(), null);
					}
					else
					{
						recordBuilder.set(field.getName(), field.getValue());
					}
				}
			}
			catch(Exception ex)
			{
				logger.error(Constants.LOG_LEVEL_SUBTYPE_GENERAL + "exception when creating avro generic record from rowfield collection");
			}
		}
		return recordBuilder.build();
	}
		
	/**
	 * converts a rowfield collection into an avro generic record in the form of a byte array
	 * 
	 * @param rowFieldCollection	collection of row fields
	 * @param excludedFields		fields to exclude from the process
	 * @param schema				the avro schema
	 * @return						the avro generic record as a byte array
	 */
	public static byte[] convertToAvroByteArray(RowFieldCollection rowFieldCollection, ArrayList<String> excludedFields, Schema schema)
	{
		GenericRecord record =  convertToAvro(rowFieldCollection,excludedFields, schema);
		return recordToByteArray(schema, record);
	}
}
