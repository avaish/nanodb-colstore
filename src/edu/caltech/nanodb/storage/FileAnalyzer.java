package edu.caltech.nanodb.storage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.CreateTableCommand;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.SQLDataType;

/** 
 * The FileAnalyzer class is a multifaceted class that handles both file input
 * and analysis to look for interesting properties in the data. *
 */
public class FileAnalyzer {
	private static Logger logger = Logger.getLogger(FileAnalyzer.class);
	private String filename;
	private BufferedReader fileReader;
	private FileEncoding[] encodings;
	private int[] distincts;
	private BufferedReader[] readers;
	private int seekBuffer;
	
	/** 
	 * Creates a FileAnalyzer object for a specific file.
	 * 
	 * @param name the filename
	 * @throws FileNotFoundException
	 */
	public FileAnalyzer(String name) throws FileNotFoundException
	{
		filename = name;
		fileReader = new BufferedReader(
			new FileReader("input_datafiles/" + filename));
		seekBuffer = 0;
	}
	
	/**
	 * Scans through the file, analyzing cardinality and locality (for now)!
	 * 
	 * @param colInfos the column infos of the file
	 * @throws IOException
	 */
	public void analyze(List<ColumnInfo> colInfos) throws IOException
	{
		int columnCount = 0;
		String row = fileReader.readLine();
		if (row != null)
		{
			String[] rowArray = row.split(",");
			columnCount = rowArray.length;
		}
		
		String[] names = new String[columnCount];
		HashSet[] sets = new HashSet[columnCount];
		int[] sort = new int[columnCount];
		int[] runs = new int[columnCount];
		int[] counts = new int[columnCount];
		distincts = new int[columnCount];
		String[] prev = new String[columnCount];
		boolean[] onruns = new boolean[columnCount];
		encodings = new FileEncoding[columnCount];
		readers = new BufferedReader[columnCount];
		
		names = row.split(",");
		
		row = fileReader.readLine();
		while (row != null)
		{
			if (row.length() > seekBuffer)
				seekBuffer = row.length() * 4;
			//logger.debug(row);
			String[] rowArray = row.split(",");
			for (int i = 0; i < columnCount; i++)
			{
				counts[i]++;
				
				if (sets[i] != null)
				{
					sets[i].add(rowArray[i]);
				}
				else
				{
					sets[i] = new HashSet<String>();
					sets[i].add(rowArray[i]);
				}
				
				if (prev[i] != null)
				{
					if (SQLDataType.isNumber(colInfos.get(i).getType().getBaseType()))
					{
						// Check if on a run
						if (Double.parseDouble(rowArray[i]) == Double.parseDouble(prev[i]))
						{
							if (onruns[i])
							{
								runs[i]++;
							}
							else
							{
								runs[i] += 2;
								onruns[i] = true;
							}
						}
						else
						{
							onruns[i] = false;
						}
						// Check if the data is still sorted
						if (Double.parseDouble(rowArray[i]) > Double.parseDouble(prev[i]))
						{
							if ((sort[i] != -1) && (sort[i] != 2))
							{
								sort[i] = 1;
							}
							else
							{
								sort[i] = 2;
							}
						}
						else if (Double.parseDouble(rowArray[i]) < Double.parseDouble(prev[i]))
						{
							if ((sort[i] != 1) && (sort[i] != 2))
							{
								sort[i] = -1;
							}
							else
							{
								sort[i] = 2;
							}
						}
					}
					else
					{
						// Check if on a run
						if (prev[i].equals(rowArray[i]))
						{
							if (onruns[i])
							{
								runs[i]++;
							}
							else
							{
								runs[i] += 2;
								onruns[i] = true;
							}
						}
						else
						{
							onruns[i] = false;
						}
						// Check if data is still sorted
						if (rowArray[i].compareTo(prev[i]) > 0)
						{
							if ((sort[i] != -1) && (sort[i] != 2))
							{
								sort[i] = 1;
							}
							else
							{
								sort[i] = 2;
							}
						}
						else if (rowArray[i].compareTo(prev[i]) < 0)
						{
							if ((sort[i] != 1) && (sort[i] != 2))
							{
								sort[i] = -1;
							}
							else
							{
								sort[i] = 2;
							}
						}
					}
				}
				prev[i] = rowArray[i];
				distincts[i] = sets[i].size();
			}
			row = fileReader.readLine();
		}
		
		// Log output
		for (int i = 0; i < columnCount; i++)
		{
			logger.debug(names[i] + ": Count: " + counts[i] + ", with " + 
				distincts[i] + " unique values and " + runs[i] + " values " + 
				"part of runs.");
			logger.debug(names[i] + ": Cardinality: " + 
				(distincts[i] / (float) counts[i]) + ". Locality: " + 
				(runs[i] / (float) counts[i]) + ".");
			if (sort[i] == -1)
				logger.debug(names[i] + ": Sorted decreasing");
			else if (sort[i] == 0)
				logger.debug(names[i] + ": One valued");
			else if (sort[i] == 1)
				logger.debug(names[i] + ": Sorted increasing");
			else
				logger.debug(names[i] + ": Not sorted");
			
			/* Determine file encoding. This is easy for our encodings, but with
			 * more types we'd need better ways of determining what's best. For
			 * now, the logic is simply - a lot of runs means go for RLE, and a
			 * few distincts leans it towards dictionary encode.
			 * 
			 * The uncompressed option is overweighted at the moment for purposes
			 * of demo and test.
			 */
			if (sort[i] != 2)
			{
				if ((runs[i] / (float) counts[i]) > 0.75)
				{
					encodings[i] = FileEncoding.RLE;
				}
				else
				{
					encodings[i] = FileEncoding.NONE;
				}
			}
			else
			{
				if ((distincts[i] / (float) counts[i]) < 0.75)
				{
					encodings[i] = FileEncoding.DICTIONARY;
				}
				else
				{
					encodings[i] = FileEncoding.NONE;
				}
			}
			logger.debug(names[i] + ": Encoding: " + encodings[i]);
			
			logger.debug(" ");
		}
	}
	
	/**
	 * Gets the next object from file from a column.
	 * @param column column to get object from
	 * @return object
	 * @throws IOException
	 */
	public String getNextObject(int column) throws IOException {
		if (readers[column] == null) {
			readers[column] = new BufferedReader(new FileReader
				("input_datafiles/" + filename));
			readers[column].readLine();
		}
		
		readers[column].mark(seekBuffer);
		// logger.debug("Marked with " + seekBuffer + " buffer length.");
		String line = readers[column].readLine();
		if (line != null) {
			// logger.debug("Reading " + line.split(",")[column]);
			return line.split(",")[column].trim();
		}
		else {
			return null;
		}
	}
	
	public void reset(int column) throws IOException {
		readers[column].reset();
	}

	/**
	 * Returns the recommended encoding for a column
	 * @param i column index
	 * @return file encoding
	 */
	public FileEncoding getEncoding(int i) {
		return encodings[i];
	}
	
	public int getCounts(int i) {
		return distincts[i];
	}
}
