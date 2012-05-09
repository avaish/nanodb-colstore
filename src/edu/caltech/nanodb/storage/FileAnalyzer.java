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

public class FileAnalyzer {
	private static Logger logger = Logger.getLogger(FileAnalyzer.class);
	private String filename;
	private BufferedReader fileReader;
	private String[] encodings;
	
	public FileAnalyzer(String name) throws FileNotFoundException
	{
		filename = name;
		fileReader = new BufferedReader(
			new FileReader("input_datafiles/" + filename));
	}
	
	@SuppressWarnings("unchecked")
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
		int[] distincts = new int[columnCount];
		String[] prev = new String[columnCount];
		boolean[] onruns = new boolean[columnCount];
		encodings = new String[columnCount];
		
		names = row.split(",");
		
		row = fileReader.readLine();
		while (row != null)
		{
			logger.debug(row);
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
			
			if (sort[i] != 2)
			{
				if ((runs[i] / (float) counts[i]) > 0.75)
				{
					encodings[i] = "RLE";
				}
				else
				{
					encodings[i] = "Bit-string";
				}
			}
			else
			{
				if ((runs[i] / (float) counts[i]) > 0.75)
				{
					encodings[i] = "Dictionary";
				}
				else
				{
					encodings[i] = "Uncompressed";
				}
			}
			logger.debug(names[i] + ": Encoding: " + encodings[i]);
			
			logger.debug(" ");
		}
	}
}