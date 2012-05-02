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
		
		HashSet[] sets = new HashSet[columnCount];
		int[] sort = new int[columnCount];
		int[] runs = new int[columnCount];
		int[] counts = new int[columnCount];
		int[] distincts = new int[columnCount];
		String[] prev = new String[columnCount];
		boolean[] onruns = new boolean[columnCount];
		
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
				logger.debug("Count for column " + i + ": " + counts[i]);
				logger.debug("Uniques for column " + i + ": " + distincts[i]);
				logger.debug("Sort for column " + i + ": " + sort[i]);
				logger.debug("Run for column " + i + ": " + runs[i]);
			}
			row = fileReader.readLine();
		}
	}
}
