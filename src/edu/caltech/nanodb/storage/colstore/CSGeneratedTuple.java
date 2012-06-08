package edu.caltech.nanodb.storage.colstore;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.TypeConverter;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.FilePointer;

public class CSGeneratedTuple implements Tuple {

	/**
     * The columns that appear within the tuple.  We don't use a {@link Schema}
     * object so that we can use this class in a wider range of contexts.
     */
    private List<ColumnInfo> colInfos;
    
    private ArrayList<Object> values;
    
    public CSGeneratedTuple(List<ColumnInfo> info) {
    	colInfos = info;
    	values = new ArrayList<Object>();
    }

	@Override
	public boolean isCacheable() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public int getColumnCount() {
		// TODO Auto-generated method stub
		return colInfos.size();
	}

	@Override
	public boolean isNullValue(int colIndex) {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
     * This helper method checks the column index for being in the proper
     * range of values.
     *
     * @param colIndex the column index to check
     *
     * @throws java.lang.IllegalArgumentException if the specified column
     *         index is out of range.
     */
    private void checkColumnIndex(int colIndex) {
        if (colIndex < 0 || colIndex >= colInfos.size()) {
            throw new IllegalArgumentException("Column index must be in range [0," +
                (colInfos.size() - 1) + "], got " + colIndex);
        }
    }

	@Override
	public Object getColumnValue(int colIndex) {
		checkColumnIndex(colIndex);

        Object value = null;
        if (!isNullValue(colIndex)) {

            ColumnType colType = colInfos.get(colIndex).getType();
            switch (colType.getBaseType()) {

            case INTEGER:
                value = TypeConverter.getIntegerValue(values.get(colIndex));
                break;

            case SMALLINT:
                value = TypeConverter.getShortValue(values.get(colIndex));
                break;

            case BIGINT:
                value = TypeConverter.getLongValue(values.get(colIndex));
                break;

            case TINYINT:
                value = TypeConverter.getByteValue(values.get(colIndex));
                break;

            case FLOAT:
                value = TypeConverter.getFloatValue(values.get(colIndex));
                break;

            case DOUBLE:
                value = TypeConverter.getDoubleValue(values.get(colIndex));
                break;

            case CHAR:
            case VARCHAR:
            case FILE_POINTER:
                value = TypeConverter.getStringValue(values.get(colIndex));
                break;

            default:
                throw new UnsupportedOperationException(
                    "Cannot currently store type " + colType.getBaseType());
            }
        }

        return value;
    }

	@Override
	public void setColumnValue(int colIndex, Object value) {
		checkColumnIndex(colIndex);

        if (value == null) {
            throw new IllegalArgumentException("This should never be null!");
        }
        else {
            // Update the value stored in the tuple.
            values.add(value);
        }
	}
    
    
}
