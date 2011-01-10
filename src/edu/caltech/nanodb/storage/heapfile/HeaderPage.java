package edu.caltech.nanodb.storage.heapfile;


import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.TableStats;


/**
 * This class contains some constants for where different values live in
 * the header page of a table file.  <b>Note that the data file's page
 * size is always the first two bytes of the first page in the data
 * file!</b>  Thus, we don't get to tinker with the
 *
 * @todo INCORRECT COMMENT!
 */
public class HeaderPage {

    /**
     * The offset in the header page where the size of the database schema is
     * stored.
     */
    public static final int OFFSET_SCHEMA_SIZE = 2;


    /** The offset in the header page where the number of columns is stored. */
    public static final int OFFSET_NCOLS = 4;

    /**
     * The offset in the header page where the column descriptions start.
     * There could be as many as 255 column descriptions, so this is only the
     * beginning of that data!
     */
    public static final int OFFSET_COL_DESCS = 5;


    /**
     * This is the <em>relative</em> offset of the number of data-pages in the
     * table file, relative to the start of the table statistics.  This value is
     * an unsigned short, since we constrain all data files to have at most 64K
     * total pages.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_NUM_DATA_PAGES = 0;


    /**
     * This is the <em>relative</em> offset of the number of tuples in the data
     * file, relative to the start of the table statistics.  This value is an
     * unsigned integer (4 bytes), since it is feasible (although highly
     * unlikely) that each tuple could be a single byte.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_NUM_TUPLES = 2;


    /**
     * This is the <em>relative</em> offset of the average tuple-size in the
     * data file, relative to the start of the table statistics.  This value is
     * a float (4 bytes).
     * <p>
     * Note that this value is not just the number of pages multiplied by the
     * page size, then divided by the number of tuples.  Data pages may have a
     * lot of empty space in them, and this value does not reflect that empty
     * space; it reflects the actual number of bytes that comprise a tuple, on
     * average, for the data file.
     *
     * @see #getStatsOffset
     */
    public static final int RELOFF_AVG_TUPLE_SIZE = 6;


    //public static final int ENDOFF_NONFULL_LIST = -4;

    //public static final int ENDOFF_FULL_LIST = -2;


    public static int getSchemaSize(DBPage dbPage) {
        return dbPage.readUnsignedShort(OFFSET_SCHEMA_SIZE);
    }


    public static void setSchemaSize(DBPage dbPage, int numBytes) {
        if (numBytes < 0) {
            throw new IllegalArgumentException(
                "numButes must be >= 0; got " + numBytes);
        }

        dbPage.writeShort(OFFSET_SCHEMA_SIZE, numBytes);
    }


    public static int getStatsOffset(DBPage dbPage) {
        return OFFSET_NCOLS + getSchemaSize(dbPage);
    }


    public static void setStatNumDataPages(DBPage dbPage, int numPages) {
        int offset = getStatsOffset(dbPage) + RELOFF_NUM_DATA_PAGES;
        dbPage.writeShort(offset, numPages);
    }


    public static int getStatNumDataPages(DBPage dbPage) {
        int offset = getStatsOffset(dbPage) + RELOFF_NUM_DATA_PAGES;
        return dbPage.readUnsignedShort(offset);
    }


    public static void setStatNumTuples(DBPage dbPage, long numTuples) {
        int offset = getStatsOffset(dbPage) + RELOFF_NUM_TUPLES;
        // Casting long to int here is fine, since we are writing an
        // unsigned int.
        dbPage.writeInt(offset, (int) numTuples);
    }


    public static long getStatNumTuples(DBPage dbPage) {
        int offset = getStatsOffset(dbPage) + RELOFF_NUM_TUPLES;
        return dbPage.readUnsignedInt(offset);
    }


    public static void setStatAvgTupleSize(DBPage dbPage, float avgTupleSize) {
        int offset = getStatsOffset(dbPage) + RELOFF_AVG_TUPLE_SIZE;
        dbPage.writeFloat(offset, avgTupleSize);
    }


    public static float getStatAvgTupleSize(DBPage dbPage) {
        int offset = getStatsOffset(dbPage) + RELOFF_AVG_TUPLE_SIZE;
        return dbPage.readFloat(offset);
    }


    public static TableStats getTableStats(DBPage dbPage) {
        return new TableStats(getStatNumDataPages(dbPage),
            getStatNumTuples(dbPage), getStatAvgTupleSize(dbPage));
    }


    public static void setTableStats(DBPage dbPage, TableStats stats) {
        setStatNumDataPages(dbPage, stats.numDataPages);
        setStatNumTuples(dbPage, stats.numTuples);
        setStatAvgTupleSize(dbPage, stats.avgTupleSize);
    }
}
