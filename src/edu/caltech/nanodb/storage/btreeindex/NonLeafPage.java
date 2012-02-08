package edu.caltech.nanodb.storage.btreeindex;


import java.util.List;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBPage;


/**
 */
public class NonLeafPage {
    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /**
     * The number of pointer entries in the page.  Note that this value is the
     * number of keys + 1.
     */
    public static final int OFFSET_NUM_POINTERS = 1;


    /**
     * The offset of the first page-pointer in the non-leaf page.  Each
     * page-pointer is an unsigned short, since there can only be 64K pages
     * in each data file.
     */
    public static final int OFFSET_FIRST_PTR = 3;


    /**
     * The offset of the first key in the non-leaf page.  Each page-pointer is
     * an unsigned short, since there can only be 64K pages in each data file.
     */
    public static final int OFFSET_FIRST_KEY = 5;


    /**
     * Returns the number of pointers stored in the non-leaf index page.  Note
     * that the number of pointers in non-leaf index pages is one more than the
     * number of keys.
     *
     * @param dbPage the page to get the pointer-count from
     *
     * @return the number of pointers stored in the non-leaf index page
     */
    public static int numPointers(DBPage dbPage) {
        return dbPage.readUnsignedShort(OFFSET_NUM_POINTERS);
    }


    /**
     * Returns the number of keys stored in the non-leaf index page.  Note
     * that the number of pointers in non-leaf index pages is one more than the
     * number of keys.
     *
     * @param dbPage the page to get the key-count from
     *
     * @return the number of keys stored in the non-leaf index page
     */
    public static int numKeys(DBPage dbPage) {
        return numPointers(dbPage) - 1;
    }


    public static int getPointerBeforeKey(BTreeIndexPageTuple tup) {
        // Each pointer is stored immediately before the corresponding key,
        // except for the last pointer which follows the last key in the page.
        // Thus, there are <em>N</em> keys, and <em>N</em> + 1 pointers, with
        // each key sandwiched by a pair of pointers.
        DBPage dbPage = tup.getDBPage();
        return dbPage.readUnsignedShort(tup.getOffset() - 2);
    }


    public static int getPointerAfterKey(BTreeIndexPageTuple tup) {
        // Each pointer is stored immediately before the corresponding key,
        // except for the last pointer which follows the last key in the page.
        // Thus, there are <em>N</em> keys, and <em>N</em> + 1 pointers, with
        // each key sandwiched by a pair of pointers.
        DBPage dbPage = tup.getDBPage();
        return dbPage.readUnsignedShort(tup.getEndOffset());
    }


    public static BTreeIndexPageTuple getFirstKey(DBPage dbPage,
                                                  List<ColumnInfo> colInfos) {
        return new BTreeIndexPageTuple(dbPage, OFFSET_FIRST_KEY, colInfos);
    }


    public static BTreeIndexPageTuple getNextKey(BTreeIndexPageTuple tup) {
        DBPage dbPage = tup.getDBPage();
        List<ColumnInfo> colInfos = tup.getColumnInfos();
        return new BTreeIndexPageTuple(dbPage, tup.getEndOffset() + 2, colInfos);
    }


    /**
     */
    @SuppressWarnings("unchecked")
    public static int compareToKey(Tuple keyA, Tuple keyB) {

        if (keyA.getColumnCount() != keyB.getColumnCount())
            throw new IllegalArgumentException("keys must be the same size");

        int compareResult = 0;

        int size = keyA.getColumnCount();
        for (int i = 0; i < size && compareResult == 0; i++) {
            Comparable valueA = (Comparable) keyA.getColumnValue(i);
            Comparable valueB = (Comparable) keyB.getColumnValue(i);

            // Although it should be "unknown" when we compare two NULL values
            // for equality, we say they are equal so that they will all appear
            // together in the sorting results.
            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if ( /* valueA != null && */ valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }
        }

        return compareResult;
    }
}
