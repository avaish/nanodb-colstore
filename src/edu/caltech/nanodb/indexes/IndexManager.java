package edu.caltech.nanodb.indexes;


import java.io.IOException;


/**
 * This interface specifies all operations that must be implemented to support
 * a particular kind of index file.
 */
public interface IndexManager {
    /**
     * This method initializes a newly created index file, using the details
     * specified in the passed-in <tt>IndexFileInfo</tt> object.
     *
     * @param idxFileInfo This object is an in/out parameter.  It is used to
     *        specify the name and details of the new index being created.  When
     *        the index is successfully created, the object is updated with the
     *        actual file that the index's data is stored in.
     *
     * @throws IOException if the file cannot be created, or if an error occurs
     *         while storing the initial index data.
     */
    void initIndexInfo(IndexFileInfo idxFileInfo) throws IOException;


    /**
     * This method loads the details for the specified index.
     *
     * @param idxFileInfo the index information object to populate.  When this
     *        is passed in, it only contains the index's name, the name of the
     *        table the index is specified on, and the opened database file to
     *        read the data from.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         index's details.
     */
    void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException;
}
