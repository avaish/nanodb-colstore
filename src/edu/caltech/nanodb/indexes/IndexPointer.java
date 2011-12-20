package edu.caltech.nanodb.indexes;


import edu.caltech.nanodb.storage.FilePointer;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/20/11
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class IndexPointer extends FilePointer {
    public IndexPointer(int pageNo, int offset) {
        super(pageNo, offset);
    }
}
