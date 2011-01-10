package edu.caltech.test.nanodb.storage;


import org.testng.annotations.Test;

import edu.caltech.nanodb.storage.DBFile;


/**
 * This class exercises some of the core utility methods of the {@link DBFile}
 * class.
 */
@Test
public class TestDBFile {

    public void testIsValidPageSize() {
        // This is too small.
        assert !DBFile.isValidPageSize(256);
        assert !DBFile.isValidPageSize(511);

        assert DBFile.isValidPageSize(512);
        assert DBFile.isValidPageSize(1024);
        assert DBFile.isValidPageSize(2048);
        assert DBFile.isValidPageSize(4096);
        assert DBFile.isValidPageSize(8192);
        assert DBFile.isValidPageSize(65536);

        assert !DBFile.isValidPageSize(515);
        assert !DBFile.isValidPageSize(1063);
        assert !DBFile.isValidPageSize(3072);
        assert !DBFile.isValidPageSize(4095);
        assert !DBFile.isValidPageSize(10000);
        assert !DBFile.isValidPageSize(65535);

        // This is too large.
        assert !DBFile.isValidPageSize(65537);
        assert !DBFile.isValidPageSize(131072);
    }


    public void testCheckValidPageSize() {
        DBFile.checkValidPageSize(65536);
        DBFile.checkValidPageSize(512);

        try {
            DBFile.checkValidPageSize(8000);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // Success!
        }

        try {
            DBFile.checkValidPageSize(10000);
            assert false;
        }
        catch (IllegalArgumentException e) {
            // Success!
        }
    }


    private void tryEncoding(int value, int expected) {
        int actual = DBFile.encodePageSize(value);

        assert actual == expected :
            "Error encoding " + value + " - expected " + expected + "; got " +
            actual;
    }


    public void testEncodePageSize() {
        tryEncoding(  512,  9);
        tryEncoding( 1024, 10);
        tryEncoding( 2048, 11);
        tryEncoding( 4096, 12);
        tryEncoding( 8192, 13);
        tryEncoding(16384, 14);
        tryEncoding(32768, 15);
        tryEncoding(65536, 16);
    }


    public void testDecodePageSize() {
        assert DBFile.decodePageSize( 9) ==   512;
        assert DBFile.decodePageSize(10) ==  1024;
        assert DBFile.decodePageSize(11) ==  2048;
        assert DBFile.decodePageSize(12) ==  4096;
        assert DBFile.decodePageSize(13) ==  8192;
        assert DBFile.decodePageSize(14) == 16384;
        assert DBFile.decodePageSize(15) == 32768;
        assert DBFile.decodePageSize(16) == 65536;
    }
    

}
