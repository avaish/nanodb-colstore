package edu.caltech.nanodb.storage;


import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The buffer manager reduces the number of disk IO operations by managing an
 * in-memory cache of data pages.
 *
 *
 */
public class BufferManager {

    /**
     * The system property that can be used to specify the size of the page
     * cache in the buffer manager.
     */
    public static final String PROP_PAGECACHE_SIZE = "nanodb.pagecache.size";

    /** The default page-cache size is defined to be 1MB. */
    public static final long DEFAULT_PAGECACHE_SIZE = 1024 * 1024;


    /**
     * The system property that can be used to specify the page replacement
     * policy in the buffer manager.
     */
    public static final String PROP_PAGECACHE_POLICY = "nanodb.pagecache.policy";


    private static class CachedPageInfo {
        public DBFile dbFile;

        public int pageNo;

        public CachedPageInfo(DBFile dbFile, int pageNo) {
            if (dbFile == null)
                throw new IllegalArgumentException("dbFile cannot be null");

            this.dbFile = dbFile;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CachedPageInfo) {
                CachedPageInfo other = (CachedPageInfo) obj;
                return dbFile.equals(other.dbFile) && pageNo == other.pageNo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 31 * hash + dbFile.hashCode();
            hash = 31 * hash + pageNo;
            return hash;
        }
    }


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BufferManager.class);

    
    private FileManager fileManager;
    

    private LinkedHashMap<CachedPageInfo, DBPage> cachedPages;


    private long totalBytesCached;

    private long maxCacheSize;


    public BufferManager(FileManager fileManager) {
        this.fileManager = fileManager;

        configureMaxCacheSize();

        String replacementPolicy = configureReplacementPolicy();
        cachedPages = new LinkedHashMap<CachedPageInfo, DBPage>(16, 0.75f,
            "lru".equals(replacementPolicy));

        totalBytesCached = 0;
    }


    private void configureMaxCacheSize() {
        // Set the default up-front; it's just easier that way.
        maxCacheSize = DEFAULT_PAGECACHE_SIZE;

        String str = System.getProperty(PROP_PAGECACHE_SIZE);
        if (str != null) {
            str = str.trim().toLowerCase();

            long scale = 1;
            if (str.length() > 1) {
                char modifierChar = str.charAt(str.length() - 1);
                boolean removeModifier = true;
                if (modifierChar == 'k')
                    scale = 1024;
                else if (modifierChar == 'm')
                    scale = 1024 * 1024;
                else if (modifierChar == 'g')
                    scale = 1024 * 1024 * 1024;
                else
                    removeModifier = false;

                if (removeModifier)
                    str = str.substring(0, str.length() - 1);
            }

            try {
                maxCacheSize = Long.parseLong(str);
                maxCacheSize *= scale;
            }
            catch (NumberFormatException e) {
                logger.error(String.format(
                    "Could not parse page-cache size value \"%s\"; " +
                    "using default value of %d bytes",
                    System.getProperty(PROP_PAGECACHE_SIZE),
                    DEFAULT_PAGECACHE_SIZE));

                maxCacheSize = DEFAULT_PAGECACHE_SIZE;
            }
        }
    }


    private String configureReplacementPolicy() {
        String str = System.getProperty(PROP_PAGECACHE_POLICY);
        if (str != null) {
            str = str.trim().toLowerCase();

            if (!("lru".equals(str) || "fifo".equals(str))) {
                logger.error(String.format(
                    "Unrecognized value \"%s\" for page-cache replacement " +
                    "policy; using default value of LRU.",
                    System.getProperty(PROP_PAGECACHE_POLICY)));
            }
        }

        return str;
    }


    public DBPage getPage(DBFile dbFile, int pageNo) {
        DBPage dbPage = cachedPages.get(new CachedPageInfo(dbFile, pageNo));

        logger.debug(String.format(
            "Requested page [%s,%d] is%s in page-cache.",
            dbFile, pageNo, (dbPage != null ? "" : " NOT")));

        return dbPage;
    }


    public void addPage(DBPage dbPage) throws IOException {
        if (dbPage == null)
            throw new IllegalArgumentException("dbPage cannot be null");

        DBFile dbFile = dbPage.getDBFile();
        int pageNo = dbPage.getPageNo();

        logger.debug(String.format( "Adding page [%s,%d] to page-cache.",
            dbFile, pageNo));

        int pageSize = dbPage.getPageSize();

        if (pageSize + totalBytesCached > maxCacheSize && !cachedPages.isEmpty()) {
            // The cache will be too large after adding this page.  Try to solve
            // this problem by evicting pages.

            Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
                cachedPages.entrySet().iterator();

            while (entries.hasNext() &&
                   pageSize + totalBytesCached > maxCacheSize) {
                Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

                DBPage oldPage = entry.getValue();

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache to make room.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; saving to disk.");
                    fileManager.saveDBPage(oldPage);
                }

                entries.remove();
                totalBytesCached -= oldPage.getPageSize();
            }
        }

        cachedPages.put(new CachedPageInfo(dbFile, pageNo), dbPage);
    }


    public void flushDBFile(DBFile dbFile) throws IOException {
        logger.info("Flushing all pages for file " + dbFile +
            " from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            CachedPageInfo info = entry.getKey();
            if (dbFile.equals(info.dbFile)) {
                DBPage oldPage = entry.getValue();

                logger.debug(String.format(
                    "    Evicting page [%s,%d] from page-cache.",
                    oldPage.getDBFile(), oldPage.getPageNo()));

                if (oldPage.isDirty()) {
                    logger.debug("    Evicted page is dirty; saving to disk.");
                    fileManager.saveDBPage(oldPage);
                }

                // Remove the page from the cache.
                entries.remove();
                totalBytesCached -= oldPage.getPageSize();
            }
        }
    }


    public void flushAll() throws IOException {
        logger.info("Flushing ALL database pages from the Buffer Manager.");

        Iterator<Map.Entry<CachedPageInfo, DBPage>> entries =
            cachedPages.entrySet().iterator();

        while (entries.hasNext()) {
            Map.Entry<CachedPageInfo, DBPage> entry = entries.next();

            DBPage oldPage = entry.getValue();

            logger.debug(String.format(
                "    Evicting page [%s,%d] from page-cache.",
                oldPage.getDBFile(), oldPage.getPageNo()));

            if (oldPage.isDirty()) {
                logger.debug("    Evicted page is dirty; saving to disk.");
                fileManager.saveDBPage(oldPage);
            }

            // Remove the page from the cache.
            entries.remove();
            totalBytesCached -= oldPage.getPageSize();
        }
    }
}
