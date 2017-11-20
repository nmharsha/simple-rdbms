#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <map>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

#define FREE_SPACE_OFFSET 2
#define PAGE_TYPE_OFFSET 4
#define NEXT_PAGE_OFFSET 8

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

public:
    static IndexManager* instance();
    PagedFileManager* pagedFileManager;
    int rootNodePageNum;
    map<string, PageNum> indexRootNodeMap;


    // Create an index file.
    RC createFile(const string &fileName);

    // Delete an index file.
    RC destroyFile(const string &fileName);

    // Open an index and return an ixfileHandle.
    RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

    // Close an ixfileHandle for an index.
    RC closeFile(IXFileHandle &ixfileHandle);

    // Insert an entry into the given index that is indicated by the given ixfileHandle.
    RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixfileHandle.
    RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixfileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;


    static IndexManager *_index_manager;

    RC insertIntoTree(IXFileHandle &ixfileHandle, PageNum currPageNum, const Attribute &attribute, const void *key, const RID &rid,
                      PageNum &newChildPageNum, void* splitKey);

    unsigned short getFreeSpaceFromPage(void *pageData) const;

    int getPageTypeFromPage(void *pageData);

    int findLeaf(IXFileHandle &ixfileHandle, void* pageData, PageNum currPageNum, const Attribute &attribute, const void* lowKey);

    int getIntValueAtOffset(void *pageRecord, int offset);

    float getRealValueAtOffset(void *pageRecord, int offset);


    int createLeafEntry(const Attribute &attribute, const void* key, const RID &rid, void *record, int &recordLen);

    RC readLeafEntryAtOffset(int offset, void *pageData, const Attribute &attribute, void *currentOffsetEntry,
                             int &currentOffsetEntryLen);

    RC squeezeEntryIntoLeaf(void *pageData, const Attribute &attribute, void* entry, int entryLen, const void *key, const RID &rid);

    RC persistIndexRootNodeMap();

    void printBTreeRecursively(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum, int depth) const;

    RC setFreeSpace(void *pageData, unsigned short freeSpace);

    RC splitLeafNode(void *pageData, void* newPageData, const Attribute &attribute, void* entry, int entryLen, const void *key, const RID &rid, int &location);

    RC setPageType(void *pageData, unsigned short pageType);

    PageNum getRightInsertPage(void *pageData, const Attribute &attribute, const void *key);

    int getEndOfRecordOffsetFromPage(void *pageData);

    RC squeezeEntryIntoNonLeaf(void *pageData, const Attribute &attribute, const void *key, const PageNum pointerPageNum, bool flagx);

    RC splitNonLeafNode(void *pageData, void *newPageData, const void* key, const Attribute &attribute, const PageNum pointerPageNum, int &location);

    PageNum getNextSiblingPage(void *pageData);

    RC setNextSiblingPage(void *pageData, PageNum pageNum);

	protected:
		IndexManager();
		~IndexManager();



};

typedef enum {
    LEAF = 0,
    NON_LEAF
} PAGE_TYPE;


class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    //class members
    IXFileHandle* ixfileHandle;
    Attribute attribute;
    void* lowKey;
    void* highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    IndexManager* indexManager;

    int scanOffset;
    PageNum leafPageNum;
    bool end;
    void* scanPageData;
    float getRealValueAtOffset(void *pageRecord, int offset);
    int getIntValueAtOffset(void *pageRecord, int offset);
    int findLeaf(IndexManager &indexManager, void* pageData, PageNum currPageNum);
};



class IXFileHandle: public FileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    string fileName;

//    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
