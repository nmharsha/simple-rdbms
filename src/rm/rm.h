
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
# define TABLES_TABLE_FILE_NAME "Tables.tbl"
# define COLUMNS_TABLE_FILE_NAME "Columns.tbl"
# define TABLES_TABLE_NAME "Tables"
# define COLUMNS_TABLE_NAME "Columns"
# define TABLE_ID "table-id"
# define TABLE_NAME "table-name"
# define TABLE_FILE_NAME "file-name"
# define COLUMN_NAME "column-name"

class RM_IndexScanIterator {
public:
    RM_IndexScanIterator();  	// Constructor
    ~RM_IndexScanIterator() {}; 	// Destructor

    IX_ScanIterator ix_scanIterator;
    IXFileHandle ixFileHandle;

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
    RC close();             			// Terminate index scan
};

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() {};
    ~RM_ScanIterator() {};

    RBFM_ScanIterator rbfmScanIterator;
    FileHandle fileHandle;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);
    RC close();
};


// Relation Manager
class RelationManager
{
public:
    static RelationManager* instance();

    int table_id;

    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    RC createIndex(const string &tableName, const string &attributeName);

    RC destroyIndex(const string &tableName, const string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const string &tableName,
                 const string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator
    );

// Extra credit work (10 points)
public:
    RC addAttribute(const string &tableName, const Attribute &attr);

    RC dropAttribute(const string &tableName, const string &attributeName);

    vector<Attribute> createColumnAttributes();

    vector<Attribute> getTableMetaAttributes();

    RC fetchRIDs(const string &tableName, const string conditionalAttribute, const CompOp compOp, void *value,
                 const vector<Attribute> attributes, vector<RID> &rids);

private:


protected:
    RelationManager();
    ~RelationManager();


    int getTableId(const string tableName);

    string getTableFileName(const string tableName);

    Attribute getAttributeFromName(vector<Attribute> &attributes, string attributeName);

    RC getAttributeFromRecord(const void *data, void *attributeData, string attributeName, vector<Attribute> attributes);
};

#endif
