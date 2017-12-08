#include "rm.h"
#include "../rbf/rbfm.h"
#include "../ix/ix.h"
#include <cstring>

int debug1 = 0;
//int table_id = 1;

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
    table_id = 1;
}

RelationManager::~RelationManager()
{
}

void createRecordsForColumns(void* data, int id, const string columnName, const int columnType, const int columnLength, const int columnPosition) {
    byte nullDescriptor = 0x00;

    int offset = 0;
    memcpy((char*)data, &nullDescriptor, sizeof(byte));
    offset += sizeof(byte);
    memcpy((char*)data+offset, &id, sizeof(int));
    offset += sizeof(int);
    *(int*)((char*)data + offset) = columnName.size();
    offset += sizeof(int);
    memcpy((char*)data + offset, columnName.c_str(), columnName.size());
    offset += columnName.size();
    memcpy((char*)data+offset, &columnType, sizeof(int));
    offset += sizeof(int);
    *(int*)((char*)data + offset) = columnLength;
    offset += sizeof(int);
    memcpy((char*)data+offset, &columnPosition, sizeof(int));
    offset += sizeof(int);
}

void createRecordsForTable(void* data, const int id, const string tableName, const string fileName) {
    byte nullDescriptor= 0x00;

    int offset = 0;
    memcpy((char*)data, &nullDescriptor, sizeof(byte));
    offset += sizeof(byte);
    *(int*)((char*)data + offset) = id;
    offset += sizeof(int);
    *(int*)((char*)data + offset) = tableName.size();
    offset += sizeof(int);
    memcpy((char*)data + offset, tableName.c_str(), tableName.size());
    offset += tableName.size();
    *(int*)((char*)data + offset) = fileName.size();
    offset += sizeof(int);
    memcpy((char*)data + offset, fileName.c_str(), fileName.size());
    offset += fileName.size();
//    cout << "\n\nActual tableName: " << tableName.c_str()<<endl;
}

void getTableMetaAttributes(vector<Attribute> &attributes) {
    Attribute a;
    a.length = 4;
    a.name = "table-id";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 50;
    a.name = "table-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
    a.length = 50;
    a.name = "file-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
}

vector<Attribute> RelationManager::getTableMetaAttributes() {
    vector<Attribute> attributes;
    Attribute a;
    a.length = 4;
    a.name = "table-id";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 50;
    a.name = "table-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
    a.length = 50;
    a.name = "file-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
    return attributes;
}

void createColumnAttributes(vector<Attribute> &attributes) {
    Attribute a;
    a.length = 4;
    a.name = "table-id";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 50;
    a.name = "column-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-type";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-length";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-position";
    a.type = TypeInt;
    attributes.push_back(a);
}

vector<Attribute> RelationManager::createColumnAttributes() {
    vector<Attribute> attributes;
    Attribute a;
    a.length = 4;
    a.name = "table-id";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 50;
    a.name = "column-name";
    a.type = TypeVarChar;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-type";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-length";
    a.type = TypeInt;
    attributes.push_back(a);
    a.length = 4;
    a.name = "column-position";
    a.type = TypeInt;
    attributes.push_back(a);
    return attributes;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName) {
    IndexManager* indexManager = IndexManager::instance();
    string indexFileName = tableName + "_" + attributeName;
    int result = indexManager -> createFile(indexFileName);

    //if there are already records in the file add these to index
    vector<Attribute> attrs;
    getAttributes(tableName, attrs);
    vector<string> attrNames;
    attrNames.push_back(attributeName);
    RM_ScanIterator rm_scanIterator;
    this -> scan(tableName, "", NO_OP, NULL, attrNames, rm_scanIterator); //blind scan. This assumption may kick me in the ass later

    RID rid;
    void* data = calloc(PAGE_SIZE,1);
    void* attributeData = calloc(PAGE_SIZE, 1);
    IXFileHandle ixFileHandle;
    result = indexManager -> openFile(indexFileName, ixFileHandle);
    if(result != 0) {
        cout << "Opening of the index file fails!" << endl;
    }
    Attribute indexedAttr;
    for(int i=0;i<attrs.size();i++) {
        if(attrs[i].name == attributeName) {
            indexedAttr = attrs[i];
        }
    }
//    cout << "Printing before once!" << endl;
    while(rm_scanIterator.getNextTuple(rid, data) != RM_EOF) {
//        cout << "Printing a couple of them!" << endl;
        char nullByte = *(char*)data;
        if(nullByte <= 0) {
            indexManager -> insertEntry(ixFileHandle, indexedAttr, attributeData, rid);
        } else {
            cout << "Attribute to be indexed is null" << endl;
        }
    }
    free(attributeData);
    rm_scanIterator.close();
    indexManager -> closeFile(ixFileHandle);
    free(data);
    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName) {
    IndexManager* indexManager = IndexManager::instance();
    return indexManager->destroyFile(tableName + "_" + attributeName);
}

RC RelationManager::createCatalog()
{
    rbfm -> createFile(TABLES_TABLE_FILE_NAME);
    rbfm -> createFile(COLUMNS_TABLE_FILE_NAME);

    fstream f;
    f.open("auto_inc", fstream::out);
    f<<0;
    f.close();

//    FileHandle* tableFileHandle = new FileHandle();
//    rbfm -> openFile(TABLES_TABLE_FILE_NAME, *tableFileHandle);
//
//    FileHandle* columnsFileHandle = new FileHandle();
//    rbfm -> openFile(COLUMNS_TABLE_FILE_NAME, *columnsFileHandle);

    vector<Attribute> tableMetaAttributes = getTableMetaAttributes();;
    vector<Attribute> columnMetaAttributes = createColumnAttributes();

    if(this -> createTable(TABLES_TABLE_NAME, tableMetaAttributes) != 0) {
        return -1;
    }
    if(this -> createTable(COLUMNS_TABLE_NAME, columnMetaAttributes)) {
        return -1;
    }
    return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    if(rbfm -> destroyFile(TABLES_TABLE_FILE_NAME) != 0) {
        return -1;
    }
    return rbfm -> destroyFile(COLUMNS_TABLE_FILE_NAME);
//    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string tableFileName = tableName + ".tbl";
    int result = rbfm -> createFile(tableFileName);
    if(result != 0 && tableName.compare(TABLES_TABLE_NAME) != 0 && tableName.compare(COLUMNS_TABLE_NAME) != 0)
        return -1;

    FileHandle tableFileHandle;
    result = rbfm -> openFile(TABLES_TABLE_FILE_NAME, tableFileHandle);
    if(result != 0)
        return -1;

    void* data = calloc(PAGE_SIZE,1);
    RID rid;

    int tabId;

    fstream f;
    f.open("auto_inc");
    f>>tabId;
    tabId++;
    f.seekg(0, ios::beg);
    f<<tabId;
    f.close();
    createRecordsForTable(data, tabId, tableName, tableFileName); //TODO table id implementation
    if(debug1) {
        cout<< "\nPrinting data for the " + tableName + " table:\n";
        rbfm -> printRecord(getTableMetaAttributes(), data);
    }
//    cout<<"\nTable id is: "<<table_id<<endl;
    insertTuple(TABLES_TABLE_NAME, data, rid);

    void** data2 = (void**)calloc(sizeof(void*)*attrs.size(), 1);
    for(int i=0;i<attrs.size();i++) {
        *(data2+i) = calloc(PAGE_SIZE* sizeof(char), 1);
        createRecordsForColumns(*(data2+i), tabId, attrs[i].name, attrs[i].type, attrs[i].length, i+1); //TODO table id implementation
        insertTuple(COLUMNS_TABLE_NAME, *(data2+i), rid);
    }

//    table_id += 1;

    free(data);
    free(data2);
    return 0;
}

/**
 * Returns the RIDs of the records which satisfy the given condition
 * @param tableName
 * @param conditionalAttribute
 * @param compOp
 * @param value
 * @param attributes
 * @param rids
 * @return
 */
RC RelationManager::fetchRIDs(const string& tableName, const string conditionalAttribute, const CompOp compOp, void* value, const vector<Attribute> attributes, vector<RID> &rids) {
    RM_ScanIterator rm_scanIterator;
    vector<string> stringAttributes;
    for(vector<Attribute>::const_iterator it = attributes.begin(); it != attributes.end(); ++it) {
        stringAttributes.push_back((*it).name);
    }
    this -> scan(tableName, conditionalAttribute, compOp, value, stringAttributes, rm_scanIterator);
    RID rid;
    void* data = calloc(PAGE_SIZE, 1);
    while(rm_scanIterator.getNextTuple(rid, data) != RM_EOF) {
        rids.push_back(rid);
    }
    free(data);
    return 0;
}

int RelationManager::getTableId(const string tableName) {
    void* value = calloc(sizeof(char)*tableName.size() + sizeof(int), 1);
    *(int*)value = tableName.size();
    memcpy((char*)value+4, tableName.c_str(), tableName.size());

    vector<RID> rids;
    fetchRIDs(TABLES_TABLE_NAME, TABLE_NAME, EQ_OP, value, getTableMetaAttributes(), rids);
    if(rids.size() <= 0)
        return -1;

    void* data = calloc(sizeof(PAGE_SIZE), 1);
    this -> readAttribute(TABLES_TABLE_NAME, rids[0], TABLE_ID, data);
    return *(int*)((char*)data + 1);
}

string RelationManager::getTableFileName(const string tableName) {

    void* value = calloc(sizeof(char)*tableName.size() + sizeof(int), 1);
    *(int*)value = tableName.size();
    memcpy((char*)value+4, tableName.c_str(), tableName.size());

    if(debug1) cout<<"String value is: "<<((char*)value+4)<<endl;

    vector<RID> rids;
    fetchRIDs(TABLES_TABLE_NAME, TABLE_NAME, EQ_OP, value, getTableMetaAttributes(), rids);

    char* data = (char*)calloc(PAGE_SIZE, 1);
    this -> readAttribute(TABLES_TABLE_NAME, rids[0], TABLE_FILE_NAME, data);
    string fileName = (char*)data + 5;
//    cout << "\nTable file name: " << fileName<<endl;
    return fileName;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(tableName == TABLES_TABLE_NAME || tableName == COLUMNS_TABLE_NAME)
        return -1;
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    CompOp compOp = EQ_OP;

    void* value = calloc(sizeof(char)*tableName.size(), 1);
    memcpy((char*)value, tableName.c_str(), tableName.size());

    vector<RID> rids;
    fetchRIDs(TABLES_TABLE_NAME, TABLE_NAME, compOp, value, getTableMetaAttributes(), rids);

    int tableId =  getTableId(tableName);

    for(int i=0; i<rids.size();i++) {
        this -> deleteTuple(TABLES_TABLE_NAME, rids[i]);
    }
    rids.clear();
    fetchRIDs(COLUMNS_TABLE_NAME, TABLE_ID, compOp, &tableId, createColumnAttributes(), rids);
    for(int i=0; i<rids.size();i++) {
        this -> deleteTuple(COLUMNS_TABLE_NAME, rids[i]);
    }
    free(value);
    if(rbfm -> destroyFile(tableName + ".tbl") != 0) {
        return -1;
    }

    IndexManager* indexManager = IndexManager::instance();
    for(map<string, PageNum>::iterator it = indexManager->indexRootNodeMap.begin(); it!=indexManager->indexRootNodeMap.end(); ++it) {
        string indexFileName = it->first;
//        char* c = indexFileName.c_str();
        string next;
        vector<string> result;

        for (string::const_iterator itt = indexFileName.begin(); itt != indexFileName.end(); itt++) {
            if(*itt == '_') {
                if (!next.empty()) {
                    // Add them to the result vector
                    result.push_back(next);
                    next.clear();
                }
            } else {
                next += *itt;
            }
        }
        if (!next.empty())
            result.push_back(next);

        string tableNameInd = result[0];
        cout << "Column index file name is: " << tableNameInd << endl;
        string attributeNameInd = result[1];
        cout << "Column index column name is: " << attributeNameInd << endl;
        if(tableNameInd == tableName) {
            if(indexManager -> destroyFile(indexFileName) != 0) {
                cout << "Destroy index failed";
                return -1;
            }
        }
    }

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
//    cout <<"\n\n\n\n\n\n------------------ Inside get attributes-------------------\n\n\n\n";
    if(tableName == TABLES_TABLE_NAME) {
        attrs = getTableMetaAttributes();
//        cout << "Tables table name" << endl;
        return 0;
    } else if(tableName == COLUMNS_TABLE_NAME) {
        attrs = createColumnAttributes();
//        cout << "columns table name" << endl;
        return 0;
    }



    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

    int tableId = this -> getTableId(tableName);
//    cout << "TableID: " << tableId << endl;
//    cout <<"\n\n\n\n\n\n------------------ Later on -------------------\n\n\n\n";
    vector<string> attributeNames;
//    attributeNames.push_back("table-id");
    attributeNames.push_back("column-name"); //type varchar
    attributeNames.push_back("column-type"); //type int
    attributeNames.push_back("column-length"); //type int
    attributeNames.push_back("column-position"); //type int
//    cout << "AttributeNames SIze: " << attributeNames.size();
    RM_ScanIterator rm_scanIterator;
    this -> scan(COLUMNS_TABLE_NAME, TABLE_ID, EQ_OP, &tableId, attributeNames, rm_scanIterator);

    RID rid;
    void* data = calloc(PAGE_SIZE, 1);
    while(rm_scanIterator.getNextTuple(rid, data) != RM_EOF) {
//        cout << "Getting RID: " << rid.pageNum << " ," << rid.slotNum << endl;
        Attribute a;
        int offset = 0;
        int nullAttributesIndicatorActualSize = ceil((double) attributeNames.size() / CHAR_BIT);
//        cout << "Null bytes: " << nullAttributesIndicatorActualSize << endl;
        offset += nullAttributesIndicatorActualSize;
        int columnNameLength;
        memcpy(&columnNameLength, (char*)data + offset, sizeof(int));
//        cout << "Column Name Length: " << columnNameLength << endl;
        offset += sizeof(int);
        char* columnName = (char*)calloc(columnNameLength, 1);
        int columnType;
        int columnLength;
        int columnPosition;
        memcpy(columnName, (char*)data + offset, columnNameLength);
        offset += columnNameLength;
        memcpy(&columnType, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&columnLength, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&columnPosition, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        a.name = columnName;
//        cout << "Att Name: " << a.name << endl;
        a.type = static_cast<AttrType>(columnType);
        a.length = columnLength;
        attrs.push_back(a);
        free(columnName);
    }

    free(data);
    return 0;
}
//NOTE: This is also present in qe take care
RC RelationManager::getAttributeFromRecord(const void* data, void* attributeData, string attributeName, vector<Attribute> attributes) {
    int offset = 0;
    for(int i=0;i < attributes.size(); i++) {
        int numOfNullBytes = (int)ceil((double)attributes.size()/8);
        char nullByte = *((char*)data + i/8);
        bool nullBitSet = (nullByte & (1<<((8-i%8-1)))) > 0;
//        cout << "Null bit set: " << nullBitSet << endl;
        if(attributes[i].name == attributeName) {
            if(attributes[i].type == TypeInt || attributes[i].type == TypeReal) {
                *(char*)attributeData = 0;
                memcpy((char*)attributeData + 1, (char*)data + offset, sizeof(int));
            } else {
                int varcharLen =  *(int*)((char*)data + offset);
                *(char*)attributeData = 0;
                memcpy((char*)attributeData + 1, (char*)data + offset, sizeof(int) + varcharLen);
            }
        } else {
            if(!nullBitSet) {
                if(attributes[i].type == TypeInt || attributes[i].type == TypeReal) {
                    offset += sizeof(int);
                } else {
                    int varcharLength = *(int*)((char*)data + offset);
                    offset += sizeof(int) + varcharLength;
                }
            }
        }
    }
}
Attribute RelationManager::getAttributeFromName(vector<Attribute> &attributes, string attributeName) {
    Attribute indexedAttr;
    for(int i=0;i<attributes.size();i++) {
        if(attributes[i].name == attributeName) {
            indexedAttr = attributes[i];
        }
    }
    return indexedAttr;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string fileName;

    if(tableName.compare(TABLES_TABLE_NAME) == 0 || tableName.compare(COLUMNS_TABLE_NAME) == 0) {
        vector<Attribute> attributes;
        fileName = tableName + ".tbl";
        if(tableName.compare(TABLES_TABLE_NAME) == 0)
            attributes = getTableMetaAttributes();
        else
            attributes = createColumnAttributes();
        FileHandle fileHandle;
        int result = rbfm -> openFile(fileName, fileHandle);
        rbfm -> insertRecord(fileHandle, attributes, data, rid);
        rbfm -> closeFile(fileHandle);
        return 0;
    }

    fileName = this -> getTableFileName(tableName);
    vector<Attribute> attributes;
    this -> getAttributes(tableName, attributes);
    FileHandle fileHandle;
    int result = rbfm -> openFile(fileName, fileHandle);

    if(result != 0)
        return -1;

    result = rbfm -> insertRecord(fileHandle, attributes, data, rid);
    rbfm -> closeFile(fileHandle);

    IndexManager* indexManager = IndexManager::instance();
    for(map<string, PageNum>::iterator it = indexManager->indexRootNodeMap.begin(); it!=indexManager->indexRootNodeMap.end(); ++it) {
        string indexFileName = it->first;
        string next;
        vector<string> resultSet;

        for (string::const_iterator itt = indexFileName.begin(); itt != indexFileName.end(); itt++) {
            if(*itt == '_') {
                if (!next.empty()) {
                    // Add them to the result vector
                    resultSet.push_back(next);
                    next.clear();
                }
            } else {
                next += *itt;
            }
        }
        if (!next.empty())
            resultSet.push_back(next);

        string tableNameInd = resultSet[0];
        cout << "Column index file name is: " << tableNameInd << endl;
        string attributeNameInd = resultSet[1];
        cout << "Column index column name is: " << attributeNameInd << endl;

        if(tableNameInd == tableName) {
            cout << "Equals successful" << endl;
            IXFileHandle ixFileHandle;
            result = indexManager -> openFile(indexFileName.c_str(), ixFileHandle);
            if(result == -1) {
                cout << "Opening index file failed in insert tuple" << endl;
            }
            void* attributeData = calloc(PAGE_SIZE, 1);
            getAttributeFromRecord(data, attributeData, attributeNameInd, attributes);
            indexManager -> insertEntry(ixFileHandle, getAttributeFromName(attributes, string(attributeNameInd)), attributeData, rid);
            free(attributeData);
        }
    }

    return result;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(tableName == TABLES_TABLE_NAME || tableName == COLUMNS_TABLE_NAME)
        return -1;
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string fileName = (char*)calloc(50, 1);
    fileName = this -> getTableFileName(tableName);
    vector<Attribute> attributes;
    getAttributes(tableName, attributes);
    FileHandle fileHandle;
    rbfm -> openFile(fileName, fileHandle);

    void* data = calloc(PAGE_SIZE, 1);
    rbfm -> readRecord(fileHandle, attributes, rid, data);

    int result = rbfm -> deleteRecord(fileHandle, attributes, rid);
    rbfm -> closeFile(fileHandle);

    IndexManager* indexManager = IndexManager::instance();
    for(map<string, PageNum>::iterator it = indexManager->indexRootNodeMap.begin(); it!=indexManager->indexRootNodeMap.end(); ++it) {
        string indexFileName = it->first;
        string next;
        vector<string> resultSet;

        for (string::const_iterator itt = indexFileName.begin(); itt != indexFileName.end(); itt++) {
            if(*itt == '_') {
                if (!next.empty()) {
                    // Add them to the result vector
                    resultSet.push_back(next);
                    next.clear();
                }
            } else {
                next += *itt;
            }
        }
        if (!next.empty())
            resultSet.push_back(next);

        string tableNameInd = resultSet[0];
//        cout << "Column index file name is: " << tableNameInd << endl;
        string attributeNameInd = resultSet[1];
//        cout << "Column index column name is: " << attributeNameInd << endl;
        if(tableNameInd == tableName) {
//            cout << "Equals successful" << endl;
            IXFileHandle ixFileHandle;
            result = indexManager -> openFile(indexFileName, ixFileHandle);
            if(result == -1) {
                cout << "Opening index file failed in insert tuple" << endl;
            }
            void* attributeData = calloc(PAGE_SIZE, 1);
            getAttributeFromRecord(data, attributeData, attributeNameInd, attributes);
            indexManager -> deleteEntry(ixFileHandle, getAttributeFromName(attributes, string(attributeNameInd)), attributeData, rid);
            free(attributeData);
        }
    }
    free(data);
    return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string fileName = (char*)calloc(50, 1);
    fileName = this -> getTableFileName(tableName);
    vector<Attribute> attributes;
    getAttributes(tableName, attributes);
    FileHandle fileHandle;
    rbfm -> openFile(fileName, fileHandle);

    int result = rbfm -> updateRecord(fileHandle, attributes, data, rid);
    rbfm -> closeFile(fileHandle);
    return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string fileName = (char*)calloc(50, 1);
    fileName = this -> getTableFileName(tableName);
    vector<Attribute> attributes;
    getAttributes(tableName, attributes);
    FileHandle fileHandle;
    int result = rbfm -> openFile(fileName, fileHandle);

    if(result != 0)
        return -1;

    result = rbfm -> readRecord(fileHandle, attributes, rid, data);
    rbfm -> closeFile(fileHandle);

    return result;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    rbfm -> printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    //1. get the file name corresponding to the table name
    //2. open file
    //2. construct the record descriptor
    //3. call rbfm's read attribute
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    string fileName;
    if(tableName == TABLES_TABLE_NAME) {
        fileName = string(TABLES_TABLE_NAME) + ".tbl";
    } else if(tableName == COLUMNS_TABLE_NAME) {
        fileName = string(COLUMNS_TABLE_NAME) + ".tbl";
    } else {
        fileName = this -> getTableFileName(tableName);
    }
//    cout<<"Fiel name is: " << fileName << endl;
    vector<Attribute> attributes;
    this -> getAttributes(tableName, attributes);
    FileHandle fileHandle;
    if(rbfm -> openFile(fileName, fileHandle) != 0)
        return -1;
    rbfm -> readAttribute(fileHandle, attributes, rid, attributeName, data);
    rbfm -> closeFile(fileHandle);

    return 0;
}


RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
//    RBFM_ScanIterator rbfmScanIterator;
    int result = rbfm -> openFile(tableName + ".tbl", rm_ScanIterator.fileHandle); //TODO: remove this hack
//    cout<<"Open file result: "<<result<<endl;
    vector<Attribute> attributes;

    if((tableName.compare(TABLES_TABLE_NAME) == 0) || (tableName.compare(COLUMNS_TABLE_NAME) == 0)) {
        if(tableName.compare(TABLES_TABLE_NAME) == 0)
            attributes = getTableMetaAttributes();
        else
            attributes = createColumnAttributes();
    } else
        getAttributes(tableName, attributes);
    rbfm -> scan(rm_ScanIterator.fileHandle, attributes, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmScanIterator);
//    rm_ScanIterator.rbfmScanIterator = rbfmScanIterator;
//    rbfm -> closeFile(fileHandle);
    return 0;
}


RC RelationManager::indexScan(const string &tableName,
             const string &attributeName,
             const void *lowKey,
             const void *highKey,
             bool lowKeyInclusive,
             bool highKeyInclusive,
             RM_IndexScanIterator &rm_IndexScanIterator
) {
//	cout << "scan check 1" << endl;
    IndexManager* indexManager = IndexManager::instance();
//    cout << "scan check 2" << endl;
//    IX_ScanIterator ix_scanIterator;
//    cout << "scan check 3" << endl;
    vector<Attribute> attributes;
//    cout << "scan check 4" << endl;
    getAttributes(tableName, attributes);
//    cout << "scan check 5" << endl;
    indexManager -> openFile(tableName + "_" + attributeName, rm_IndexScanIterator.ixFileHandle);
//    cout << "scan check 6" << endl;
    indexManager -> scan(rm_IndexScanIterator.ixFileHandle, getAttributeFromName(attributes, attributeName), lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scanIterator);
//    cout << "scan check 7" << endl;
    return 0;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
    return ix_scanIterator.getNextEntry(rid, key);
}

RM_IndexScanIterator::RM_IndexScanIterator(){

}
RC RM_IndexScanIterator::close() {
    IndexManager* indexManager = IndexManager::instance();
    indexManager -> closeFile(ixFileHandle);
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return rbfmScanIterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    rbfm -> closeFile(fileHandle);
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}
