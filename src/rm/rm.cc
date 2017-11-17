#include "rm.h"
#include "../rbf/rbfm.h"

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

    int result = rbfm -> deleteRecord(fileHandle, attributes, rid);
    rbfm -> closeFile(fileHandle);
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
    RBFM_ScanIterator rbfmScanIterator;
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

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return rbfmScanIterator.getNextRecord(rid, data);
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
