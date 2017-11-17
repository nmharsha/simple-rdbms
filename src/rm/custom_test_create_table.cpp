#include "rm_test_util.h"
#include "../rbf/rbfm.h"

vector<Attribute> getTableMetaAttributes1() {
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

int main()
{
    // By executing this script, the following tables including the system tables will be removed and constructed again.

    // Before executing rmtest_xx, you need to make sure that this script work properly.
    cout << endl << "***** RM TEST - Creating the Catalog and user tables *****" << endl;

    // Try to delete the System Catalog.
    // If this is the first time, it will generate an error. It's OK and we will ignore that.
    RC rc = rm->deleteCatalog();

    rc = rm->createCatalog();
    assert (rc == success && "Creating the Catalog should not fail.");
//    rc = rm->createTable("some_table", getTableMetaAttributes1());
    cout << "And rc is: "<<rc<<endl;


    int* tableId = new int(1);
    const char* attrs[] = {"table-id", "column-name", "column-type", "column-length", "column-position"};
    vector<string> attributeNames(attrs, attrs+3);
    RM_ScanIterator rm_scanIterator;


    rm -> scan("Columns", "table-id", NO_OP, tableId, attributeNames, rm_scanIterator);

    RID rid;
    void* data = malloc(100);
    vector<Attribute> attributes;
    cout << "\nIn process of printing\n";
    int x = 1;
    while(rm_scanIterator.getNextTuple(rid, data) != RM_EOF) {
        cout << "\nPrinting table metadata: \n";
        rm->getAttributes("Columns", attributes);
        cout << "Slot num is: " << x <<endl;
        x++;
        rm -> printTuple(attributes, data);
        memset(data, 0, 100);
    }
    cout << endl;

    FileHandle fileHandle1;
    rbfm -> openFile("Columns.tbl", fileHandle1);
    void* pageData = malloc(PAGE_SIZE);
    fileHandle1.readPage(0, pageData);
    cout << "\nNum of fucking slots is: " <<*(int*)((char*)pageData + PAGE_SIZE - 8)<<endl;
    free(pageData);
    cout<< "Very large crap"<<endl;
    cout << "MF table id is: " << rm->table_id<<endl;
    for(int i=0;i<8;i++) {
        rid.slotNum = i+1;
        rid.pageNum = 0;
        void* fdata = malloc(PAGE_SIZE);
        rbfm -> readRecord(fileHandle1, attributes, rid, fdata);
        rbfm -> printRecord(attributes, fdata);
        cout<<endl;
    }
    // Delete the actual file and create Table tbl_employee
    remove("tbl_employee");

    rc = createTable("tbl_employee");
    assert (rc == success && "Creating a table should not fail.");

//    RM_ScanIterator rm_scanIterator1;
//
//    rm -> scan("Tables", "table-id", NO_OP, tableId, attributeNames, rm_scanIterator1);
//    cout << "\n\n";
//    while(rm_scanIterator1.getNextTuple(rid, data) != RM_EOF) {
//        cout << "\nPrinting table metadata post new table create: \n";
//        rm->getAttributes("Tables", attributes);
//        rm -> printTuple(attributes, data);
//        memset(data, 0, 100);
//    }
//    cout << endl;

//    FileHandle fileHandle;
//    rbfm -> openFile("Tables.tbl", fileHandle);
//
//    RID rid1;
//    rid1.pageNum = 0;
//    rid1.slotNum = 2;
////    rm->getAttributes("Tables", attributes);
//    void* recordData = malloc(100);
//    rbfm -> readRecord(fileHandle, attributes, rid1, recordData);
//    cout<<"\n\nParticular record is: \n";
//    rbfm -> printRecord(attributes, recordData);
//    cout<<endl;

    // Delete the actual file and create Table tbl_employee
    remove("tbl_employee2");

    rc = createTable("tbl_employee2");
    assert (rc == success && "Creating a table should not fail.");

    // Delete the actual file and create Table tbl_employee
    remove("tbl_employee3");

    rc = createTable("tbl_employee3");
    assert (rc == success && "Creating a table should not fail.");

    // Delete the actual file and create Table tbl_employee
    remove("tbl_employee4");

    rc = createLargeTable("tbl_employee4");
    assert (rc == success && "Creating a table should not fail.");

    return success;
}
