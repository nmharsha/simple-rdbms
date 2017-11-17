#include "rm_test_util.h"
#include "../rbf/rbfm.h"

RC TEST_RM_13(const string &tableName)
{
    // Functions Tested:
    // 1. Conditional scan
    cout << endl << "***** In RM Test Case 13 *****" << endl;

    RID rid;
    int tupleSize = 500;
    int numTuples = 1;
    void *tuple;
    void *returnedData = malloc(200);
    int ageVal = 25;
    int age = 0;

    RID rids[numTuples];
    vector<char *> tuples;

    for(int i=0;i<7;i++) {
        rid.slotNum = 1+i;
        rid.pageNum = 0;
        void* data = calloc(PAGE_SIZE, 1);
        rm->readTuple("Tables", rid, data);
        cout<<"Printing RM tuple: "<<endl;
        rm->printTuple(rm->getTableMetaAttributes(), data);

        free(data);
    }

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");


    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;

        age = (rand()%10) + 23;

        prepareTuple(attrs.size(), nullsIndicator, 6, "Tester", age, height, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success && "RelationManager::insertTuple() should not fail.");

        rids[i] = rid;
        free(tuple);
    }

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tableName, attr, GT_OP, &ageVal, attributes, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
    	age = *(int *)((char *)returnedData+1);
    	if (age <= ageVal) {
    		cout << "Returned value from a scan is not correct." << endl;
    	    cout << "***** [FAIL] Test Case 13 Failed *****" << endl << endl;
    	    rmsi.close();
    	    free(returnedData);
        free(nullsIndicator);
    	    return -1;
    	}
    }
    rmsi.close();
    free(returnedData);
    free(nullsIndicator);

    cout << "***** Test Case 13 Finished. The result will be examined. *****" << endl << endl;
    
    return success;
}

void main1()
{
    fstream f;
    f.open("testing", fstream::out);
    f<<1;
    f.seekg(0, ios::beg);
    f<<2;
}
int main()
{
	// Scan with conditions
    remove("tbl_b_employee4");
    RC rcmain = createTable("tbl_b_employee4");
//    main1();
    rcmain = TEST_RM_13("tbl_b_employee4");

    return 0;
}
