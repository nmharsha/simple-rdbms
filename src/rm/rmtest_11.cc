#include "rm_test_util.h"
#include "../rbf/rbfm.h"

RC TEST_RM_11(const string &tableName, vector<RID> &rids)
{
    // Functions Tested for large tables:
    // 1. delete tuple
    // 2. read tuple
    cout << endl << "***** In RM Test Case 11 *****" << endl;

    int numTuples = 2000;
//    int numTuples = 50;
    RC rc = 0;
    void * returnedData = malloc(4000);
    
    readRIDsFromDisk(rids, numTuples);

    // Delete the first 1000 tuples
    void *buff = malloc(4000);
    vector<Attribute> attrs;
    rc = rm->getAttributes(tableName, attrs);
    for(int i = 0; i < 1000; i++)
    {
        cout << "Deleting Tuple Iteration: " << i << endl;
        cout << "RID Deleting: " << rids[i].pageNum << ", " << rids[i].slotNum << endl << endl << endl;
        rc = rm->deleteTuple(tableName, rids[i]);
//        rm -> printTuple(attrs, buff);
        assert(rc == success && "RelationManager::deleteTuple() should not fail.");
    }

    // Try to read the first 1000 deleted tuples
    cout<<"Printing the reading\n";
    for(int i = 0; i < 1000; i++)
    {
		rc = rm->readTuple(tableName, rids[i], returnedData);
		assert(rc != success && "RelationManager::readTuple() on a deleted tuple should fail.");
    }
    cout<<"Done with reading\n";

    for(int i = 1000; i < 2000; i++)
    {
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");
    }
    cout<<"Read tuple passed\n";
    cout << "***** Test Case 11 Finished. The result will be examined. *****" << endl << endl;

    free(returnedData);
    
    return success;
}

int main()
{
    vector<RID> rids;
    vector<int> sizes;

	// Delete Tuple
    RC rcmain = TEST_RM_11("tbl_employee4", rids);

    return rcmain;
}
