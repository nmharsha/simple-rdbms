#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_8(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 1 Custom *****" << endl;

    RC rc;
    string fileName = "test8Custom";

    // Create a file named "test8"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "test8"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    for(int count = 0; count < 90; count++) {
    		cout << endl << "Count: " << count << endl << endl;
		// Insert a record into a file and print the record
		prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, record, &recordSize);
		cout << endl << "Inserting Data:" << endl;
		rbfm->printRecord(recordDescriptor, record);

		rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
		assert(rc == success && "Inserting a record should not fail.");

		// Given the rid, read the record from file
		rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
		assert(rc == success && "Reading a record should not fail.");

		cout << endl << "Returned Data:" << endl;
		rbfm->printRecord(recordDescriptor, returnedData);

		// Compare whether the two memory blocks are the same
		if(memcmp(record, returnedData, recordSize) != 0)
		{
			cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
			free(record);
			free(returnedData);
			return -1;
		}
    }
    
    // need to fix => oldoffset always starts from 0, it should start from the start offset where the record resides
    // just change the offset to slotOffset - I think?

    cout << endl;

    // ****************************** test the same length update
    void *updateRecord1 = malloc(100);
    void *returnedData1 = malloc(100);

    int updateRecordSize1 = 0;
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "SameSame", 25, 177.8, 6200, updateRecord1, &updateRecordSize1);

    cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

    rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord1, rid);
    assert(rc == success && "Updating a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData1);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData1);

    // Compare whether the two memory blocks are the same
    if(memcmp(updateRecord1, returnedData1, updateRecordSize1) != 0)
    {
        cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
        free(updateRecord1);
        free(returnedData1);
        return -1;
    }
    free(updateRecord1);

    // ****************************** test smaller length update

    void *updateRecord2 = malloc(100);
    void *returnedData2 = malloc(100);

	int updateRecordSize2 = 0;
	prepareRecord(recordDescriptor.size(), nullsIndicator, 5, "Small", 25, 177.8, 6200, updateRecord2, &updateRecordSize2);

	cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

	rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord2, rid);
	assert(rc == success && "Updating a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData2);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData2);

	// Compare whether the two memory blocks are the same
	if(memcmp(updateRecord2, returnedData2, updateRecordSize2) != 0)
	{
		cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
		free(updateRecord2);
		free(returnedData2);
		return -1;
	}
	free(updateRecord2);


    // ****************************** test bigger length update

    void *updateRecord3 = malloc(100);
    void *returnedData3 = malloc(100);

	int updateRecordSize3 = 0;
	prepareRecord(recordDescriptor.size(), nullsIndicator, 10, "MyAnteater", 25, 177.8, 6200, updateRecord3, &updateRecordSize3);

	cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

	rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord3, rid);
	assert(rc == success && "Updating a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData3);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData3);

	// Compare whether the two memory blocks are the same
	if(memcmp(updateRecord3, returnedData3, updateRecordSize3) != 0)
	{
		cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
		free(updateRecord3);
		free(returnedData3);
		return -1;
	}
	free(updateRecord3);

    // ****************************** test bigger length update for remote insert

    void *updateRecord4 = malloc(100);
    void *returnedData4 = malloc(100);

	int updateRecordSize4 = 0;
	prepareRecord(recordDescriptor.size(), nullsIndicator, 47, "MyAnteater_123456789_123456789_123456789_123456", 25, 177.8, 6200, updateRecord4, &updateRecordSize4);

	cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

	rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord4, rid);
	assert(rc == success && "Updating a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData4);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData4);

	// Compare whether the two memory blocks are the same
	if(memcmp(updateRecord4, returnedData4, updateRecordSize4) != 0)
	{
		cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
		free(updateRecord4);
		free(returnedData4);
		return -1;
	}
	free(updateRecord4);

	// One more
    void *updateRecord5 = malloc(4096);
    void *returnedData5 = malloc(4096);
	string longstr;
	for (int i = 0; i < 4005; i++)
	{
		longstr.push_back('a');
	}

	int updateRecordSize5 = 0;
	prepareRecord(recordDescriptor.size(), nullsIndicator, 4005, longstr, 25, 177.8, 6200, updateRecord5, &updateRecordSize5);

	cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

	rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord5, rid);
	assert(rc == success && "Updating a record should not fail.");

	// Given the rid, read the record from file
	rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData5);
	assert(rc == success && "Reading a record should not fail.");

	cout << endl << "Returned Data:" << endl;
	rbfm->printRecord(recordDescriptor, returnedData5);

	// Compare whether the two memory blocks are the same
	if(memcmp(updateRecord5, returnedData5, updateRecordSize5) != 0)
	{
		cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
		free(updateRecord5);
		free(returnedData5);
		return -1;
	}
	free(updateRecord5);





	// One more
	    void *updateRecord6 = malloc(4096);
	    void *returnedData6 = malloc(4096);
		string longstr2;
		for (int i = 0; i < 4005; i++)
		{
			longstr2.push_back('b');
		}

		int updateRecordSize6 = 0;
		prepareRecord(recordDescriptor.size(), nullsIndicator, 4005, longstr2, 26, 177.8, 6200, updateRecord6, &updateRecordSize6);

		cout << endl << endl << "NOW COMES UPDATE TESTING...  " << endl << endl;

		rc = rbfm->updateRecord(fileHandle, recordDescriptor, updateRecord6, rid);
		assert(rc == success && "Updating a record should not fail.");

		// Given the rid, read the record from file
		rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData6);
		assert(rc == success && "Reading a record should not fail.");

		cout << endl << "Returned Data:" << endl;
		rbfm->printRecord(recordDescriptor, returnedData6);

		// Compare whether the two memory blocks are the same
		if(memcmp(updateRecord6, returnedData6, updateRecordSize6) != 0)
		{
			cout << "[FAIL] Test Case 8 Failed!" << endl << endl;
			free(updateRecord6);
			free(returnedData6);
			return -1;
		}
		free(updateRecord6);






    cout << endl << endl << "END UPDATE TESTING...  " << endl << endl;



    // Close the file "test8"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy the file
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");

    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    cout << "RBF Test Case 1 Custom Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    remove("test8Custom");

    RC rcmain = RBFTest_8(rbfm);
    return rcmain;
}
