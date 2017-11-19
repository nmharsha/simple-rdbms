#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

#include "ix.h"
#include "ix_test_util.h"
#include "../rbf/rbfm.h"

IndexManager *indexManager;

RC testCase_1(const string &indexFileName)
{
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Create Index File -- when index file is already created **
    // 4. Open Index File ** -- when a file handle is already opened **
    // 5. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In IX Test Case 1 *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // create duplicate index file
    rc = indexManager->createFile(indexFileName);
    assert(rc != success && "Calling indexManager::createFile() on an existing file should fail.");

    // open index file again using the file handle that is already opened.
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc != success && "Calling indexManager::openFile() using an already opened file handle should fail.");

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

int testCase_basicSplit(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Disk I/O check of Insertion - CollectCounterValues **
    // 4. print B+ Tree **
    // 5. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In IX Test Case 2 *****" << endl;

    RID rid;
    int key = 200;
    rid.pageNum = 500;
    rid.slotNum = 20;

    RID rid2;
    int key2 = 250;
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;


    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    for(int i=0;i<350;i++) {
        rid.pageNum = i;
        rid.slotNum = i+1;
        key = i+1;
        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    } 

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attribute);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

int testCase_2(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Disk I/O check of Insertion - CollectCounterValues **
    // 4. print B+ Tree **
    // 5. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In IX Test Case 2 *****" << endl;

    RID rid;
    int key = 200;
    rid.pageNum = 500;
    rid.slotNum = 20;

    RID rid2;
    int key2 = 250;
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;


    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

//    for(int i=0;i<350;i++) {
//        rid.pageNum = i;
//        rid.slotNum = i+1;
//        key = i+1;
//        rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
//        assert(rc == success && "indexManager::insertEntry() should not fail.");
//    }

    // insert entry
    rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attribute);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

RC test_basicIntInsertion() {
        const string indexFileName = "age_idx";

    remove(indexFileName.c_str());
    remove("root_nodes");

    indexManager = IndexManager::instance();

    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;
    RC result = 0;


    result = testCase_1(indexFileName.c_str());
    if (result == success) {
        cerr << "***** IX Test Case 1 finished. The result will be examined. *****" << endl;
    } else {
        cerr << "***** [FAIL] IX Test Case 1 failed. *****" << endl;
        return fail;
    }
    result = testCase_2(indexFileName, attrAge);
    if (result == success) {
        cerr << "***** IX Test Case 2 finished. The result will be examined. *****" << endl;
    } else {
        cerr << "***** [FAIL] IX Test Case 2 failed. *****" << endl;
    }
    return 0;
}

RC test_basicRealInsertion(const string &indexFileName, const Attribute &attribute) {
    cerr << endl << "***** In IX Test Case 2 *****" << endl;

    RID rid;
    float key = 20.4;
    rid.pageNum = 500;
    rid.slotNum = 20;

    RID rid2;
    float key2 = 25.6;
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;


    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    // insert entry
    rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rid.pageNum += 100;
    rid.slotNum += 100;

    rid2.pageNum += 100;
    rid2.slotNum += 100;

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, &key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attribute);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

RC testVarcharInsertion(const string &indexFileName, const Attribute &attribute) {
    cerr << endl << "***** In IX Test Case 2 *****" << endl;

    RID rid;
    void* key = calloc(100, 1);
    *(int*)key = 6;
    char* keyString = "Harsha";
    memcpy((char*)key+4, keyString, 6);
    rid.pageNum = 500;
    rid.slotNum = 20;

    RID rid2;
    void* key2 = calloc(100, 1);
    *(int*)key2 = 5;
    char* keyString2 = "Rahil";
    memcpy((char*)key2+4, keyString2, 5);
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;


    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    // insert entry
    rc = indexManager->insertEntry(ixfileHandle, attribute, key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rid.pageNum += 100;
    rid.slotNum += 100;

    rid2.pageNum += 100;
    rid2.slotNum += 100;

    rc = indexManager->insertEntry(ixfileHandle, attribute, key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    rc = indexManager->insertEntry(ixfileHandle, attribute, key2, rid2);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attribute);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");
    free(key);
    free(key2);

    return success;
}

RC test_leafRootSplitFloat() {
    RID rid;
    float key = 20.4;
    rid.pageNum = 500;
    rid.slotNum = 20;

    RID rid2;
    float key2 = 25.6;
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;

    const string indexFileNameFloat = "height_idx";
    remove(indexFileNameFloat.c_str());
    remove("root_nodes");
    indexManager = IndexManager::instance();
    Attribute attrHeight;
    attrHeight.length = 4;
    attrHeight.type = TypeReal;
    attrHeight.name = "height";

    testCase_1(indexFileNameFloat);


    rc = indexManager->openFile(indexFileNameFloat, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    for(int i=0;i<3;i++) {
        rid.pageNum = i;
        rid.slotNum = i+1;
        key = key+1;
        rc = indexManager->insertEntry(ixfileHandle, attrHeight, &key, rid);
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attrHeight);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

RC test_leafRootSplitVarchar() {
    RID rid;
    void* key = calloc(100, 1);
    *(int*)key = 6;
    char* keyString = "Harsha";
    memcpy((char*)key+4, keyString, 6);
    rid.pageNum = 500;
    rid.slotNum = 20;



    RID rid2;
    void* key2 = calloc(100, 1);
    *(int*)key2 = 5;
    char* keyString2 = "Rahil";
    memcpy((char*)key2+4, keyString2, 5);
    rid2.pageNum = 510;
    rid2.slotNum = 22;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    int rc;

    const string indexFileNameVarChar = "name_idx";
    remove(indexFileNameVarChar.c_str());
    remove("root_nodes");
    indexManager = IndexManager::instance();
    Attribute attrName;
    attrName.type = TypeVarChar;
    attrName.length = 30;
    attrName.name = "name";

    testCase_1(indexFileNameVarChar);


    rc = indexManager->openFile(indexFileNameVarChar, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");
//    indexManager->indexRootNodeMap;


    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    for(int i=0;i<350;i++) {
        rid.pageNum = i;
        rid.slotNum = i+1;
        rc = indexManager->insertEntry(ixfileHandle, attrName, key, rid);
        if(rc != 0) {
            cout << "Fucked up for i = " << i << endl;
        }
        cout << "So far so good"<<endl;
        assert(rc == success && "indexManager::insertEntry() should not fail.");
    }

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attrName);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");
    free(key);
    free(key2);

    return success;
}

RC test_basicRealInsertion() {
        cout << "\n\nNow testing float insertion" << endl;
    int result;

    const string indexFileNameFloat = "height_idx";
    remove(indexFileNameFloat.c_str());
    Attribute attrHeight;
    attrHeight.length = 4;
    attrHeight.type = TypeReal;
    attrHeight.name = "height";

    indexManager = IndexManager::instance();

    result = testCase_1(indexFileNameFloat);
    if (result == success) {
        cerr << "***** IX Test Case 1 for float finished. The result will be examined. *****" << endl;
    } else {
        cerr << "***** [FAIL] IX Test Case 1 for float failed. *****" << endl;
        return fail;
    }

    result = test_basicRealInsertion(indexFileNameFloat, attrHeight);
    if (result == success) {
        cerr << "***** IX Test Case 2 for float finished. The result will be examined. *****" << endl;
//        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case 2 for float failed. *****" << endl;
//        return fail;
    }
    return 0;
}

RC test_basicVarCharInsertion() {
        cout << "\n\nNow testing varchar insertion" << endl;
    int result;

    const string indexFileNameVarChar = "name_idx";
    remove(indexFileNameVarChar.c_str());
    remove("root_nodes");
    Attribute attrName;
    attrName.type = TypeVarChar;
    attrName.length = 30;
    attrName.name = "name";

    indexManager = IndexManager::instance();

    result = testCase_1(indexFileNameVarChar);
    if (result == success) {
        cerr << "***** IX Test Case 1 for varchar finished. The result will be examined. *****" << endl;
    } else {
        cerr << "***** [FAIL] IX Test Case 1 for varchar failed. *****" << endl;
        return fail;
    }

    result = testVarcharInsertion(indexFileNameVarChar, attrName);
    if (result == success) {
        cerr << "***** IX Test Case 2 for varchar finished. The result will be examined. *****" << endl;
//        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case 2 for varchar failed. *****" << endl;
//        return fail;
    }
    return 0;
}


int main()
{
//    main1();
//    return 0;
    // Global Initialization
    test_basicIntInsertion();
//    test_basicRealInsertion();
//    test_basicVarCharInsertion();
//    test_leafRootSplitFloat();
//    test_leafRootSplitVarchar();
    return 0;

}

