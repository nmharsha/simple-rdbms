//Author: Rahil Shah
#include <zconf.h>
#include "rbfm.h"
int debug = 0;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pagedFileManager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    //Success condition
    return pagedFileManager->createFile(fileName);
    //return -1;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pagedFileManager->destroyFile(fileName);
    //return -1;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pagedFileManager->openFile(fileName,fileHandle);
    //return -1;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pagedFileManager->closeFile(fileHandle);
    //return -1;
}

void RecordBasedFileManager::addNewPageToFile(FileHandle &fileHandle) {
    void *data = calloc(PAGE_SIZE, 1);
    for(unsigned i = 0; i < PAGE_SIZE; i++)
    {
        *((char *)data+i) = '~';
    }
    fileHandle.appendPage(data);

    void* pageRecord = calloc(PAGE_SIZE, 1);
    int pageNum = fileHandle.getPersistedAppendCounter() - 1;
    fileHandle.readPage(pageNum, pageRecord);

    int space = 4088;
    memcpy((char *) pageRecord + 4092, &space, sizeof(int));
    int slots = 0;
    memcpy((char *) pageRecord + 4088, &slots, sizeof(int));
    fileHandle.writePage(pageNum, pageRecord);

    free(pageRecord);
//    int availableSpace = 4088;
//    fileHandle.currFilePointer->seekp(-4, ios::end);
//    char* availableSpacePtr = (char *) malloc(sizeof(int));
//    memcpy((char*)availableSpacePtr, &availableSpace, sizeof(int));
//    fileHandle.currFilePointer->write((char *) availableSpacePtr, sizeof(int));

//    int totalSlots = 0;
//    fileHandle.currFilePointer->seekp(-8, ios::end);
//    char* totalSlotsPtr = (char *) malloc(sizeof(int));
//    memcpy((char*)totalSlotsPtr, &totalSlots, sizeof(int));
//    fileHandle.currFilePointer->write((char *) totalSlotsPtr, sizeof(int));

    free(data);
//    free(availableSpacePtr);
//    free(totalSlotsPtr);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {


    int pagesInFile = fileHandle.getPersistedAppendCounter();
//				cout << "Total pages in the file: " << pagesInFile << endl;

    // if current pages are 0, then append a page and then
    // persist the available space in the end of the data
    int pageNumberToAddRecord = -1;
    int lastPage = -1;
    if(pagesInFile == 0) {
//					cout << endl;
//					cout << "Pages in File: " << pagesInFile << endl;
//					cout << "Need to add new page" << endl;
        addNewPageToFile(fileHandle);
//					cout << "New Page Added" << endl;
//					cout << "Updated Page Count: " << fileHandle.getPersistedAppendCounter() << endl;
//					cout << endl;
        lastPage = 0;
    }

    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
//						cout << "printing record" << endl;
//						for(int i = 0; i < 100; i++){
//							cout << *((char *)data + i);
//						}
//						cout << endl;
//						cout << "record desc size: " << recordDescriptor.size() << endl;

    vector<unsigned int> bitVector;

//		    				cout << "Printing null indicators" << endl;
//    if(debug) cout-the-way-a-number-is-stored-in-memory
    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            if(debug) cout << j << " : " << bitValue << endl;
            bitVector.push_back(bitValue);
        }
        if(debug) cout << x;
        if(debug) cout << endl;
    }
//						cout << "Verifying the bit vector" << endl;
//						for(int i = 0; i < 8 * nullFieldsIndicatorActualSize; i++) {
//								cout << i << " : " << bitVector[i] << endl;
//						}
//
//						cout << "total attributes in record descriptor: " << recordDescriptor.size() << endl;

    // counting actual nonnull attributes in recordDescriptor
    int actualNonNull = 0;

    // seeeeee if you can fix this
//		    for(int i = 0; i < recordDescriptor.size(); i++) {
//		    		Attribute currAtt = recordDescriptor[i];
//		    		if(currAtt.length > 0) {
//		    			actualNonNull++;
//		    		}
//		    }
//		    cout << "Actual non null: " << actualNonNull << endl << endl;


    unsigned int i = 0;
    int offset = nullFieldsIndicatorActualSize;
    //	    int offset = ceil((double) actualNonNull / CHAR_BIT);
    if(debug) cout << "starting offset: " << offset << endl;


    //create a new vector for storing the offsets for newOffset arrays
    vector<int> newOffsetsVector;
    vector<unsigned short> newOffsetsVectorShort;
    if(debug) cout << "why?" << endl;
    //int varcharCount = 0;
    while(i < recordDescriptor.size()) {
        Attribute currAtt = recordDescriptor[i];
        string name = currAtt.name;
        string type;
        if(currAtt.type == TypeInt) {
            type = "int";
        } else if(currAtt.type == TypeReal){
            type = "real";
        } else if(currAtt.type == TypeVarChar) {
            type = "varchar";
        }
        //int length = currAtt.length;
//							cout << "Name  : " << name << endl;
//							cout << "Type  : " << type << endl;
//							cout << "Length: " << length << endl;
        bool entry = false;
        if(debug) cout << "Type: " << type << endl;
        if(bitVector[i] == 0){
            if(debug) cout << "doesnt even go in" << endl;
            actualNonNull++;
            if(currAtt.type == TypeInt) {
                void *entryPtr;
                entryPtr = calloc(sizeof(int), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(int));
                offset += sizeof(int);
                if(debug) cout << "Entry: " << *(int *) entryPtr << endl;
                free(entryPtr);
                if(debug) cout << "Offset now" << offset << endl;
                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeReal) {
                void *entryPtr;
                entryPtr = calloc(sizeof(float), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(float));
                offset += sizeof(float);
                if(debug)				cout << "Entry: " << *(float *) entryPtr << endl;
                free(entryPtr);
                if(debug) cout << "Offset now" << offset << endl;

                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeVarChar){
                if(debug) cout << "spotted" << endl;
                void *entryPtr;
                // first take 4 bytes for the length of the string
                entryPtr = calloc(sizeof(int), 1);
                memcpy( entryPtr , (char*)data+offset, sizeof(int));

//		    							cout << "offset before varchar length: " << offset << endl;
                offset += sizeof(int); // increment by 4 bytes for length of string
//									cout << "offset after varchar length: " << offset << endl;
                if(debug) cout << "done with int" << endl;
                int len = *(int*)entryPtr;

                if(debug) cout << "The length is :" << len << endl;
                free(entryPtr);

                // offset is updated to the string start position now
                entryPtr = calloc(len, 1);
                memcpy(entryPtr, (char*) data + offset, len);
                offset += len;
//		    							cout << "offset after incrementing the length: " << offset << endl;
                //((char *) entryPtr)[len] = '\0'; //end with a null character for string
                if(debug)	cout << "Entry: " << (char *) entryPtr << endl;
                free(entryPtr);
                if(debug) cout << "Offset now" << offset << endl;

                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
                //varcharCount++;
            }
        } else {
            if(debug) cout << "lol" << endl;
        }
        if(!entry) {
//		    						cout << "Entry: NULL" << endl;
        }
//		    						cout << endl;
        i++;
    }
//		    					cout << "Actual non null: " << actualNonNull << endl << endl;


    // varChar count is just for our reference so we know when we
    // parse it, we have added a null character each time in the end
    // essentially adding one more character to new data
    // but we dont have to worry about that since we never really do anything

    // create a new Data for putting in record

    if(debug) cout << "length of data: " << offset<< endl;

    void* newData;
    int originalSize = offset;
    // length of original data containing n bytes and raw data
    int totalFieldSize = actualNonNull; // total number of fields that are not null
    int offsetsOfNonNulls = totalFieldSize * sizeof(int);
    // total size needed to keep offsets of all nonnull fields right bit

    int offsetsOfNonNullsShort = totalFieldSize * FIELD_OFFSET_SIZE;

    // orig: int totalNewDataSize = originalSize + totalFieldSize + offsetsOfNonNulls;

    // update to this
    //int totalNewDataSize = originalSize + offsetsOfNonNulls;
    int totalNewDataSize = originalSize + offsetsOfNonNullsShort;

    if(debug) cout << "total new data size: " << totalNewDataSize << endl;

    newData = calloc(totalNewDataSize, 1);

    int newDataOffset = 0;
    int oldDataOffset = 0;
    memcpy((char*)newData + newDataOffset, (char*) data + oldDataOffset, nullFieldsIndicatorActualSize);
    newDataOffset += nullFieldsIndicatorActualSize;
    oldDataOffset += nullFieldsIndicatorActualSize;

    if(debug) cout << "Verifying copied bit vector" << endl;

    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)newData + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            int bitValue = x[j];
            if(debug) cout << j << " : " << bitValue << endl;
            //		    			bitVector.push_back(bitValue);
        }
        if(debug) cout << endl;
    }

    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;

//		    				cout << "Adding total NonNull fields: " << actualNonNull << endl;

    // found out how to add & to the int to convert to pointer from prepare record in testcase8/custom
//		    memcpy((char*) newData + newDataOffset, &actualNonNull, sizeof(int));
//		    newDataOffset += sizeof(int);
    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    if(debug) cout << endl;
    if(debug) cout << "Verifying the nonNull count... " << endl;

//		    void *verify;
//			verify = malloc(sizeof(int));
//			memcpy(verify, (char*) newData + newDataOffset - 4, sizeof(int));
//
//						cout << "Nonnulls in new data: " << *(int *) verify << endl;
//
//			free(verify);

    if(debug) cout << endl;
    if(debug) cout << "Now adding the new bit offset vector" << endl;
//						cout << "First printing to verify end of field offsets" << endl;
//						for(int i = 0; i < newOffsetsVector.size(); i++) {
//							cout << newOffsetsVector[i] << endl;
//						}

    if(debug) cout << " SHORT First printing to verify end of field offsets" << endl;
    for(int i = 0; i < newOffsetsVectorShort.size(); i++) {
        if(debug) cout << newOffsetsVectorShort[i] << endl;
    }


    if(debug) cout << "Set correct offsets positions in the vector: " << endl;
    if(debug) cout << "adding size of totalfiends and offset vector itself: " << /*totalFieldSize +*/ offsetsOfNonNullsShort << endl;
    if(debug) cout << "New offsets" << endl;

//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//				newOffsetsVector[i] += /*totalFieldSize +*/ offsetsOfNonNulls;
//				cout << newOffsetsVector[i] << endl;
//			}

// *********************************************************************************************
//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//				newOffsetsVectorShort[i] += /*totalFieldSize +*/ offsetsOfNonNullsShort;
//				cout << newOffsetsVectorShort[i] << endl;
//			}
    // no need to do this, offsets are relative, just compare in the read
// *********************************************************************************************

    if(debug) cout << "Verified Correctly!" << endl;
//
//			cout << endl;
//
//			cout << "Now copying them into the newData" << endl;

//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//			    memcpy((char*) newData + newDataOffset, &newOffsetsVector[i], sizeof(int));
//			    newDataOffset += sizeof(int);
//			}

    for(unsigned int i = 0; i < newOffsetsVectorShort.size(); i++) {
        memcpy((char*) newData + newDataOffset, &newOffsetsVectorShort[i], FIELD_OFFSET_SIZE);
        newDataOffset += FIELD_OFFSET_SIZE;
    }

//			cout << "Added!" << endl;
//			cout << "Now verifying" << endl;

//			int verifyOffsetsCounter = actualNonNull * sizeof(int);
//			for(int i = 0; i < actualNonNull; i++){
//				void *verify;
//				verify = malloc(sizeof(int));
//				memcpy(verify, (char*) newData + newDataOffset - verifyOffsetsCounter, sizeof(int));
////				cout << *(int *) verify << endl;
//				free(verify);
//				verifyOffsetsCounter -= sizeof(int);
//			}

    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    //
    if(debug) cout << "Awesome, properly added the offsets" << endl;
    if(debug) cout << endl;
    //
    if(debug) cout << "Now add the rest of the data and verify offsets as you go" << endl;
    if(debug) cout << endl;
    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    if(debug) cout << endl;

    int rawOldDataSize = offset - nullFieldsIndicatorActualSize;
//		    cout << "raw data in bytes to be added: " << rawOldDataSize << endl;

    memcpy((char*)newData + newDataOffset, (char*) data + oldDataOffset, rawOldDataSize);
    newDataOffset += rawOldDataSize;

    int newDataSize = newDataOffset;
//		    cout << "newDataOffset ends at: " << newDataOffset - 1 << endl;
//		    cout << "newData Length: " << newDataSize << endl;

    int freeBytesNeeded = newDataSize + 12;
    // 4 bytes each for slotID, slotSize, slotOffSet = 12

    lastPage = fileHandle.getPersistedAppendCounter() - 1;

    //check current(last) page if it has space
    int availableSpace = getAvailableSpaceOnPage(lastPage, fileHandle);
//	cout << "Last page:" << lastPage << endl;
//	cout << "Available space on last page: " << availableSpace << endl;

    if(pageNumberToAddRecord < 0) {
        if(availableSpace >= freeBytesNeeded) {
            pageNumberToAddRecord = lastPage;
        } else {
            int currentPage = 0;
            while(currentPage < lastPage) {
                availableSpace = getAvailableSpaceOnPage(lastPage, fileHandle);
                if(availableSpace >= freeBytesNeeded) {
                    pageNumberToAddRecord = currentPage;
                    break;
                }
                currentPage++;
            }
        }
    }

    // if pageNumberToAddRecord is still -1 then append a page
    if(pageNumberToAddRecord < 0) {
        addNewPageToFile(fileHandle);
        pageNumberToAddRecord = fileHandle.getPersistedAppendCounter() - 1;
        availableSpace = getAvailableSpaceOnPage(pageNumberToAddRecord, fileHandle);
    }


    int numSlots = getTotalSlotsOnPage(pageNumberToAddRecord, fileHandle);
    if(debug) cout << "Current Number of Slots on the page to add record: " << numSlots << endl;

//	cout << "PageNumberToAddRecord: " << pageNumberToAddRecord << endl;
//	cout << "Available Space: " << availableSpace << endl;

    void* pageRecord; //pageRecord that will be updated and added back!
    pageRecord = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(pageNumberToAddRecord, pageRecord);


    if(debug) cout <<" HERERERERE " << endl;


    //find a delete slot
    int insertAtSlotID;
    int insertAtOffset;
    bool lastSlot = false;
    bool foundDeletedSlot = false;
    int count = 0;

    int reUseID;
    int updateOffsetStartID;
    if(numSlots != 0) {
        int slotDirectoryOffset = PAGE_SIZE - (12 + 8);
        int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
        int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
        int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);

        while(count < numSlots && slotOffset >= 0) {
            if(slotSize == 0) {
                foundDeletedSlot = true;
                if(debug) cout << "Its found deleted slot" << endl;
//				insertAtSlotID = slotDirectoryOffset;
                insertAtSlotID = count + 1;
                if(debug) cout << "Resuing the slotID: " << insertAtSlotID << endl;
                reUseID = count + 1;
                if(count + 1 == numSlots){
                    insertAtOffset = PAGE_SIZE - (availableSpace + 8 + 12*numSlots);
                    lastSlot = true;
                } else {
//					insertAtOffset = PAGE_SIZE - (availableSpace + 8 + 12*(count+1));
                    insertAtOffset = getIntValueAtOffset(pageRecord, PAGE_SIZE - (8 + 12*(count+1)));
                }
                if(debug) cout << "Inserting At the offset of deleted slotID which is: " << insertAtOffset << endl;
//				else {
//					//find the next non-deleted directory
//					count++;
//					slotDirectoryOffset -=12;
//					slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
//					slotSizeDirectoryOffset = slotDirectoryOffset + 4;
//					slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
//
//					while((slotSize == 0 || slotOffset < 0) && (count < numSlots)) {
//						count++;
//						slotDirectoryOffset -=12;
//						slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
//						slotSizeDirectoryOffset = slotDirectoryOffset + 4;
//						slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
//					}
//					updateOffsetStartID = count + 1;
//					insertAtOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
//				}
                break;
            }
            slotDirectoryOffset -= 12;
            slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
            slotSizeDirectoryOffset = slotDirectoryOffset + 4;
            slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);

            count++;
        }
    }


    if(debug) cout << "Searching deleted slot is done" << endl << endl << endl;

    if(foundDeletedSlot == true) {
        freeBytesNeeded -= 12; // or not write this and do updatedspace = availableSpace - newDataSize;
        int updatedSpace = availableSpace - freeBytesNeeded;
        updateAvailableSpace(pageRecord, updatedSpace);
        if(lastSlot == true) {
            if(debug) cout << "Its the last slot" << endl;
            int currSlotID = numSlots;
            int directoryOffset = PAGE_SIZE - (currSlotID * 12 + 8);
            int offsetToRecord = PAGE_SIZE - (updatedSpace + currSlotID * 12 + 8) - newDataSize;
            writeDirectoryEntry(directoryOffset, offsetToRecord, newDataSize, currSlotID, pageRecord);
            writeRecordData(offsetToRecord, newDataSize, pageRecord, newData);
            fileHandle.writePage(pageNumberToAddRecord, pageRecord);
            rid.pageNum = pageNumberToAddRecord;
            rid.slotNum = currSlotID;
            free(pageRecord);
            free(newData);
            vector<unsigned int>().swap(bitVector);
            return 0;
        } else {
            if(debug) cout << "Its some middle slot" << endl;

            // extract the offset of the directory after the current one
            int currSlotID = reUseID;
            int directoryOffset = PAGE_SIZE - (currSlotID * 12 + 8);
            int offsetToRecord = insertAtOffset;
            writeDirectoryEntry(directoryOffset, offsetToRecord, newDataSize, currSlotID, pageRecord);

            if(debug) cout << "Its some middle slot2" << endl;
            if(debug) cout << "InsertAtOffset: " << insertAtOffset << endl;
            //store the buffer because everything will be shifter to the right
            int bufferSize = PAGE_SIZE - (availableSpace + numSlots*12 + 8) - insertAtOffset;
            void* buffer = calloc(bufferSize, 1);
            if(debug) cout << "Not a problem declaring buffer " << endl;
            memcpy((char *) buffer, (char *) pageRecord + insertAtOffset, bufferSize);

            if(debug) cout << "Its some middle slot3" << endl;

            // update the all the rest offsets by += length
            int updateRestCount = reUseID + 1;
            directoryOffset = PAGE_SIZE - (updateRestCount * 12 + 8);
            int slotOffset1 = getIntValueAtOffset(pageRecord, directoryOffset);
            int slotSizeDirectoryOffset1 = directoryOffset + 4;
            int slotSize1 = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset1);

            while(updateRestCount <= numSlots && slotOffset1 > 0) {

                int newOffset = getIntValueAtOffset(pageRecord, directoryOffset) + newDataSize;
                memcpy((char *) pageRecord + directoryOffset, &newOffset, sizeof(int));

                directoryOffset -= 12;
                slotOffset1 = getIntValueAtOffset(pageRecord, directoryOffset);
                slotSizeDirectoryOffset1 = directoryOffset + 4;
                slotSize1 = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset1);

            }
            // write the record data now


            memcpy((char *) pageRecord + insertAtOffset, (char*) newData, newDataSize);
            // now copy the buffer
            memcpy((char *) pageRecord + insertAtOffset + newDataSize, (char*) buffer, bufferSize);

            fileHandle.writePage(pageNumberToAddRecord, pageRecord);
            rid.pageNum = pageNumberToAddRecord;
            rid.slotNum = currSlotID;
            free(pageRecord);
            free(newData);
            free(buffer);
            vector<unsigned int>().swap(bitVector);
            return 0;
        }
    }




    //Verifying Page Record
//	cout << endl;
//	cout << "Verifying Page Record Stuff: " << endl;
    void *verifyPageRecordAvailableSpace;
    verifyPageRecordAvailableSpace = calloc(sizeof(int), 1);
    memcpy(verifyPageRecordAvailableSpace, (char*) pageRecord + 4092, sizeof(int));
//	cout << "Space should be 4088, and it is: ";
//	cout << *(int *) verifyPageRecordAvailableSpace << endl;
//	cout << ":)" << endl;
//
//	cout << "Free bytes needed: " << freeBytesNeeded << endl;
    int updatedSpace = availableSpace - freeBytesNeeded;
//	cout << "updatedSpace should be: " << updatedSpace << endl;

    updateAvailableSpace(pageRecord, updatedSpace);
//	cout << "Verifying proper update of free space:" << endl;
    memcpy(verifyPageRecordAvailableSpace, (char*) pageRecord + 4092, sizeof(int));
//		cout << "Space should be 4031, and it is: ";
//		cout << *(int *) verifyPageRecordAvailableSpace << endl;
//		cout << ":)" << endl;

    // now updating the slots
//	cout << "Updating the number of slots" << endl;
    numSlots += 1;
    updateSlotNum(pageRecord, numSlots);
    int currSlotID = getSlotNum(pageRecord);
//	cout << "Verifying... " << endl;
//	cout << "Slots should be 1, and it is: ";
//	cout << currSlotID << endl;
//	cout << ":)" << endl << endl;
//
//	cout << "Now creating the Directory Entry: " << endl;
    int directoryOffset = PAGE_SIZE - (currSlotID * 12 + 8);
    int offsetToRecord = PAGE_SIZE - (updatedSpace + currSlotID * 12 + 8) - newDataSize;
//	cout << "Directory Offset: " << directoryOffset << endl;
//	cout << "Expected: 4076" << endl;
//	cout << "Offset to Record: " << offsetToRecord << endl;
//	cout << "Expected: 0" << endl;
    writeDirectoryEntry(directoryOffset, offsetToRecord, newDataSize, currSlotID, pageRecord);
//	cout << "Fantastic :)" << endl;
//	cout << "Confirming values in the pageRecord: " << endl;
//	cout << "offsetToRecord, newDataSize, currSlotID: " << endl;
//	cout << "[" << getIntValueAtOffset(pageRecord, directoryOffset) << "]";
//	cout << "[" << getIntValueAtOffset(pageRecord, directoryOffset + 4) << "]";
//	cout << "[" << getIntValueAtOffset(pageRecord, directoryOffset + 8) << "]" << endl << endl;
//
//	cout << "Almost there, Now writing the new Data..." << endl;
    writeRecordData(offsetToRecord, newDataSize, pageRecord, newData);


//	cout << "Now writing to File..." << endl;
    fileHandle.writePage(pageNumberToAddRecord, pageRecord);
    rid.pageNum = pageNumberToAddRecord;
    rid.slotNum = currSlotID;

    if (debug) cout << "New Data Size in Insert at RID: " << rid.pageNum << ", " << rid.slotNum << " : " << newDataSize << endl;


    free(pageRecord);
    free(newData);
    free(verifyPageRecordAvailableSpace);
    vector<unsigned int>().swap(bitVector);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    if(debug) cout << "\n\n\nReading the Record \n\n\n";
    if(debug) cout << "record descriptor size: " << recordDescriptor.size() << endl;
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;
    void* pageRecord = calloc(PAGE_SIZE, 1);
    if(fileHandle.readPage(pageNum, pageRecord) == -1) {
        return -1;
    }
    int totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);
    if(totalSlots < slotNum){
        return -1;
    }


    if(debug) cout << "Access Page Num: " << pageNum << endl;
    if(debug) cout << "Access Slot Num: " << slotNum << endl;

    int slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
    int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
    int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
    int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);

    if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
    if(debug) cout << "Slot Offset: " << slotOffset << endl;
    if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
    if(debug) cout << "Slot Size: " << slotSize << endl;
//	if(slotSize <= 0 && slotOffset < 0) {
//		return -1;
//	} // this is what i had before when i was putting -1 in the first 4 bytes when deleted
    if(slotSize <= 0) {
        return -1;
    }
    while(slotOffset < 0) {
        if(debug) cout << "Remote Page Reading" << endl;

        pageNum = slotSize;
        slotNum = (-1 * slotOffset);

        fileHandle.readPage(pageNum, pageRecord);

        if(debug) cout << "Access Page Num: " << pageNum << endl;
        if(debug) cout << "Access Slot Num: " << slotNum << endl;

        slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
        slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
        slotSizeDirectoryOffset = slotDirectoryOffset + 4;
        slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);

        if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
        if(debug) cout << "Slot Offset: " << slotOffset << endl;
        if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
        if(debug) cout << "Slot Size: " << slotSize << endl;
    }

    void* slotRawData = calloc(slotSize, 1);
    getAnyValueAtOffset(pageRecord, slotOffset, slotRawData, slotSize);

    //get the nonnull count
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
//	cout << "Total null indicator bytes: " << nullFieldsIndicatorActualSize << endl;

    void* nonNullBytes = calloc(nullFieldsIndicatorActualSize, 1);
//    cout << "Malloc non null bytes works" << endl;
    getAnyValueAtOffset(slotRawData, 0, nonNullBytes, nullFieldsIndicatorActualSize);
//    memcpy((char *) nonNullBytes, (char *)slotRawData + 0, nullFieldsIndicatorActualSize);

    // uncomment above ^

//	cout << "verifying the bits, in reverse order?" << endl;
//
//    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
//    		std::bitset<8> x(*((char *)nonNullBytes + i));
//    		for(int j = 0; j < x.size(); j++) {
//    			int bitValue = x[j];
//    			cout << j << " : " << bitValue << endl;
//    		}
//        cout << endl;
//    }

    int rawOffset = 0;
    int dataOffset = 0;

    //Now copying into data
//    cout << "Now copying into data..." << endl;
    memcpy((char *) data + dataOffset, (char *) slotRawData + rawOffset, nullFieldsIndicatorActualSize);


    int totalNonNullFieldsCurrent = 0;
//    cout << "Verify from data: in reverse order: " << endl;
//    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
//    		    		std::bitset<8> x(*((char *)data + i));
//    		    		for(int j = 0; j < x.size(); j++) {
//    		    			int bitValue = x[j];
//    		    			cout << j << " : " << bitValue << endl;
//    		    			if(bitValue == 0) {
//    		    				totalNonNullFieldsCurrent++;
//    		    			}
//    		    		}
//    		        cout << endl;
//    	}

    if(debug) cout << "Verify from data... " << endl;
    int vSize = recordDescriptor.size();
    for(int i = 0; i < nullFieldsIndicatorActualSize && vSize > 0; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0 && vSize > 0; j--) {
            int bitValue = x[j];
            if(debug) cout << j << " : " << bitValue << endl;
            if(bitValue == 0) {
                totalNonNullFieldsCurrent++;
            }
            vSize--;
        }
        if(debug) cout << endl;
    }


    if(debug) cout << "Total NonNull Fields are: " << totalNonNullFieldsCurrent << endl;
    if(debug) cout << "Verified Correctly! " << endl;
    if(debug) cout << "Incremented the offsets..." << endl;

    rawOffset += nullFieldsIndicatorActualSize;
    dataOffset += nullFieldsIndicatorActualSize;

    if(debug) cout << "rawOffset: " << rawOffset << endl;
    if(debug) cout << "dataOffset: " << dataOffset << endl;

// orig: 	int totalNonNullFields = getIntValueAtOffset(slotRawData, rawOffset);
//	cout << "Total Non Null Fields: " << totalNonNullFields << endl;

    // increasing offset to skip the nonNullField Count
    if(debug) cout << "Increasing offset to skip the nonNullField Count" << endl;
// orig: 	rawOffset += sizeof(int);

    if(debug) cout << "rawOffset: " << rawOffset << endl;
    if(debug) cout << "dataOffset: " << dataOffset << endl;

    // orig: int dataSize = slotSize - (sizeof(int) + (totalNonNullFields * sizeof(int)) + nullFieldsIndicatorActualSize);
    int dataSize = slotSize - ((totalNonNullFieldsCurrent * FIELD_OFFSET_SIZE) + nullFieldsIndicatorActualSize);
    if(debug) cout << "Fields Data size: " << dataSize << endl;

    rawOffset += (totalNonNullFieldsCurrent * FIELD_OFFSET_SIZE);

//	cout << "rawOffset: " << rawOffset << endl;
//    cout << "dataOffset: " << dataOffset << endl;

//    cout << "Now copying the rest of the field raw data to data* " << endl;
    memcpy((char*) data + dataOffset, (char *) slotRawData + rawOffset, dataSize);

    rawOffset += dataSize;
    dataOffset += dataSize;

//    cout << "rawOffset: " << rawOffset << endl;
//    cout << "dataOffset: " << dataOffset << endl;

//    cout << "End of Read Record :)" << endl;

    free(pageRecord);
    free(slotRawData);
    free(nonNullBytes);

    return 0;

}


void RecordBasedFileManager::writeRecordData(int offset, int size, void* pageRecord, void* data) {
    memcpy((char *) pageRecord + offset, (char *)data, size);
}

void RecordBasedFileManager::writeDirectoryEntry(int position, int offsetToRecord, int recordSize, int slotID, void* pageRecord) {
    memcpy((char *) pageRecord + position, &offsetToRecord, sizeof(int));
    position += sizeof(int);
    memcpy((char *) pageRecord + position, &recordSize, sizeof(int));
    position += sizeof(int);
    memcpy((char *) pageRecord + position, &slotID, sizeof(int));
    position += sizeof(int);
}

void RecordBasedFileManager::getAnyValueAtOffset(void *pageRecord, int offset, void* buffer, int size) {
    memcpy(buffer, (char*) pageRecord + offset, size);
}

int RecordBasedFileManager::getIntValueAtOffset(void *pageRecord, int offset) {
    void *verify;
    verify = calloc(sizeof(int), 1);
    memcpy(verify, (char*) pageRecord + offset, sizeof(int));
    int offsetResult = *(int *) verify;
    free(verify);
    return offsetResult;
}
int RecordBasedFileManager::getAvailableSpaceOfPageRecord(void *pageRecord) {
    void *verify;
    verify = calloc(sizeof(int), 1);
    memcpy(verify, (char*) pageRecord + 4092, sizeof(int));
    int space = *(int *) verify;
    free(verify);
    return space;
}

int RecordBasedFileManager::getSlotNum(void *pageRecord) {
    void *verify;
    verify = calloc(sizeof(int), 1);
    memcpy(verify, (char*) pageRecord + 4088, sizeof(int));
    int slotNum = *(int *) verify;
    free(verify);
    return slotNum;
}

void RecordBasedFileManager::updateAvailableSpace(void *pageRecord, int updatedSpace) {
    memcpy((char*)pageRecord + 4092, &updatedSpace, sizeof(int));
}

void RecordBasedFileManager::updateSlotNum(void *pageRecord, int updatedSlots) {
    memcpy((char*)pageRecord + 4088, &updatedSlots, sizeof(int));
}

int RecordBasedFileManager::getAvailableSpaceOnPage(int pageNum, FileHandle &fileHandle){

    void *pageRecord = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(pageNum, pageRecord);
    int getSpace = getIntValueAtOffset(pageRecord, 4092);
    free(pageRecord);
    return getSpace;

//	char* getAvailableSpace = (char *) malloc(sizeof(int));
//	fileHandle.currFilePointer->seekg((pageNum + 1)*PAGE_SIZE + 4092, fileHandle.currFilePointer->beg);
//	fileHandle.currFilePointer->read(getAvailableSpace, sizeof(int));
//	int space = *((int *) getAvailableSpace);
//	free(getAvailableSpace);
//	return space;
}

int RecordBasedFileManager::getTotalSlotsOnPage(int pageNum, FileHandle &fileHandle){
    void *pageRecord = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(pageNum, pageRecord);
    int getSlots = getIntValueAtOffset(pageRecord, 4088);
    free(pageRecord);
    return getSlots;

//	char* getTotalSlotsOnPage = (char *) malloc(sizeof(int));
//	fileHandle.currFilePointer->seekg((pageNum + 1)*PAGE_SIZE + 4088, fileHandle.currFilePointer->beg);
//	fileHandle.currFilePointer->read(getTotalSlotsOnPage, sizeof(int));
//	int space = *((int *) getTotalSlotsOnPage);
//	free(getTotalSlotsOnPage);
//	return space;
}

int RecordBasedFileManager::getTotalSlotsOnPage(void* pageData){

    int getSlots = getIntValueAtOffset(pageData, 4088);
    return getSlots;

//	char* getTotalSlotsOnPage = (char *) malloc(sizeof(int));
//	fileHandle.currFilePointer->seekg((pageNum + 1)*PAGE_SIZE + 4088, fileHandle.currFilePointer->beg);
//	fileHandle.currFilePointer->read(getTotalSlotsOnPage, sizeof(int));
//	int space = *((int *) getTotalSlotsOnPage);
//	free(getTotalSlotsOnPage);
//	return space;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

    // -------------------- version from insert ----------------//
//	cout << "\n\n\n IN PRINT \n\n\n" << endl;
    ////		int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
//			cout << "printing record" << endl;
//		    for(int i = 0; i < 100; i++){
//		        cout << *((char *)data + i);
//		    }
//		    cout << endl;
//		    cout << "record desc size: " << recordDescriptor.size() << endl;

    vector<unsigned int> bitVector;

//		    cout << "Printing null indicators" << endl;
//    if(debug) cout-the-way-a-number-is-stored-in-memory
    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
//		    			cout << j << " : " << bitValue << endl;
            bitVector.push_back(bitValue);
        }
        if(debug) cout << x;
//		        cout << endl;
    }
//		    cout << "Verifying the bit vector" << endl;
//		    for(int i = 0; i < 8 * nullFieldsIndicatorActualSize; i++) {
//		    		cout << i << " : " << bitVector[i] << endl;
//		    }

//		    cout << "total attributes in record descriptor: " << recordDescriptor.size() << endl;

    // counting actual nonnull attributes in recordDescriptor
//		    int actualNonNull = 0;

//		    for(unsigned int i = 0; i < recordDescriptor.size(); i++) {
//		    		Attribute currAtt = recordDescriptor[i];
//		    		if(currAtt.length > 0) {
//		    			actualNonNull++;
//		    		}
//		    }
//		    cout << "Actual non null: " << actualNonNull << endl << endl;
    unsigned int i = 0;
    int offset = nullFieldsIndicatorActualSize;
    //	    int offset = ceil((double) actualNonNull / CHAR_BIT);
//		    cout << "starting offset: " << offset << endl;


    //create a new vector for storing the offsets for newOffset arrays
    vector<int> newOffsetsVector;

    //int varcharCount = 0;
    while(i < recordDescriptor.size()) {
        Attribute currAtt = recordDescriptor[i];
        string name = currAtt.name;
        string type;
        if(currAtt.type == TypeInt) {
            type = "int";
        } else if(currAtt.type == TypeReal){
            type = "real";
        } else if(currAtt.type == TypeVarChar) {
            type = "varchar";
        }
//		    		int length = currAtt.length;
//		    		cout << "Name  : " << name << endl;
//		    		cout << "Type  : " << type << endl;
//		    		cout << "Length: " << length << endl;
        cout << name << ": ";
        bool entry = false;
        if(bitVector[i] == 0){
            if(currAtt.type == TypeInt) {
                void *entryPtr;
                entryPtr = calloc(sizeof(int), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(int));
                offset += sizeof(int);
                cout << *(int *) entryPtr;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeReal) {
                void *entryPtr;
                entryPtr = calloc(sizeof(float), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(float));
                offset += sizeof(float);
                cout << *(float *) entryPtr;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeVarChar){
                void *entryPtr;
                // first take 4 bytes for the length of the string
                entryPtr = calloc(sizeof(int), 1);
                memcpy( entryPtr , (char*)data+offset, sizeof(int));

//		    			 	cout << "offset before varchar length: " << offset << endl;
                offset += sizeof(int); // increment by 4 bytes for length of string
//		    				cout << "offset after varchar length: " << offset << endl;

                int len = *(int*)entryPtr;
                if(debug) cout << "The length is :" << len << " ";
                free(entryPtr);

                // offset is updated to the string start position now
                entryPtr = calloc(len+10, 1);
                memcpy(entryPtr, (char*) data + offset, len);
                offset += len;
//		    				cout << "offset after incrementing the length: " << offset << endl;
                //((char *) entryPtr)[len] = '\0'; //end with a null character for string
                cout << (char *) entryPtr;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                entry = true;
                //varcharCount++;
            }

        }
        if(!entry) {
            cout << "NULL" << endl;
        }
        cout << "    ";
        i++;
    }
    if(debug) cout << endl;
    vector<unsigned int>().swap(bitVector);

    return 0;
    //return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    /* First grab the original data on the RID, store it and get the size;
     * Then parse the newData as how you do in print or insert
     * Compare the size and decide what to do based on that
     *
     * */







    if(debug) cout << "IN UPDATE BHAI" << endl;




    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
//						cout << "printing record" << endl;
//						for(int i = 0; i < 100; i++){
//							cout << *((char *)data + i);
//						}
//						cout << endl;
//						cout << "record desc size: " << recordDescriptor.size() << endl;

    vector<unsigned int> bitVector;

//		    				cout << "Printing null indicators" << endl;
//    if(debug) cout-the-way-a-number-is-stored-in-memory
    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            if(debug) cout << j << " : " << bitValue << endl;
            bitVector.push_back(bitValue);
        }
        if(debug) cout << x;
        if(debug) cout << endl;
    }
//						cout << "Verifying the bit vector" << endl;
//						for(int i = 0; i < 8 * nullFieldsIndicatorActualSize; i++) {
//								cout << i << " : " << bitVector[i] << endl;
//						}
//
//						cout << "total attributes in record descriptor: " << recordDescriptor.size() << endl;

    // counting actual nonnull attributes in recordDescriptor
    int actualNonNull = 0;

    // seeeeee if you can fix this
//		    for(int i = 0; i < recordDescriptor.size(); i++) {
//		    		Attribute currAtt = recordDescriptor[i];
//		    		if(currAtt.length > 0) {
//		    			actualNonNull++;
//		    		}
//		    }
//		    cout << "Actual non null: " << actualNonNull << endl << endl;


    unsigned int i = 0;
    int offset = nullFieldsIndicatorActualSize;
    //	    int offset = ceil((double) actualNonNull / CHAR_BIT);
//		    				cout << "starting offset: " << offset << endl;


    //create a new vector for storing the offsets for newOffset arrays
    vector<int> newOffsetsVector;
    vector<unsigned short> newOffsetsVectorShort;

    //int varcharCount = 0;
    while(i < recordDescriptor.size()) {
        Attribute currAtt = recordDescriptor[i];
        string name = currAtt.name;
        string type;
        if(currAtt.type == TypeInt) {
            type = "int";
        } else if(currAtt.type == TypeReal){
            type = "real";
        } else if(currAtt.type == TypeVarChar) {
            type = "varchar";
        }
        //int length = currAtt.length;
//							cout << "Name  : " << name << endl;
//							cout << "Type  : " << type << endl;
//							cout << "Length: " << length << endl;
        bool entry = false;
        if(bitVector[i] == 0){
            actualNonNull++;
            if(currAtt.type == TypeInt) {
                void *entryPtr;
                entryPtr = malloc(sizeof(int));
                memcpy(entryPtr, (char*) data + offset, sizeof(int));
                offset += sizeof(int);
//		    						cout << "Entry: " << *(int *) entryPtr << endl;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeReal) {
                void *entryPtr;
                entryPtr = malloc(sizeof(float));
                memcpy(entryPtr, (char*) data + offset, sizeof(float));
                offset += sizeof(float);
//		        					cout << "Entry: " << *(float *) entryPtr << endl;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
            }
            else if(currAtt.type == TypeVarChar){
                void *entryPtr;
                // first take 4 bytes for the length of the string
                entryPtr = malloc(sizeof(int));
                memcpy( entryPtr , (char*)data+offset, sizeof(int));

//		    							cout << "offset before varchar length: " << offset << endl;
                offset += sizeof(int); // increment by 4 bytes for length of string
//									cout << "offset after varchar length: " << offset << endl;

                int len = *(int*)entryPtr;
                if(debug) cout << "UPDATE VARCHAR The length is :" << len << endl;
                free(entryPtr);

                // offset is updated to the string start position now
                entryPtr = malloc(len);
                memcpy(entryPtr, (char*) data + offset, len);
                offset += len;
//		    							cout << "offset after incrementing the length: " << offset << endl;
                //((char *) entryPtr)[len] = '\0'; //end with a null character for string
//		    							cout << "Entry: " << (char *) entryPtr << endl;
                free(entryPtr);
                newOffsetsVector.push_back(offset - 1);
                newOffsetsVectorShort.push_back((unsigned short) offset - 1);
                entry = true;
                //varcharCount++;
            }
        }
        if(!entry) {
//		    						cout << "Entry: NULL" << endl;
        }
//		    						cout << endl;
        i++;
    }
//		    					cout << "Actual non null: " << actualNonNull << endl << endl;


    // varChar count is just for our reference so we know when we
    // parse it, we have added a null character each time in the end
    // essentially adding one more character to new data
    // but we dont have to worry about that since we never really do anything

    // create a new Data for putting in record

    if(debug) cout << "length of data: " << offset<< endl;

    void* newData;
    int originalSize = offset;
    // length of original data containing n bytes and raw data
    int totalFieldSize = actualNonNull; // total number of fields that are not null
    int offsetsOfNonNulls = totalFieldSize * sizeof(int);
    // total size needed to keep offsets of all nonnull fields right bit

    int offsetsOfNonNullsShort = totalFieldSize * FIELD_OFFSET_SIZE;

    // orig: int totalNewDataSize = originalSize + totalFieldSize + offsetsOfNonNulls;

    // update to this
    //int totalNewDataSize = originalSize + offsetsOfNonNulls;
    int totalNewDataSize = originalSize + offsetsOfNonNullsShort;

    if(debug) cout << "total new data size: " << totalNewDataSize << endl;

    newData = malloc(totalNewDataSize);

    int newDataOffset = 0;
    int oldDataOffset = 0;
    memcpy((char*)newData + newDataOffset, (char*) data + oldDataOffset, nullFieldsIndicatorActualSize);
    newDataOffset += nullFieldsIndicatorActualSize;
    oldDataOffset += nullFieldsIndicatorActualSize;

    if(debug) cout << "Verifying copied bit vector" << endl;

    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)newData + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            int bitValue = x[j];
            if(debug) cout << j << " : " << bitValue << endl;
            //		    			bitVector.push_back(bitValue);
        }
        if(debug) cout << endl;
    }

    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;

//		    				cout << "Adding total NonNull fields: " << actualNonNull << endl;

    // found out how to add & to the int to convert to pointer from prepare record in testcase8/custom
//		    memcpy((char*) newData + newDataOffset, &actualNonNull, sizeof(int));
//		    newDataOffset += sizeof(int);
    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    if(debug) cout << endl;
    if(debug) cout << "Verifying the nonNull count... " << endl;

//		    void *verify;
//			verify = malloc(sizeof(int));
//			memcpy(verify, (char*) newData + newDataOffset - 4, sizeof(int));
//
//						cout << "Nonnulls in new data: " << *(int *) verify << endl;
//
//			free(verify);

    if(debug) cout << endl;
    if(debug) cout << "Now adding the new bit offset vector" << endl;
//						cout << "First printing to verify end of field offsets" << endl;
//						for(int i = 0; i < newOffsetsVector.size(); i++) {
//							cout << newOffsetsVector[i] << endl;
//						}

    if(debug) cout << " SHORT First printing to verify end of field offsets" << endl;
    for(int i = 0; i < newOffsetsVectorShort.size(); i++) {
        if(debug) cout << newOffsetsVectorShort[i] << endl;
    }


    if(debug) cout << "Set correct offsets positions in the vector: " << endl;
    if(debug) cout << "adding size of totalfiends and offset vector itself: " << /*totalFieldSize +*/ offsetsOfNonNulls << endl;
    if(debug) cout << "New offsets" << endl;

//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//				newOffsetsVector[i] += /*totalFieldSize +*/ offsetsOfNonNulls;
//				cout << newOffsetsVector[i] << endl;
//			}

//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//				newOffsetsVectorShort[i] += /*totalFieldSize +*/ offsetsOfNonNullsShort;
//				cout << newOffsetsVectorShort[i] << endl;
//			}

    if(debug) cout << "Verified Correctly!" << endl;
//
//			cout << endl;
//
//			cout << "Now copying them into the newData" << endl;

//			for(unsigned int i = 0; i < newOffsetsVector.size(); i++) {
//			    memcpy((char*) newData + newDataOffset, &newOffsetsVector[i], sizeof(int));
//			    newDataOffset += sizeof(int);
//			}

    for(unsigned int i = 0; i < newOffsetsVectorShort.size(); i++) {
        memcpy((char*) newData + newDataOffset, &newOffsetsVectorShort[i], FIELD_OFFSET_SIZE);
        newDataOffset += FIELD_OFFSET_SIZE;
    }

//			cout << "Added!" << endl;
//			cout << "Now verifying" << endl;

//			int verifyOffsetsCounter = actualNonNull * sizeof(int);
//			for(int i = 0; i < actualNonNull; i++){
//				void *verify;
//				verify = malloc(sizeof(int));
//				memcpy(verify, (char*) newData + newDataOffset - verifyOffsetsCounter, sizeof(int));
////				cout << *(int *) verify << endl;
//				free(verify);
//				verifyOffsetsCounter -= sizeof(int);
//			}

    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    //
    if(debug) cout << "Awesome, properly added the offsets" << endl;
    if(debug) cout << endl;
    //
    if(debug) cout << "Now add the rest of the data and verify offsets as you go" << endl;
    if(debug) cout << endl;
    if(debug) cout << "newOffset: " << newDataOffset << endl;
    if(debug) cout << "oldOffset: " << oldDataOffset << endl;
    if(debug) cout << endl;

    int rawOldDataSize = offset - nullFieldsIndicatorActualSize;
//		    cout << "raw data in bytes to be added: " << rawOldDataSize << endl;

    memcpy((char*)newData + newDataOffset, (char*) data + oldDataOffset, rawOldDataSize);
    newDataOffset += rawOldDataSize;

    int newDataSize = newDataOffset;
//		    cout << "newDataOffset ends at: " << newDataOffset - 1 << endl;
//		    cout << "newData Length: " << newDataSize << endl;

    vector<unsigned int>().swap(bitVector);

    if(debug) cout << endl << endl << endl << "newDataSize: " << newDataSize << endl<< endl<< endl;


    //******************************************** read in the new record that is coming in *****************************

    /**
     * Few conditions to keep in mind:
     * 1. Same size record
     * 2. New Record Smaller than Original
     * 		- Update all the subsequent offsets of directories as you shift left the record slots
     *
     * 3. New Record Bigger than Original
     * 	a. Check if it fits in the page
     *		- Update all the subsequent offsets of directories as you shift right the record slots
     *	b. Check if its exceeding the available space in page
     *		- Figure it out.......
     */

    //******************************************** now read the original data and get the size for that  *****************************


    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;
    void* pageRecord = malloc(PAGE_SIZE);
    if(fileHandle.readPage(pageNum, pageRecord) == -1) {
        free(newData);
        return -1;
    }
    int totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);
    if(slotNum > totalSlots) {
        free(newData);
        return -1;
    }

    if(debug) cout << "Access Page Num: " << pageNum << endl;
    if(debug) cout << "Access Slot Num: " << slotNum << endl;
    /*
     * Right now it only calculates without delete, so doesn't iterate.
     * When delete happens, we need to iterate to find that particular slot
     *
     * Loop to find the correct slot in read,
     * */
    int slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
    int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
    int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
    int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
    int slotNumInDir = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset + 8);

    if(debug) cout << "Slot Num in Directory: " << slotNumInDir << endl;
    if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
    if(debug) cout << "Slot Offset: " << slotOffset << endl;
    if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
    if(debug) cout << "Slot Size: " << slotSize << endl;

    if(slotSize <= 0 && slotOffset < 0) {
        free(newData);
        return -1;
    }

    while(slotOffset < 0) {
        if(debug) cout << "Remote Page updating" << endl;

        pageNum = slotSize;
        slotNum = (-1 * slotOffset);

        fileHandle.readPage(pageNum, pageRecord);

        if(debug) cout << "Access Page Num: " << pageNum << endl;
        if(debug) cout << "Access Slot Num: " << slotNum << endl;

        slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
        slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
        slotSizeDirectoryOffset = slotDirectoryOffset + 4;
        slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
        slotNumInDir = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset + 8);
        totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);

        if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
        if(debug) cout << "Slot Offset: " << slotOffset << endl;
        if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
        if(debug) cout << "Slot Size: " << slotSize << endl;
    }



    //**************************** check the size
    int availableSpace = getIntValueAtOffset(pageRecord, PAGE_SIZE - 4);
    if(debug) cout << "Current Available Space: " << availableSpace << endl;
    if(debug) cout << endl << endl;


    if(slotSize == newDataSize) {
        if(debug) cout << "Sizes match, can replace the data" << endl;
        writeRecordData(slotOffset, newDataSize, pageRecord, newData);
        fileHandle.writePage(pageNum, pageRecord);
    } else {
        if(newDataSize > slotSize) {
            if(debug) cout << "New Data is bigger Size" << endl;
            if((availableSpace + slotSize - newDataSize) > 0) {
                if(debug) cout << "New Data can fit into the page, need to shift right the rest" << endl;
                // ************************************************************************************************

                // write from 0 to the offset into
                void* newPageRecord = malloc(PAGE_SIZE);
                int newPageRecordOffset = 0;
                int oldOffset = 0;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                /*
                 * Copies everything until you hit the offset
                 * */
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, slotOffset);
                newPageRecordOffset += slotOffset;
                oldOffset += slotOffset;
                if(debug) cout << "SlotOffset" << slotOffset << endl;
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                /*
                 * Now copy the newData
                 * */
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)newData, newDataSize);
                newPageRecordOffset += newDataSize;
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                /*
                 * Increase the old offset by the size of the original Data to copy the rest of the stuff in
                 *
                 * Calculate the last offset position
                 * */
                oldOffset += slotSize;
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                int lastOffsetPos = PAGE_SIZE - ((totalSlots*12) + 4 + 4 + availableSpace);
                if(debug) cout << "LastOffsetPos: " << lastOffsetPos << endl;
                int sizeToCopy = lastOffsetPos - oldOffset;
                if(debug) cout << "Size to Copy" << sizeToCopy << endl;
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, sizeToCopy);
                newPageRecordOffset += sizeToCopy;
                oldOffset += sizeToCopy;

                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                /*
                 * Add the difference of the data sizes for creating junk in the middle
                 * */
//				int junkSize = (slotSize - newDataSize);
//				cout << "JunkSize: " << junkSize << endl;
//
//				void *junk = malloc(junkSize);
//			    for(unsigned i = 0; i < junkSize; i++)
//			    {
//			        *((char *)junk+i) = '~';
//			    }
//				cout << endl;
//				cout << "OldOffset: " << oldOffset << endl;
//				cout << "NewOffset: " << newPageRecordOffset << endl;

//				memcpy((char *) newPageRecord + newPageRecordOffset, (char *)junk, junkSize);
//				newPageRecordOffset += (junkSize);

                oldOffset += (newDataSize - slotSize);
                /*
                 * By now old and newOffsets should be the same
                 * */
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

                /*
                 * Copy the rest, and then update the offsets of each slot from slot Number that got update
                 * */
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, PAGE_SIZE - newPageRecordOffset);


                /*
                 * Update available space
                 * */
                int updatedSpace = availableSpace + slotSize - newDataSize;
                if(debug) cout << "UpdateSpace" << updatedSpace << endl << endl;
                memcpy((char *) newPageRecord + 4092, &updatedSpace, sizeof(int));
                availableSpace = updatedSpace;

                int k = 0;
                int updateOffset = 4088;
                while(k < slotNum) {
                    updateOffset -= 12;
                    k++;
                }

                if(debug) cout << "UpdateOffset: " << updateOffset << endl;
                /*
                 * Change size of this directory
                 * */
                memcpy((char *) newPageRecord + updateOffset + 4, &newDataSize, sizeof(int));

                /*
                 * Now update the rest of the directory stuff
                 * */

                while(k < totalSlots) {
                    updateOffset -= 12;
                    int checkRemote = getIntValueAtOffset(newPageRecord, updateOffset + 4);
                    if(checkRemote > 0){
                        int newValue = getIntValueAtOffset(newPageRecord, updateOffset);
                        if(debug) cout << "Original Offset: " << newValue << endl;
                        newValue += (newDataSize - slotSize);
                        if(debug) cout << "Updated Offset: " << newValue << endl;
                        memcpy((char *) newPageRecord + updateOffset, &newValue, sizeof(int));
                    }
                    k++;
                }
                fileHandle.writePage(pageNum, newPageRecord);
                free(newPageRecord);

                //*************************************************************************************************
            } else {
                if(debug) cout << "New Data cannot fit into the page, shift it to other page" << endl;

                //*************************************** Shifting to another page *****************************************

                /*
                 *
                 * Mark the size in the directory as "-8"
                 * now look for the page that can fit this data, create a directory entry in it towards the end
                 * insert the data, create an RID => just call insertRecord function
                 * Once the RID is formed, just put the new RID in the record and shrink it left
                 *
                 * OMG USE MEMMOVE FOR DEFRAGMENTATION
                 * */


                RID remoteRID;




                int val = getIntValueAtOffset(newData, 9);
                if(debug) cout << "val: " << val << endl;

//				insertRecord(fileHandle, recordDescriptor, newData, remoteRID);
                insertRecord(fileHandle, recordDescriptor, data, remoteRID);


                int remotePageNum = remoteRID.pageNum;
                int remoteSlotNum = remoteRID.slotNum;


                // read that page back and mark the slotID negative for Scan purposes
                void* markNegative = malloc(PAGE_SIZE);
                fileHandle.readPage(remotePageNum, markNegative);
                int markNegativeDirectorySlotOffset = (PAGE_SIZE - (8 + 12*remoteSlotNum));
                markNegativeDirectorySlotOffset += 8; // go the the slotID

                if(debug) cout << "Mark the newly added record on remote page as negative slotID" << endl;
                int markNegativeSlotID = getIntValueAtOffset(markNegative, markNegativeDirectorySlotOffset);
                if(debug) cout << "Just verifiy: " << markNegativeSlotID << endl;
                markNegativeSlotID = markNegativeSlotID * (-1);
                if(debug) cout << "update to : " << markNegativeSlotID << endl;
                memcpy((char *) markNegative + markNegativeDirectorySlotOffset, &markNegative, sizeof(int));

                fileHandle.writePage(remotePageNum, markNegative);
                free(markNegative);
                if(debug) cout << "Done with marking negative..." << endl << endl;


                if(debug) cout << "Remote Page Number: " << remotePageNum << endl;
                if(debug) cout << "Remote Slot Number: " << remoteSlotNum << endl;
                int checkRemotePageSizeAvailable = getAvailableSpaceOnPage(remotePageNum, fileHandle);
                if(debug) cout << "Available space on remote page: " << checkRemotePageSizeAvailable << endl;

                int offsetForRemoteRID = PAGE_SIZE - 8 - 12*(slotNum);
                if(debug) cout << "offsetForRemoteRID... " << offsetForRemoteRID << endl;
                if(debug) cout << "printing before updation p: " << getIntValueAtOffset(pageRecord, offsetForRemoteRID) << endl;
                if(debug) cout << "printing before updation s: " << getIntValueAtOffset(pageRecord, offsetForRemoteRID + 4) << endl;

                remoteSlotNum *= -1;
                memcpy((char *) pageRecord + offsetForRemoteRID, &remoteSlotNum, sizeof(int));
                offsetForRemoteRID += 4;
                memcpy((char *) pageRecord + offsetForRemoteRID, &remotePageNum, sizeof(int));

                if(debug) cout << "printing updation p: " << getIntValueAtOffset(pageRecord, offsetForRemoteRID - 4) << endl;
                if(debug) cout << "printing updation s: " << getIntValueAtOffset(pageRecord, offsetForRemoteRID) << endl;


                /*
                 * Now follow the same logic as: New Data is smaller size, need to shift left the rest
                 * */

                // write from 0 to the offset into
                void* newPageRecord = malloc(PAGE_SIZE);
                int newPageRecordOffset = 0;
                int oldOffset = 0;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//				/*
//				 * Copies everything until you hit the offset
//				 * */
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, slotOffset);
                newPageRecordOffset += slotOffset;
                oldOffset += slotOffset;
//
                if(debug) cout << "SlotOffset" << slotOffset << endl;
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

//				/*
//				 * Increase the old offset by the size of the original Data to copy the rest of the stuff in
//				 *
//				 * Calculate the last offset position
//				 * */
                oldOffset += slotSize;
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
                int lastOffsetPos = PAGE_SIZE - ((totalSlots*12) + 4 + 4 + availableSpace);
                if(debug) cout << "LastOffsetPos: " << lastOffsetPos << endl;
                int sizeToCopy = lastOffsetPos - oldOffset;
                if(debug) cout << "Size to Copy" << sizeToCopy << endl;
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, sizeToCopy);
                newPageRecordOffset += sizeToCopy;
                oldOffset += sizeToCopy;
//
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//				/*
//				 * Add the difference of the data sizes for creating junk in the middle
//				 * */
                int junkSize = slotSize;
                if(debug) cout << "JunkSize: " << junkSize << endl;
//
                void *junk = malloc(junkSize);
                for(unsigned i = 0; i < junkSize; i++)
                {
                    *((char *)junk+i) = '~';
                }
                if(debug) cout << endl;
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)junk, junkSize);
                free(junk);
                newPageRecordOffset += (junkSize);
//
//				/*
//				 * By now old and newOffsets should be the same
//				 * */
                if(debug) cout << "OldOffset: " << oldOffset << endl;
                if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;


                /*
                 * Copy the rest, and then update the offsets of each slot from slot Number that got update
                 * */
                memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, PAGE_SIZE - newPageRecordOffset);


                /*
                 * Update available space
                 * */
                int updatedSpace = availableSpace + slotSize;
                if(debug) cout << "UpdateSpace" << updatedSpace << endl << endl;
                memcpy((char *) newPageRecord + 4092, &updatedSpace, sizeof(int));
                availableSpace = updatedSpace;

                int k = 0;
                int updateOffset = 4088;
                while(k < slotNum) {
                    updateOffset -= 12;
                    k++;
                }

                if(debug) cout << "UpdateOffset: " << updateOffset << endl;
                /*
                 * Change offset and size of this directory to remote RID
                 * */
//				memcpy((char *) newPageRecord + updateOffset + 4, &newDataSize, sizeof(int));

                int verifyRemoteStuffSlot = getIntValueAtOffset(newPageRecord, updateOffset);
                if(debug) cout << "HERE IT IS REMOTE CHECK1: " << verifyRemoteStuffSlot << endl;
                int verifyRemoteStuffPage = getIntValueAtOffset(newPageRecord, updateOffset + 4);
                if(debug) cout << "HERE IT IS REMOTE CHECK2: " << verifyRemoteStuffPage << endl;

                /*
                 * Now update the rest of the directory stuff
                 * */

                while(k < totalSlots) {
                    updateOffset -= 12;
                    int checkRemote = getIntValueAtOffset(newPageRecord, updateOffset + 4);
                    if(checkRemote > 0){
                        int newValue = getIntValueAtOffset(newPageRecord, updateOffset);
                        if(debug) cout << "Original Offset: " << newValue << endl;
                        newValue -= junkSize;
                        if(debug) cout << "Updated Offset: " << newValue << endl;
                        memcpy((char *) newPageRecord + updateOffset, &newValue, sizeof(int));
                    }
                    k++;
                }
                fileHandle.writePage(pageNum, newPageRecord);
                free(newPageRecord);
                // now indicate it as negative slotID because its coming from elsewhere
                // primarily for doing the scan
//				fileHandle.readPage(pageNum, newPageRecord);



                if(debug) cout << endl << endl << "Ending the REMOTE" << endl << endl;

                //*************************************************************************************************



            }
        } else {
            if(debug) cout << "New Data is smaller size, need to shift left the rest" << endl;

            // write from 0 to the offset into
            void* newPageRecord = malloc(PAGE_SIZE);
            int newPageRecordOffset = 0;
            int oldOffset = 0;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            /*
             * Copies everything until you hit the offset
             * */
            memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, slotOffset);
            newPageRecordOffset += slotOffset;
            oldOffset += slotOffset;

            if(debug) cout << "SlotOffset" << slotOffset << endl;
            if(debug) cout << endl;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            /*
             * Now copy the newData
             * */
            memcpy((char *) newPageRecord + newPageRecordOffset, (char *)newData, newDataSize);
            newPageRecordOffset += newDataSize;
            if(debug) cout << endl;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            /*
             * Increase the old offset by the size of the original Data to copy the rest of the stuff in
             *
             * Calculate the last offset position
             * */
            oldOffset += slotSize;
            if(debug) cout << endl;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            int lastOffsetPos = PAGE_SIZE - ((totalSlots*12) + 4 + 4 + availableSpace);
            if(debug) cout << "LastOffsetPos: " << lastOffsetPos << endl;
            int sizeToCopy = lastOffsetPos - oldOffset;
            if(debug) cout << "Size to Copy" << sizeToCopy << endl;
            memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, sizeToCopy);
            newPageRecordOffset += sizeToCopy;
            oldOffset += sizeToCopy;

            if(debug) cout << endl;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            /*
             * Add the difference of the data sizes for creating junk in the middle
             * */
            int junkSize = slotSize - newDataSize;
            if(debug) cout << "JunkSize: " << junkSize << endl;

            void *junk = malloc(junkSize);
            for(unsigned i = 0; i < junkSize; i++)
            {
                *((char *)junk+i) = '~';
            }
            if(debug) cout << endl;
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            memcpy((char *) newPageRecord + newPageRecordOffset, (char *)junk, junkSize);
            free(junk);
            newPageRecordOffset += (junkSize);

            /*
             * By now old and newOffsets should be the same
             * */
            if(debug) cout << "OldOffset: " << oldOffset << endl;
            if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

            /*
             * Copy the rest, and then update the offsets of each slot from slot Number that got update
             * */
            memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, PAGE_SIZE - newPageRecordOffset);


            /*
             * Update available space
             * */
            int updatedSpace = availableSpace + slotSize - newDataSize;
            if(debug) cout << "UpdateSpace" << updatedSpace << endl << endl;
            memcpy((char *) newPageRecord + 4092, &updatedSpace, sizeof(int));
            availableSpace = updatedSpace;

            int k = 0;
            int updateOffset = 4088;
            while(k < slotNum) {
                updateOffset -= 12;
                k++;
            }

            if(debug) cout << "UpdateOffset: " << updateOffset << endl;
            /*
             * Change size of this directory
             * */
            memcpy((char *) newPageRecord + updateOffset + 4, &newDataSize, sizeof(int));

            /*
             * Now update the rest of the directory stuff
             * */

            while(k < totalSlots) {
                updateOffset -= 12;
                int checkRemote = getIntValueAtOffset(newPageRecord, updateOffset + 4);
                if(checkRemote > 0){
                    int newValue = getIntValueAtOffset(newPageRecord, updateOffset);
                    if(debug) cout << "Original Offset: " << newValue << endl;
                    newValue -= junkSize;
                    if(debug) cout << "Updated Offset: " << newValue << endl;
                    memcpy((char *) newPageRecord + updateOffset, &newValue, sizeof(int));
                }
                k++;
            }
            fileHandle.writePage(pageNum, newPageRecord);
            free(newPageRecord);
        }
    }
    free(newData);
    free(pageRecord);
    if(debug) cout << "Available Space in page: " << availableSpace << endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    if(debug) cout << "\n\n\nDeleting the Record \n\n\n";
    if(debug) cout << "record descriptor size: " << recordDescriptor.size() << endl;
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;
    void* pageRecord = malloc(PAGE_SIZE);
    if(fileHandle.readPage(pageNum, pageRecord) == -1){
        return -1;
    }
    int totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);
    if(slotNum <= 0 || slotNum > totalSlots) {
        return -1;
    }
    if(debug) cout << "Access Page Num: " << pageNum << endl;
    if(debug) cout << "Access Slot Num: " << slotNum << endl;
    int availableSpace1 = getAvailableSpaceOnPage(pageNum, fileHandle);
    cout << "Available space: " << availableSpace1 << endl;

    int slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
    int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
    int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
    int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
    if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
    if(debug) cout << "Slot Offset: " << slotOffset << endl;
    if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
    if(debug) cout << "Slot Size: " << slotSize << endl;

    while(slotOffset < 0) {
        if(debug) cout << "Remote Page Deleting" << endl;
//			int var = -1;
//			memcpy((char *) pageRecord + slotDirectoryOffset, &var, 4);
        int var = 0;
        memcpy((char *) pageRecord + slotDirectoryOffset + 4, &var, 4);

        if(debug) cout << "Updated Directory of Page: "<< endl;
        if(debug) cout << getIntValueAtOffset(pageRecord, slotDirectoryOffset) << ", ";
        if(debug) cout << getIntValueAtOffset(pageRecord, slotDirectoryOffset + 4) << endl;

        fileHandle.writePage(pageNum, pageRecord);

        pageNum = slotSize;
        slotNum = (-1 * slotOffset);

        fileHandle.readPage(pageNum, pageRecord);

        if(debug) cout << "Access Page Num: " << pageNum << endl;
        if(debug) cout << "Access Slot Num: " << slotNum << endl;

        slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
        slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
        slotSizeDirectoryOffset = slotDirectoryOffset + 4;
        slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
        totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);

        if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
        if(debug) cout << "Slot Offset: " << slotOffset << endl;
        if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
        if(debug) cout << "Slot Size: " << slotSize << endl;
    }
    int availableSpace = getIntValueAtOffset(pageRecord, PAGE_SIZE - 4);


    if(debug) cout << "Delete, need to shift left the rest" << endl;

//	 write from 0 to the offset into
    void* newPageRecord = malloc(PAGE_SIZE);
    int newPageRecordOffset = 0;
    int oldOffset = 0;
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//	/*
//	 * Copies everything until you hit the offset
//	 * */
    memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, slotOffset);
    newPageRecordOffset += slotOffset;
    oldOffset += slotOffset;
//
    if(debug) cout << "SlotOffset" << slotOffset << endl;
    if(debug) cout << endl;
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//	/*
//	 * Increase the old offset by the size of the original Data to copy the rest of the stuff in
//	 *
//	 * Calculate the last offset position
//	 * */
    oldOffset += slotSize;
    if(debug) cout << endl;
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
    int lastOffsetPos = PAGE_SIZE - ((totalSlots*12) + 4 + 4 + availableSpace);
    if(debug) cout << "LastOffsetPos: " << lastOffsetPos << endl;
    int sizeToCopy = lastOffsetPos - oldOffset;
    if(debug) cout << "Size to Copy" << sizeToCopy << endl;
    memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, sizeToCopy);
    newPageRecordOffset += sizeToCopy;
    oldOffset += sizeToCopy;
//
    if(debug) cout << endl;
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//	/*
//	 * Add the difference of the data sizes for creating junk in the middle
//	 * */
    int junkSize = slotSize;
    if(debug) cout << "JunkSize: " << junkSize << endl;
//
    void *junk = malloc(junkSize);
    for(unsigned i = 0; i < junkSize; i++)
    {
        *((char *)junk+i) = '~';
    }
    if(debug) cout << endl;
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;

    memcpy((char *) newPageRecord + newPageRecordOffset, (char *)junk, junkSize);
    free(junk);
    newPageRecordOffset += (junkSize);

//	/*
//	 * By now old and newOffsets should be the same
//	 * */
    if(debug) cout << "OldOffset: " << oldOffset << endl;
    if(debug) cout << "NewOffset: " << newPageRecordOffset << endl;
//
//	/*
//	 * Copy the rest, and then update the offsets of each slot from slot Number that got update
//	 * */
    memcpy((char *) newPageRecord + newPageRecordOffset, (char *)pageRecord + oldOffset, PAGE_SIZE - newPageRecordOffset);
//
//
//	/*
//	 * Update available space
//	 * */
    int updatedSpace = availableSpace + slotSize;
    if(debug) cout << "UpdateSpace" << updatedSpace << endl << endl;
    memcpy((char *) newPageRecord + 4092, &updatedSpace, sizeof(int));
    availableSpace = updatedSpace;

    int k = 0;
    int updateOffset = 4088;
    while(k < slotNum) {
        updateOffset -= 12;
        k++;
    }
//
    if(debug) cout << "UpdateOffset: " << updateOffset << endl;
//	/*
//	 * Change size of this directory
//	 * */
//	int var = -1;
//	memcpy((char *) newPageRecord + updateOffset, &var, sizeof(int));
    int var = 0;
    memcpy((char *) newPageRecord + updateOffset + 4, &var, sizeof(int));

//
//	/*
//	 * Now update the rest of the directory stuff
//	 * */
//
    while(k < totalSlots) {
        updateOffset -= 12;
//        int checkRemote = getIntValueAtOffset(newPageRecord, updateOffset + 4);
        int checkFirst = getIntValueAtOffset(newPageRecord, updateOffset);
        int checkSecond = getIntValueAtOffset(newPageRecord, updateOffset + 4);
        if(checkFirst >= 0 && checkSecond >= 0){
            int newValue = getIntValueAtOffset(newPageRecord, updateOffset);
            if(debug) cout << "Original Offset: " << newValue << endl;
            newValue -= junkSize;
            if(debug) cout << "Updated Offset: " << newValue << endl;
            memcpy((char *) newPageRecord + updateOffset, &newValue, sizeof(int));
        }
        k++;
    }
    memcpy((char *) newPageRecord + 4092, &updatedSpace, sizeof(int));

    fileHandle.writePage(pageNum, newPageRecord);
    free(pageRecord);
    free(newPageRecord);
//}
//}
//
    if(debug) cout << "Available Space in page: " << availableSpace << endl;
//
//

    return 0;
}

RC RecordBasedFileManager::readAttributeFromBuffer(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data, void* pageData){
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;
    void* pageRecord = malloc(PAGE_SIZE);
//    if(fileHandle.readPage(pageNum, pageRecord) == -1){
//        free(pageRecord);
//        return -1;
//    }
    memcpy(pageRecord, pageData, sizeof(PAGE_SIZE));
    int totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);
    if(slotNum <= 0 || slotNum > totalSlots) {
        free(pageRecord);
        return -1;
    }
    if(debug) cout << "Access Page Num: " << pageNum << endl;
    if(debug) cout << "Access Slot Num: " << slotNum << endl;

    int slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
    int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
    int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
    int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
    if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
    if(debug) cout << "Slot Offset: " << slotOffset << endl;
    if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
    if(debug) cout << "Slot Size: " << slotSize << endl;
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);

    int count = 0;
    vector<int> bitVector;
    int rd = recordDescriptor.size();
    for(int i = 0; i < nullFieldsIndicatorActualSize && rd > 0 ; i++){
        std::bitset<8> x(*((char *)pageRecord + slotOffset + i));
        for(int j = x.size() - 1; j >= 0 && rd > 0; j--) {
            int bitValue = x[j];
            if(bitValue == 0){
                count++;
            }
            bitVector.push_back(bitValue);
            rd--;
        }
    }
    if(debug) cout << "Non null fields count: " << count << endl;

    int i = 0;
    int num = 0;
    int length = 0;
    while(i < bitVector.size() && num < count){
        int bitValue = bitVector[i];
        if(bitValue == 0) {
            if(recordDescriptor[i].name == attributeName) {
                if(debug) cout << i << " pe " << "koi mil gaya" << endl;
                int OffsetTableIndex = slotOffset + nullFieldsIndicatorActualSize + 2*num;
                if(debug) cout << "OffsetTableIndex: " << OffsetTableIndex << endl;

                void *verify;
                verify = malloc(sizeof(unsigned short));
//					memcpy((char*)verify, (char*) pageRecord + slotOffset + OffsetTableIndex, 2);
                memcpy((char*)verify, (char*) pageRecord + OffsetTableIndex, 2);

                int rightEnd = *(unsigned short *) verify;
                free(verify);

                if(debug) cout << "Right End: " << rightEnd << endl;
                if(num == 0) {
                    length = rightEnd;
                } else {

                    void *verify;
                    verify = malloc(sizeof(unsigned short));
//                    memcpy((char*)verify, (char*) pageRecord + slotOffset + OffsetTableIndex - 2, 2);
                    memcpy((char*)verify, (char*) pageRecord + OffsetTableIndex - 2, 2);

                    int rightEndPrev = *(unsigned short *) verify;
                    free(verify);
                    if(debug) cout << "Right end prev: " << rightEndPrev << endl;
                    length = rightEnd - rightEndPrev;
                }
                if(debug) cout << "length" << length << endl;
//    					data = malloc(length + nullFieldsIndicatorActualSize);

                char c = 0;
//    					memcpy((char *) data, &c, 1);
                *(char*)data = c;
//    					memcpy((char *) data, (char*)pageRecord + slotOffset, nullFieldsIndicatorActualSize);
                if(debug) cout << "toskip: " << slotOffset  + 2*count + 1 + rightEnd - length << endl;
                memcpy((char *) data + 1, (char*)pageRecord + slotOffset + 1 + 2*count + rightEnd - length, length);
//    					cout << "length of varchar " << getIntValueAtOffset(data, 1) << endl;
//    					int vLen = getIntValueAtOffset(data, 1);
//    					void *str;
//    					str = malloc(vLen);
//    					memcpy((char *) str, (char *) data + 1 + 4, vLen);
//
//    					cout << "VALUE: " << (char *) str << endl;
                free(pageRecord);
                return 0;

            }
            num++;
        } else {
            if(recordDescriptor[i].name == attributeName){
                if(debug) cout << "Its null bro" << endl;
//					length = nullFieldsIndicatorActualSize;
                char c = 128;
                length = 1;
                memcpy((char *) data, &c, length);
                free(pageRecord);
                return 0;
            }
        }
        i++;
    }
    free(pageRecord);
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    int pageNum = rid.pageNum;
    int slotNum = rid.slotNum;
    void* pageRecord = malloc(PAGE_SIZE);
    if(fileHandle.readPage(pageNum, pageRecord) == -1){
        free(pageRecord);
        return -1;
    }
    int totalSlots = getIntValueAtOffset(pageRecord, PAGE_SIZE - 8);
    if(slotNum <= 0 || slotNum > totalSlots) {
        free(pageRecord);
        return -1;
    }
    if(debug) cout << "Access Page Num: " << pageNum << endl;
    if(debug) cout << "Access Slot Num: " << slotNum << endl;

    int slotDirectoryOffset = PAGE_SIZE - (slotNum*12 + 8);
    int slotOffset = getIntValueAtOffset(pageRecord, slotDirectoryOffset);
    int slotSizeDirectoryOffset = slotDirectoryOffset + 4;
    int slotSize = getIntValueAtOffset(pageRecord, slotSizeDirectoryOffset);
    if(debug) cout << "Slot Directory Offset: " << slotDirectoryOffset << endl;
    if(debug) cout << "Slot Offset: " << slotOffset << endl;
    if(debug) cout << "Slot Size Directory Offset: " << slotSizeDirectoryOffset << endl;
    if(debug) cout << "Slot Size: " << slotSize << endl;
    int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);

    int count = 0;
    vector<int> bitVector;
    int rd = recordDescriptor.size();
    for(int i = 0; i < nullFieldsIndicatorActualSize && rd > 0 ; i++){
        std::bitset<8> x(*((char *)pageRecord + slotOffset + i));
        for(int j = x.size() - 1; j >= 0 && rd > 0; j--) {
            int bitValue = x[j];
            if(bitValue == 0){
                count++;
            }
            bitVector.push_back(bitValue);
            rd--;
        }
    }
    if(debug) cout << "Non null fields count: " << count << endl;

    int i = 0;
    int num = 0;
    int length = 0;
    while(i < bitVector.size() && num < count){
        int bitValue = bitVector[i];
        if(bitValue == 0) {
            if(recordDescriptor[i].name == attributeName) {
                if(debug) cout << i << " pe " << "koi mil gaya" << endl;
                int OffsetTableIndex = slotOffset + nullFieldsIndicatorActualSize + 2*num;
                if(debug) cout << "OffsetTableIndex: " << OffsetTableIndex << endl;

                void *verify;
                verify = malloc(sizeof(unsigned short));
//					memcpy((char*)verify, (char*) pageRecord + slotOffset + OffsetTableIndex, 2);
                memcpy((char*)verify, (char*) pageRecord + OffsetTableIndex, 2);

                int rightEnd = *(unsigned short *) verify;
                free(verify);

                if(debug) cout << "Right End: " << rightEnd << endl;
                if(num == 0) {
                    length = rightEnd;
                } else {

                    void *verify;
                    verify = malloc(sizeof(unsigned short));
//                    memcpy((char*)verify, (char*) pageRecord + slotOffset + OffsetTableIndex - 2, 2);
                    memcpy((char*)verify, (char*) pageRecord + OffsetTableIndex - 2, 2);

                    int rightEndPrev = *(unsigned short *) verify;
                    free(verify);
                    if(debug) cout << "Right end prev: " << rightEndPrev << endl;
                    length = rightEnd - rightEndPrev;
                }
                if(debug) cout << "length" << length << endl;
//    					data = malloc(length + nullFieldsIndicatorActualSize);

                char c = 0;
//    					memcpy((char *) data, &c, 1);
                *(char*)data = c;
//    					memcpy((char *) data, (char*)pageRecord + slotOffset, nullFieldsIndicatorActualSize);
                if(debug) cout << "toskip: " << slotOffset  + 2*count + 1 + rightEnd - length << endl;
                memcpy((char *) data + 1, (char*)pageRecord + slotOffset + 1 + 2*count + rightEnd - length, length);
//    					cout << "length of varchar " << getIntValueAtOffset(data, 1) << endl;
//    					int vLen = getIntValueAtOffset(data, 1);
//    					void *str;
//    					str = malloc(vLen);
//    					memcpy((char *) str, (char *) data + 1 + 4, vLen);
//
//    					cout << "VALUE: " << (char *) str << endl;
                free(pageRecord);
                return 0;

            }
            num++;
        } else {
            if(recordDescriptor[i].name == attributeName){
                if(debug) cout << "Its null bro" << endl;
//					length = nullFieldsIndicatorActualSize;
                char c = 128;
                length = 1;
                memcpy((char *) data, &c, length);
                free(pageRecord);
                return 0;
            }
        }
        i++;
    }
    free(pageRecord);
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.currentReadRid.pageNum = 0;
    rbfm_ScanIterator.currentReadRid.slotNum = 0;
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.bufferPage = calloc(PAGE_SIZE, 1);
    fileHandle.readPage(0, rbfm_ScanIterator.bufferPage);
    return 0;
}

RC RecordBasedFileManager::getDirectorySlotForRid(FileHandle* fileHandle, RID rid, void* directorySlot) {
    void* pageData = malloc(PAGE_SIZE);
    fileHandle->readPage(rid.pageNum, pageData);
    memcpy((char*)directorySlot, ((char*)pageData + (PAGE_SIZE) - 8 - 12*(rid.slotNum)), 12);
    free(pageData);
    return 0;
}

RC RecordBasedFileManager::getDirectorySlotForRid(void* pageData, RID rid, void* directorySlot) {
//    void* pageData = malloc(PAGE_SIZE);
//    fileHandle->readPage(rid.pageNum, pageData);
    memcpy((char*)directorySlot, ((char*)pageData + (PAGE_SIZE) - 8 - 12*(rid.slotNum)), 12);
//    free(pageData);
    return 0;
}

RC RBFM_ScanIterator::incrementSlot(RID &currentRid) {
    currentReadRid.slotNum += 1;
    RecordBasedFileManager* recordBasedFileManager = RecordBasedFileManager::instance();
    int totalNumOfSlotsOnPage = recordBasedFileManager->getTotalSlotsOnPage(bufferPage);
    if(currentReadRid.slotNum > totalNumOfSlotsOnPage) {
        currentReadRid.pageNum += 1;
        currentReadRid.slotNum = 1;
        if(currentReadRid.pageNum >= fileHandle->getPersistedAppendCounter()) {
            return RBFM_EOF;
        }
        fileHandle -> readPage(currentReadRid.pageNum, bufferPage);
    }
    return 0;
}

Attribute RBFM_ScanIterator::getAttributeWithName(const string attributeName, vector<Attribute>& attributeVector) {
    for(vector<Attribute>::iterator it = attributeVector.begin(); it != attributeVector.end(); ++it) {
        Attribute a = *it;
        if(a.name == attributeName) {
            return a;
        }
    }
}

//RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
//
//}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    while(true) {
//        cout << "In get next record, rid: " << rid.pageNum << "," << rid.slotNum << endl;
        if(currentReadRid.slotNum == 0) {
            currentReadRid.pageNum = 0;
            currentReadRid.slotNum = 1;
        }
        if(currentReadRid.pageNum >= fileHandle->getPersistedAppendCounter()) {
            return RBFM_EOF;
        }

//        cout << "currentReadRid, rid: " << currentReadRid.pageNum << "," << currentReadRid.slotNum << endl;


        RecordBasedFileManager* recordBasedFileManager = RecordBasedFileManager::instance();
        void* directorySlot = malloc(12* sizeof(char));
        recordBasedFileManager->getDirectorySlotForRid(bufferPage, currentReadRid, directorySlot);
        int offset = *(int*)directorySlot;
        int len = *(int*)((char*)directorySlot + 4);
        int slotId = *(int*)((char*)directorySlot + 8);
        free(directorySlot);

        if(slotId < 0 || (len == 0)) {
            int result = incrementSlot(currentReadRid);
            if(result == RBFM_EOF)
                return RBFM_EOF;
            continue;
        }

        void* attributeData = calloc(200, 1); //TODO check this value
        int result = recordBasedFileManager->readAttributeFromBuffer(*fileHandle, recordDescriptor, currentReadRid, conditionAttribute, attributeData, bufferPage);
        if(compOp == NO_OP) {
            int offset = 0;
            offset += ceil((double)attributeNames.size()/CHAR_BIT);
            for(vector<string>::iterator it = attributeNames.begin(); it != attributeNames.end(); ++it) {
                Attribute a = getAttributeWithName(*it, recordDescriptor);
                void* recordAttribute = calloc(200, 1); //TODO check 200
                int result1 = recordBasedFileManager->readAttributeFromBuffer(*fileHandle, recordDescriptor, currentReadRid, *it, recordAttribute, bufferPage);
                char attributeNullDescriptor = *(char*)recordAttribute;
                int iteratorPosition = it - attributeNames.begin();
                if(attributeNullDescriptor > 0) {
                    char nullDescByte = *((char*)data + ((iteratorPosition+1)/ sizeof(char)));
                    nullDescByte |= (1 << (8-((iteratorPosition+1)% sizeof(char))));
                    memcpy((char*)data + ((iteratorPosition+1)/ sizeof(char)), &nullDescByte, sizeof(char));
                } else {
                    switch(a.type) {
                        case TypeInt:
//                            cout<<"The value being printed is: " << *(int*)((char*)recordAttribute + 1)<<endl;
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(int)); //TODO fix this
                            offset += sizeof(int);
                            break;
                        case TypeReal:
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(float));
                            offset += sizeof(float);
                            break;
                        case TypeVarChar:
                            int recordLen = *(int*)((char*)recordAttribute + 1);
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(int) + recordLen);
                            offset += (sizeof(int) + recordLen);
                            break;
                    }
                }
                free(recordAttribute);
            }
            rid = currentReadRid;
            incrementSlot(currentReadRid);
            return 0;
        }
//        if(result != 0) {
//            if(compOp == NO_OP) {
//                    rid = currentReadRid;
//                    incrementSlot(currentReadRid);
//                    return 0;
//            }
//        }

        Attribute attribute;
        for(int i=0; i<recordDescriptor.size(); i++) {
            if(recordDescriptor[i].name == conditionAttribute.c_str()) {
                attribute = recordDescriptor[i];
                break;
            }
        }

        bool comparisonResult = false;

        switch(attribute.type) {
            case TypeInt:
                switch (compOp) {
                    case EQ_OP: //TODO handle null case
                        if(*(int*)((char*)attributeData + 1) == *(int*)value)
                            comparisonResult = true;
                        break;
                    case LT_OP:
                        if(*(int*)((char*)attributeData + 1) < *(int*)value)
                            comparisonResult = true;
                        break;
                    case LE_OP:
                        if(*(int*)((char*)attributeData + 1) <= *(int*)value)
                            comparisonResult = true;
                        break;
                    case GT_OP:
                        if(*(int*)((char*)attributeData + 1) > *(int*)value)
                            comparisonResult = true;
                        break;
                    case GE_OP:
                        if(*(int*)((char*)attributeData + 1) >= *(int*)value)
                            comparisonResult = true;
                        break;
                    case NE_OP:
                        if(*(int*)((char*)attributeData + 1) != *(int*)value)
                            comparisonResult = true;
                        break;
                    case NO_OP:
                        comparisonResult = true;
                        break;
                    default:
                        comparisonResult = false;
                        break;
                }
                break;
            case TypeReal:
                switch (compOp) {
                    case EQ_OP:
                        if(*(float*)((char*)attributeData + 1) == *(float*)value)
                            comparisonResult = true;
                        break;
                    case LT_OP:
                        if(*(float*)((char*)attributeData + 1) < *(float*)value)
                            comparisonResult = true;
                        break;
                    case LE_OP:
                        if(*(float*)((char*)attributeData + 1) <= *(float*)value)
                            comparisonResult = true;
                        break;
                    case GT_OP:
                        if(*(float*)((char*)attributeData + 1) > *(float*)value)
                            comparisonResult = true;
                        break;
                    case GE_OP:
                        if(*(float*)((char*)attributeData + 1) >= *(float*)value)
                            comparisonResult = true;
                        break;
                    case NE_OP:
                        if(*(float*)((char*)attributeData + 1) != *(float*)value)
                            comparisonResult = true;
                        break;
                    case NO_OP:
                        comparisonResult = true;
                        break;
                    default:
                        comparisonResult = false;
                        break;
                }
                break;
            case TypeVarChar:
                int lengthAttribute = *(int*)((char*)attributeData+1);
                int lengthValue = *(int*)value;
                int result = strncmp((char*)attributeData + 1 + 4, (char*) value + 4, max(lengthAttribute, lengthValue));
                switch (compOp) {
                    case EQ_OP:
                        if(result == 0)
                            comparisonResult = true;
                        break;
                    case LT_OP:
                        if(result < 0)
                            comparisonResult = true;
                        break;
                    case LE_OP:
                        if(result <= 0)
                            comparisonResult = true;
                        break;
                    case GT_OP:
                        if(result > 0)
                            comparisonResult = true;
                        break;
                    case GE_OP:
                        if(result >= 0)
                            comparisonResult = true;
                        break;
                    case NE_OP:
                        if(result != 0)
                            comparisonResult = true;
                        break;
                    case NO_OP:
                        comparisonResult = true;
                        break;
                    default:
                        comparisonResult = false;
                        break;
                }
                break;
        }

        free(attributeData);

        if(comparisonResult) {
            int offset = 0;
            offset += ceil((double)attributeNames.size()/CHAR_BIT);
            for(vector<string>::iterator it = attributeNames.begin(); it != attributeNames.end(); ++it) {
                Attribute a = getAttributeWithName(*it, recordDescriptor);
                void* recordAttribute = calloc(200, 1); //TODO check 200
                recordBasedFileManager->readAttributeFromBuffer(*fileHandle, recordDescriptor, currentReadRid, *it, recordAttribute, bufferPage);
                char attributeNullDescriptor = *(char*)recordAttribute;
                int iteratorPosition = it - attributeNames.begin();
                if(attributeNullDescriptor > 0) {
                    char nullDescByte = *((char*)data + ((iteratorPosition+1)/ sizeof(char)));
                    nullDescByte |= (1 << (8-((iteratorPosition+1)% sizeof(char))));
                    memcpy((char*)data + ((iteratorPosition+1)/ sizeof(char)), &nullDescByte, sizeof(char));
                } else {
                    switch(a.type) {
                        case TypeInt:
//                            cout<<"The value being printed is: " << *(int*)((char*)recordAttribute + 1)<<endl;
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(int)); //TODO fix this
                            offset += sizeof(int);
                            break;
                        case TypeReal:
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(float));
                            offset += sizeof(float);
                            break;
                        case TypeVarChar:
                            int recordLen = *(int*)((char*)recordAttribute + 1);
                            memcpy((char*)data + offset, (char*)recordAttribute + 1, sizeof(int) + recordLen);
                            offset += (sizeof(int) + recordLen);
                            break;
                    }
                }
                free(recordAttribute);
            }
            rid = currentReadRid;
            incrementSlot(currentReadRid);
            return 0;
        } else {
            if(incrementSlot(currentReadRid) == RBFM_EOF) {
                return RBFM_EOF;
            }
        }
    }
    return 0;
}
