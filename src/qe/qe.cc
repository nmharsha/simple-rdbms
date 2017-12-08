
#include "qe.h"
#include <set>

Filter::Filter(Iterator* input, const Condition &condition) {
	filterInput = input;
	filterCondition = condition;
	currPos = 0;
	filterAttrs.clear();
//	cout << "Here" << endl;
	filterInput->getAttributes(filterAttrs);

	//Find the place in the descriptor
	if(filterAttrs.size() != 0) {
		for(int i = 0; i < filterAttrs.size(); i++) {
//			cout << "at i: " << i << endl;
//			cout << filterAttrs[i].name << endl;
//			cout << filterCondition.lhsAttr << endl;
			if(filterAttrs[i].name.compare(filterCondition.lhsAttr) == 0) {
				currPos = i;
				break;
			}
		}
	}
//	cout << "Constructor called" << endl;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = filterAttrs;
}

RC Filter::getNextTuple(void *data){
	// just call the getNextTuple of Iterator class until you find something
	int status = 0;
	void* returnedData = calloc(PAGE_SIZE, 1);

	do{
//		cout << "Getting the record" << endl;
		status = filterInput->getNextTuple(data);
//		cout << "Read status: " << status << endl;
		if(status != 0) {
//			cout << "Read status inv: " << status << endl;

			return QE_EOF;
		}

		getAttributeValue(data, returnedData, filterAttrs, currPos, filterCondition.rhsValue.type);
	} while(!check(returnedData));

	free(returnedData);
	return 0;
}

bool Filter::check(void *data) {

	if(filterCondition.rhsValue.type == TypeInt) {
//		cout << "Its int in check" << endl;
		int compVal = *(int *) filterCondition.rhsValue.data;
		int orig = *(int *) data;
		switch(filterCondition.op) {
			case EQ_OP: {
				return orig == compVal;
			}
			case LT_OP: {
				return orig < compVal;
			}
			case LE_OP: {
				return orig <= compVal;
			}
			case GT_OP: {
				return orig > compVal;
			}
			case GE_OP: {
				return orig >= compVal;
			}
			case NE_OP: {
				return orig != compVal;
			}
			case NO_OP: {
				return true;
			}
		}
	} else if(filterCondition.rhsValue.type == TypeReal) {
		float compVal = *(float *) filterCondition.rhsValue.data;
		float orig = *(float *) data;
		switch(filterCondition.op) {
			case EQ_OP: {
				return orig == compVal;
			}
			case LT_OP: {
				return orig < compVal;
			}
			case LE_OP: {
				return orig <= compVal;
			}
			case GT_OP: {
				return orig > compVal;
			}
			case GE_OP: {
				return orig >= compVal;
			}
			case NE_OP: {
				return orig != compVal;
			}
			case NO_OP: {
				return true;
			}
		}
	} else {
		int compValLen = *(int *) filterCondition.rhsValue.data;
		int origLen = *(int *) data;
		void* c = calloc(compValLen, 1);
		void* o = calloc(origLen, 1);
		memcpy((char*) c, (char*) filterCondition.rhsValue.data + sizeof(int), compValLen);
		memcpy((char*) o, (char*) data + sizeof(int), origLen);
		string compVal((char*)c);
		string orig((char*)o);

		switch(filterCondition.op) {
			case EQ_OP: {
				return (orig.compare(compVal) == 0);
			}
			case LT_OP: {
				return (orig.compare(compVal) < 0);
			}
			case LE_OP: {
				return (orig.compare(compVal) <= 0);
			}
			case GT_OP: {
				return (orig.compare(compVal) > 0);
			}
			case GE_OP: {
				return (orig.compare(compVal) >= 0);
			}
			case NE_OP: {
				return (orig.compare(compVal) != 0);
			}
			case NO_OP: {
				return true;
			}
		}
	}
	return 0;
}

// gets the attribute value from the data - in the form of returnedData
void Filter::getAttributeValue(void* data, void* returnedData, vector<Attribute> attrs, unsigned int currPos, AttrType type) {
//	cout << "Enterneig" << endl;
	int offset = 0;
    int nullFieldsIndicatorActualSize = ceil((double) attrs.size() / CHAR_BIT);
    offset += nullFieldsIndicatorActualSize;

    vector<unsigned int> bitVector;
    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVector.push_back(bitValue);
        }
    }
    //get to the current attribute
    int i = 0;
//    int j = 0;
//    cout << "Curr pos: " << currPos << endl;
	while(i < currPos){
		if(bitVector[i] == 0){
			if(attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
				offset += sizeof(int);
			} else {
				int len = *(int *)((char *) data + offset);
				offset += sizeof(int);
				offset += len;
			}
		}
		i++;
	}
//	cout << "I is at: " << i << endl;

//	while(i < currPos && j < attrs.size()){
//		if(bitVector[j] == 0){
//			if(attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
//				offset += sizeof(int);
//			} else {
//				int len = *(int *)((char *) data + offset);
//				offset += sizeof(int);
//				offset += len;
//			}
//			i++;
//		}
//		j++;
//	}
	// hopefully i stops before j does.
	// just for debug purposes - check if i terminated first, then legal

	int size = 0;
	if(type == TypeInt || type == TypeReal) {
//		cout << "Int or float" << endl;
		size = sizeof(int);
	} else {
//		cout << "Varchar" << endl;

		int len = *(int *)((char *) data + offset);
		size = sizeof(int);
		size += len;
	}
	memcpy(returnedData, (char *)data + offset, size);
	if(type == TypeInt) {
//		cout << "Returning: " << *(int *)returnedData << endl;
	} else if(type == TypeReal) {
//		cout << "Returning: " << *(float *)returnedData << endl;
	} else {
//		cout << "Returning: " << (char *)returnedData + 4 << endl;
	}


}

Project::Project(Iterator *input, const vector<string> &attrNames){
	projectInput = input;
	attrs.clear();
	proj.clear();
	projectInput->getAttributes(attrs);

	for(int i = 0; i < attrNames.size(); i++) {
		for(int j = 0; j < attrs.size(); j++) {
//			cout << "at I, J: " << i << ", " << j << endl;
//			cout << attrs[j].name << endl;
//			cout << attrNames[i] << endl;
			if(attrNames[i].compare(attrs[j].name)) {
				proj.push_back(attrs[j]);
			}
		}
	}
}

RC Project::getNextTuple(void *data) {
	int status = 0;
	void *record = calloc(PAGE_SIZE, 1);
//	cout << "Before getting tuple" << endl;
	status = projectInput->getNextTuple(record);
//	cout << "After getting tuple, status: " << status << endl;
	if(status != 0){
		return QE_EOF;
	}
//	cout << "before getting attsssss" << endl;
	getAttributeValuesForProject(record, data);
//	cout << "Here again " << endl;
	free(record);
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
}

//gets the attribute values for Projection from the data - in the form of returnedData
void Project::getAttributeValuesForProject(void* data, void* returnedData){

	map<string, void*> projAttrs;
	map<string, int> attrNullIndicator;
	for(int a = 0; a < proj.size(); a++) {
		void* v = calloc(PAGE_SIZE, 1);
		projAttrs.insert ( std::pair<string,void *>(proj[a].name, v) );
//		free(v);
		attrNullIndicator.insert ( std::pair<string,int>(proj[a].name, 1));
	}

	int pSize = 0;
	int offset = 0;
    int nullFieldsIndicatorActualSize = ceil((double) attrs.size() / CHAR_BIT);

    vector<unsigned int> bitVector;
    for(int i = 0; i < nullFieldsIndicatorActualSize; i++){
        std::bitset<8> x(*((char *)data + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVector.push_back(bitValue);
        }
    }
    int total = projAttrs.size();
    offset += nullFieldsIndicatorActualSize;
    vector<int> newBits;

    int i = 0;
    while(i < attrs.size() && total > 0) {
        Attribute currAtt = attrs[i];
        string name = currAtt.name;
        string type;

        bool foundInMap = false;
        auto search = projAttrs.find(name);
        if(search != projAttrs.end()) {
        		foundInMap = true;
        }

        if(bitVector[i] == 0 && foundInMap){
        		attrNullIndicator[name] = 0;
        		total--;
            if(currAtt.type == TypeInt) {
//            	cout << "In INT" << endl;
                void *entryPtr;
                entryPtr = calloc(sizeof(int), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(int));
//                projAttrs[currAtt] = entryPtr;
                memcpy((char*)projAttrs[name], entryPtr, sizeof(int));
                offset += sizeof(int);
//                cout << *(int *) entryPtr;
                free(entryPtr);
                pSize += sizeof(int);
            }
            else if(currAtt.type == TypeReal) {
//            	cout << "In REALLLLLL" << endl;

                void *entryPtr;
                entryPtr = calloc(sizeof(float), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(float));
                //projAttrs[currAtt] = entryPtr;
                memcpy((char*)projAttrs[name], entryPtr, sizeof(float));
                offset += sizeof(float);
//                cout << *(float *) entryPtr;
                free(entryPtr);
                pSize += sizeof(float);
            }
            else if(currAtt.type == TypeVarChar){
                int size = 0;
                int init = offset;
            		void *entryPtr;
                entryPtr = calloc(sizeof(int), 1);
                memcpy( entryPtr , (char*)data+offset, sizeof(int));

                offset += sizeof(int); // increment by 4 bytes for length of string
                size += sizeof(int);

                int len = *(int*)entryPtr;
                size += len;
                offset += len;

                void* attrPtr = calloc(size, 1);
                memcpy( attrPtr , (char*)data+init, size);
                //projAttrs[currAtt] = entryPtr;
                memcpy((char*)projAttrs[name], attrPtr, size);
                free(entryPtr);
                free(attrPtr);
                pSize += size;
            }

        } else if(bitVector[i] == 1 && foundInMap){
        		attrNullIndicator[name] = 1;
        		total--;
        }
        i++;
    }
    vector<unsigned int>().swap(bitVector);

    int newOffset = 0;
    int nulls = ceil((double) proj.size() / CHAR_BIT);
    newOffset += nulls;

    int attrCount = 0;
    	while(attrCount < proj.size()) {
    		Attribute currAttr = proj[attrCount];
    		int value = attrNullIndicator.find(currAttr.name)->second;
    		if(value == 1) {
    			char bite = *((char*)returnedData + attrCount/8);
    			bite |= 1<<(8-attrCount%8-1);
    			memcpy((char*) returnedData + attrCount/8, &bite, 1);
//    			*((char*)returnedData + attrCount/8) = bite;
    		}
    		attrCount++;
    	}

    for(int k = 0; k < proj.size(); k++) {
    		Attribute curr = proj[k];
    		// set the bit values here...

    		/////////////////////////////
    		if((curr.type == TypeInt || curr.type == TypeReal) && attrNullIndicator.find(curr.name)->second == 0) {
    			memcpy((char*) returnedData + newOffset, (char*) projAttrs.find(curr.name)->second, sizeof(int));
    			newOffset += sizeof(int);
    		} else if(curr.type == TypeVarChar && attrNullIndicator.find(curr.name)->second == 0) {
    			int len = *(int *)projAttrs.find(curr.name)->second;
    			memcpy((char*) returnedData + newOffset, (char*) projAttrs.find(curr.name)->second, sizeof(int) + len);
    			newOffset += (sizeof(int) + len);
    		}
    }

    projAttrs.clear();
    attrNullIndicator.clear();
}

int BNLJoin::getTupleSize(void* tuple) {
    int numOfNullBytes = (int)ceil((double)this->leftAttributes.size()/8);
    cout << "Num of null bytes is: " << numOfNullBytes << endl;
	int offset = numOfNullBytes;
	int size = numOfNullBytes;
	for(int i=0;i<this->leftAttributes.size();i++) {

		char nullByte = *((char*)tuple + i/8);
		bool nullBitSet = (nullByte & (1<<((8-i%8-1)))) > 0;

		if(!nullBitSet) {
			if(this->leftAttributes[i].type == TypeInt || this->leftAttributes[i].type == TypeReal) {
				size += sizeof(int);
				offset += sizeof(int);
			} else {
				int varcharLen = *(int*)((char*)tuple + offset);
				size += sizeof(int) + varcharLen;
				offset += sizeof(int) + varcharLen;
			}
		}
	}

	return size;
}

void getAttributeFromRecord(const void* data, void* attributeData, string attributeName, vector<Attribute> attributes) {
	int offset = 0;
    offset += 1;
	for(int i=0;i < attributes.size(); i++) {
		int numOfNullBytes = (int)ceil((double)attributes.size()/8);
		char nullByte = *((char*)data + i/8);
		bool nullBitSet = (nullByte & (1<<((8-i%8-1)))) > 0;
		if(attributes[i].name == attributeName) {
            if(nullBitSet)
                cout << "Null bit set. This shouldn't happen in an index: " << nullBitSet << " and null byte is " << nullByte << " and attribute is " << attributes[i].name << endl;
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

RC BNLJoin::fillBuffer() {
	this->leftOffset = 0;
	RC result;
	void* attributeData = calloc(PAGE_SIZE, 1);
	while(this->leftOffset <= this->numPages*PAGE_SIZE) {
		result = this->leftInIter->getNextTuple((char*)this->leftBlockBuffer + this->leftOffset);
		if(result == QE_EOF) {
			cout << "The end\n";
			return -1;
		}

		getAttributeFromRecord((char*)this->leftBlockBuffer + this->leftOffset, attributeData, this->condition.lhsAttr, this->leftAttributes);

		switch(this->joinAttribute.type) {
			case TypeInt:
				cout << "Inserting into map: " << *(int*)((char*)attributeData + 1) << endl;
				this->intMap.insert(std::pair<int, int>(*(int*)((char*)attributeData + 1), this->leftOffset));
				break;
			case TypeReal:
				this->intMap.insert(std::pair<float, int>(*(float*)((char*)attributeData + 1), this->leftOffset));
				break;
			case TypeVarChar:
				int keySize = *(int*)((char*)attributeData + 1);
				char* charKey = (char*)calloc(keySize, 1);
				memcpy(charKey, (char*)attributeData + 1 + sizeof(int), keySize);
				string key = string(charKey, keySize);
				free(charKey);
				this-> stringMap.insert(std::pair<string, int>(key, this->leftOffset));
				break;
		}

		this->leftOffset += getTupleSize((char*)this->leftBlockBuffer + this->leftOffset);
		memset(attributeData, 0, PAGE_SIZE);
	}
	free(attributeData);
	return 0;
}

int mergeRecords2(void* left, void* right, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs, void* returnedData){
    int nullsLeft = ceil((double) leftAttrs.size() / CHAR_BIT);
    int nullsRight = ceil((double) rightAttrs.size() / CHAR_BIT);
    int nullsTotal = ceil((double)(leftAttrs.size() + rightAttrs.size())/CHAR_BIT);

    int leftOffset = 0;
    leftOffset += nullsLeft;
    int rightOffset = 0;
    rightOffset += nullsRight;

    vector<unsigned int> bitVectorLeft;
    for(int i = 0; i < nullsLeft; i++){
        std::bitset<8> x(*((char *)left + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVectorLeft.push_back(bitValue);
        }
    }
    vector<unsigned int> bitVectorRight;
    for(int i = 0; i < nullsRight; i++){
        std::bitset<8> x(*((char *)right + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVectorRight.push_back(bitValue);
        }
    }

    int i = 0;
    while(i < leftAttrs.size()){
        if(bitVectorLeft[i] == 0){
            if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal) {
                leftOffset += sizeof(int);
            } else {
                int len = *(int *)((char *) left + leftOffset);
                leftOffset += sizeof(int);
                leftOffset += len;
            }
        }
        i++;
    }
    i = 0;
    while(i < rightAttrs.size()){
        if(bitVectorRight[i] == 0){
            if(rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal) {
                rightOffset += sizeof(int);
            } else {
                int len = *(int *)((char *) left + rightOffset);
                rightOffset += sizeof(int);
                rightOffset += len;
            }
        }
        i++;
    }

    int offset = 0;
//    void* returnedData = calloc(, 1);

    // set the null bits here
    int numLeft = 0;
    int numRight = 0;
    int numTotal = 0;
    while(numLeft < bitVectorLeft.size()) {
        if(bitVectorLeft[numLeft] == 1) {
            char bite = *((char*)returnedData + numTotal/8);
            bite |= 1<<(8-numTotal%8-1);
            memcpy((char*) returnedData + numTotal/8, &bite, 1);
        }
        numLeft++;
        numTotal++;
    }
    while(numRight < bitVectorRight.size()) {
        if(bitVectorRight[numRight] == 1) {
            char bite = *((char*)returnedData + numTotal/8);
            bite |= 1<<(8-numTotal%8-1);
            memcpy((char*) returnedData + numTotal/8, &bite, 1);
        }
        numRight++;
        numTotal++;
    }
    /////

    offset += nullsTotal;
    memcpy((char*) returnedData + offset, (char *) left + nullsLeft, leftOffset - nullsLeft);
    offset += (leftOffset - nullsLeft);
    memcpy((char*) returnedData + offset, (char *) right + nullsRight, rightOffset - nullsRight);
//	return returnedData;
    return nullsTotal + (leftOffset - nullsLeft) + (rightOffset - nullsRight);
}

RC BNLJoin::joinTables() {
	bool reachedEnd = false;
	void* rightTuple = calloc(PAGE_SIZE, 1);
	void* attributeData = calloc(PAGE_SIZE, 1);
	RC result;

	while(true) {
        this->intMap.clear();
        this->floatMap.clear();
        this->stringMap.clear();
        memset(this->leftBlockBuffer, 0, this->numPages*PAGE_SIZE);
//		break;
		result = fillBuffer();
		if(result == -1) {
            if(this->leftOffset == 0) {
                break;
            }
		}

		while(this->rightInIter->getNextTuple(rightTuple) != -1) {
			getAttributeFromRecord(rightTuple, attributeData, this->condition.rhsAttr, this->rightAttributes);
//            getAttributeFromRecord((char*)this->leftBlockBuffer + this->leftOffset, attributeData, this->joinAttribute.name, this->leftAttributes);

			switch(this->joinAttribute.type) {
				case TypeInt: {
                    int key = *(int *) ((char *) attributeData + 1);
//                    cout << "Key obtained from right is: " << key << endl;
                    multimap<int, int>::iterator it = this->intMap.lower_bound(key);
                    if (it->first != key) {
                        memset(rightTuple, 0, PAGE_SIZE);
                        memset(attributeData, 0, PAGE_SIZE);
                        continue;
                    }

                    while (it->first == key) {
//                        cout << "Insertion happening" << endl;
                        void *mergedRecord = calloc(PAGE_SIZE, 1);
                        int size = mergeRecords2((char *) this->leftBlockBuffer + it->second, rightTuple,
                                                 this->leftAttributes, this->rightAttributes, mergedRecord);
                        if (this->outputPageOffset + size >= PAGE_SIZE) {
                            //TODO flush page to disk
//                            outputFileHandle.appendPage(this->outputPage);
                            memset(this->outputPage, 0, PAGE_SIZE);
                            this->outputPageOffset = 0;
                        }
                        RID insertedRID;
                        this->rbfm->insertRecord(this->outputFileHandle, this->joinAttributes, mergedRecord,
                                                 insertedRID);
//                        this->rbfm->insertRecordInMemory(this->outputFileHandle, this->joinAttributes, mergedRecord, insertedRID, this->outputPage);
                        memcpy((char *) this->outputPage + this->outputPageOffset, mergedRecord, size);
                        free(mergedRecord);
                        this->outputPageOffset += size;
                        it++;
                    }
                    break;
                }
				case TypeReal: {
                    float keyF = *(float *) ((char *) attributeData + 1);
                    multimap<float, int>::iterator itF = this->floatMap.lower_bound(keyF);
                    if (itF->first != keyF) {
                        memset(rightTuple, 0, PAGE_SIZE);
                        memset(attributeData, 0, PAGE_SIZE);
                        continue;
                    }

                    while (itF->first == keyF) {
                        void *mergedRecord = calloc(PAGE_SIZE, 1);
                        int size = mergeRecords2((char *) this->leftBlockBuffer + itF->second, rightTuple,
                                                 this->leftAttributes, this->rightAttributes, mergedRecord);
                        if (this->outputPageOffset + size >= PAGE_SIZE) {
//                            outputFileHandle.appendPage(this->outputPage);
                            memset(this->outputPage, 0, PAGE_SIZE);
                            this->outputPageOffset = 0;
                        }
                        RID insertedRID;
                        this->rbfm->insertRecord(this->outputFileHandle, this->joinAttributes, mergedRecord,
                                                 insertedRID);
                        memcpy((char *) this->outputPage + this->outputPageOffset, mergedRecord, size);
                        free(mergedRecord);
                        this->outputPageOffset += size;
                        itF++;
                    }
                    break;
                }
				case TypeVarChar: {
                    int keySize = *(int *) ((char *) attributeData + 1);
                    char *charKey = (char *) calloc(keySize, 1);
                    memcpy(charKey, (char *) attributeData + 1 + sizeof(int), keySize);
                    string keyS = string(charKey, keySize);
                    multimap<string, int>::iterator itS = this->stringMap.lower_bound(keyS);
                    if (itS->first != keyS) {
                        memset(rightTuple, 0, PAGE_SIZE);
                        memset(attributeData, 0, PAGE_SIZE);
                        continue;
                    }
                    while (itS->first == keyS) {
                        void *mergedRecord = calloc(PAGE_SIZE, 1);
                        int size = mergeRecords2((char *) this->leftBlockBuffer + itS->second, rightTuple,
                                                 this->leftAttributes, this->rightAttributes, mergedRecord);
                        if (this->outputPageOffset + size >= PAGE_SIZE) {
//                            outputFileHandle.appendPage(this->outputPage);
                            memset(this->outputPage, 0, PAGE_SIZE);
                            this->outputPageOffset = 0;
                        }
                        RID insertedRID;
                        this->rbfm->insertRecord(this->outputFileHandle, this->joinAttributes, mergedRecord,
                                                 insertedRID);
                        memcpy((char *) this->outputPage + this->outputPageOffset, mergedRecord, size);
                        free(mergedRecord);
                        this->outputPageOffset += size;
                        itS++;
                    }
                    break;
                }
			}
		}
		this->rightInIter->setIterator();

	}

	free(rightTuple);
    free(attributeData);
}

int BNLJoin::autoIncId = 0;

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {

    this->outputFileName = condition.lhsAttr + "_" + condition.rhsAttr + "_" + to_string(autoIncId);
    cout << "Joining output file name is: " << this->outputFileName << endl;
    this->rbfm = RecordBasedFileManager::instance();
    this->rbfm->createFile(outputFileName);
    this->rbfm->openFile(outputFileName, this->outputFileHandle);

	this->leftInIter = leftIn;
	this->rightInIter = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	this->leftAttributes.clear();
	this->rightAttributes.clear();
	this->joinAttributes.clear();

	leftIn->getAttributes(this->leftAttributes);
	rightIn->getAttributes(this->rightAttributes);

    vector<string> joinAttributeNames;

	for(int i=0;i<this->leftAttributes.size();i++) {
		if(this->leftAttributes[i].name == this->condition.lhsAttr) {
			this->joinAttribute = this->leftAttributes[i];
		}
		this->joinAttributes.push_back(this->leftAttributes[i]);
        joinAttributeNames.push_back(this->leftAttributes[i].name);
	}
	for(int i=0;i<this->rightAttributes.size();i++) {
		this->joinAttributes.push_back(this->rightAttributes[i]);
        joinAttributeNames.push_back(this->rightAttributes[i].name);
	}

	this -> leftBlockBuffer = calloc(this->numPages*PAGE_SIZE, 1);
	this->outputPage = calloc(PAGE_SIZE, 1);
	this->rightInputPage = calloc(PAGE_SIZE, 1);

	this->joinTables();

    this->rbfm->scan(this->outputFileHandle, this->joinAttributes, "", NO_OP, NULL, joinAttributeNames, rbfm_scanIterator);

}

RC BNLJoin::getNextTuple(void *data) {
    RID rid;
    return this->rbfm_scanIterator.getNextRecord(rid, data);
}

BNLJoin::~BNLJoin() {
    cout << "Cleaning up!" << endl;
    free(leftBlockBuffer);
    free(outputPage);
    rbfm_scanIterator.close();
    this->rbfm->closeFile(this->outputFileHandle);
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    attrs = this->joinAttributes;
}


void* mergeRecords(void* left, void* right, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs){
    int nullsLeft = ceil((double) leftAttrs.size() / CHAR_BIT);
    int nullsRight = ceil((double) rightAttrs.size() / CHAR_BIT);
    int nullsTotal = leftAttrs.size() + rightAttrs.size();

	int leftOffset = 0;
	leftOffset += nullsLeft;
	int rightOffset = 0;
	rightOffset += nullsRight;

    vector<unsigned int> bitVectorLeft;
    for(int i = 0; i < nullsLeft; i++){
        std::bitset<8> x(*((char *)left + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVectorLeft.push_back(bitValue);
        }
    }
    vector<unsigned int> bitVectorRight;
    for(int i = 0; i < nullsRight; i++){
        std::bitset<8> x(*((char *)right + i));
        for(int j = x.size() - 1; j >= 0; j--) {
            unsigned int bitValue = x[j];
            bitVectorRight.push_back(bitValue);
        }
    }

    int i = 0;
	while(i < leftAttrs.size()){
		if(bitVectorLeft[i] == 0){
			if(leftAttrs[i].type == TypeInt || leftAttrs[i].type == TypeReal) {
				leftOffset += sizeof(int);
			} else {
				int len = *(int *)((char *) left + leftOffset);
				leftOffset += sizeof(int);
				leftOffset += len;
			}
		}
		i++;
	}
	i = 0;
	while(i < rightAttrs.size()){
		if(bitVectorRight[i] == 0){
			if(rightAttrs[i].type == TypeInt || rightAttrs[i].type == TypeReal) {
				rightOffset += sizeof(int);
			} else {
				int len = *(int *)((char *) left + rightOffset);
				rightOffset += sizeof(int);
				rightOffset += len;
			}
		}
		i++;
	}

	int offset = 0;
	void* returnedData = calloc(nullsTotal + leftOffset + rightOffset, 1);

	// set the null bits here
    int numLeft = 0;
    int numRight = 0;
    int numTotal = 0;
    	while(numLeft < bitVectorLeft.size()) {
    		if(bitVectorLeft[numLeft] == 1) {
    			char bite = *((char*)returnedData + numTotal/8);
    			bite |= 1<<(8-numTotal%8-1);
    			memcpy((char*) returnedData + numTotal/8, &bite, 1);
    		}
    		numLeft++;
    		numTotal++;
    	}
    	while(numRight < bitVectorRight.size()) {
		if(bitVectorRight[numRight] == 1) {
			char bite = *((char*)returnedData + numTotal/8);
			bite |= 1<<(8-numTotal%8-1);
			memcpy((char*) returnedData + numTotal/8, &bite, 1);
		}
		numRight++;
		numTotal++;
    	}
    	/////

	offset += nullsTotal;
	memcpy((char*) returnedData + offset, (char *) left + nullsLeft, leftOffset);
	offset += leftOffset;
	memcpy((char*) returnedData + offset, (char *) right + nullsRight, rightOffset);
	return returnedData;

}

//RC INLJoin::getNextTuple(void *data) {
//	return QE_EOF;
//}





