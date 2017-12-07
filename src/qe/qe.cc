
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

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	this->leftInIter = leftIn;
	this->rightInIter = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	this->leftAttributes.clear();
	this->rightAttributes.clear();
	this->joinAttributes.clear();

	leftIn->getAttributes(this->leftAttributes);
	rightIn->getAttributes(this->rightAttributes);


}






