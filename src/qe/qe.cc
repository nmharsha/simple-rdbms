
#include "qe.h"
#include <set>


Filter::Filter(Iterator* input, const Condition &condition) {
	filterInput = input;
	filterCondition = condition;
	currPos = 0;
	filterAttrs.clear();
	filterInput->getAttributes(filterAttrs);

	//Find the place in the descriptor
	if(filterAttrs.size() != 0) {
		for(int i = 0; i < filterAttrs.size(); i++) {
			if(filterAttrs[i].name.compare(filterCondition->lhsAttr)) {
				currPos = i;
				break;
			}
		}
	}
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = filterAttrs;
}


RC Filter::getNextTuple(void *data){
	// just call the getNextTuple of Iterator class until you find something
	int status = 0;


	return 0;
}

RC Filter::check(void *data) {

//	if(filterCondition->rhsValue == NULL) {
//		return true;
//	}

//	void* valueToCheck = malloc(filterCondition[currPos])

	if(filterCondition->rhsValue.type == TypeInt) {
//		int *
	} else 	if(filterCondition->rhsValue.type == TypeReal) {

	}

	return 0;
}

// gets the attribute value from the data - in the form of returnedData
void getAttributeValue(void* data, void* returnedData, vector<Attribute> attrs, int currPos, Attribute type) {
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
    int j = 0;
	while(i < currPos && j < attrs.size()){
		if(bitVector[j] == 0){
			if(attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
				offset += sizeof(int);
			} else {
				int len = *(int *)((char *) data + offset);
				offset += sizeof(int);
				offset += len;
			}
			i++;
		}
		j++;
	}
	// hopefully i stops before j does.
	// just for debug purposes - check if i terminated first, then legal

	int size = 0;
	if(type == TypeInt || type == TypeReal) {
		size = sizeof(int);
	} else {
		int len = *(int *)((char *) data + offset);
		size = sizeof(int);
		size += len;
	}
	memcpy(returnedData, (char *)data + offset, size);
}

// gets the attribute values for Projection from the data - in the form of returnedData
void getAttributeValuesForProject(void* data, void* returnedData, vector<Attribute> proj,vector<Attribute> attrs){

	map<Attribute, void*> projAttrs;
	map<Attribute, int> attrNullIndicator;
	for(int a = 0; a < proj.size(); a++) {
		void* v = calloc(PAGE_SIZE, 1);
		projAttrs.insert ( std::pair<Attribute,void *>(proj[a], v) );
		free(v);
		attrNullIndicator.insert ( std::pair<Attribute,int>(proj[a], 1));
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
        auto search = projAttrs.find(currAtt);
        if(search != projAttrs.end()) {
        		foundInMap = true;
        }

        if(bitVector[i] == 0 && foundInMap){
        		attrNullIndicator[currAtt] = 0;
        		total--;
            if(currAtt.type == TypeInt) {
                void *entryPtr;
                entryPtr = calloc(sizeof(int), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(int));
//                projAttrs[currAtt] = entryPtr;
                memcpy((char*)projAttrs[currAtt], entryPtr, sizeof(int));
                offset += sizeof(int);
                cout << *(int *) entryPtr;
                free(entryPtr);
                pSize += sizeof(int);
            }
            else if(currAtt.type == TypeReal) {
                void *entryPtr;
                entryPtr = calloc(sizeof(float), 1);
                memcpy(entryPtr, (char*) data + offset, sizeof(float));
                //projAttrs[currAtt] = entryPtr;
                memcpy((char*)projAttrs[currAtt], entryPtr, sizeof(int));
                offset += sizeof(float);
                cout << *(float *) entryPtr;
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
                memcpy((char*)projAttrs[currAtt], attrPtr, size);
                free(entryPtr);
                free(attrPtr);
                pSize += size;
            }

        } else if(bitVector[i] == 1 && foundInMap){
        		attrNullIndicator[currAtt] = 1;
        		total--;
        }
        i++;
    }
    vector<unsigned int>().swap(bitVector);

    int newOffset = 0;
    int nulls = ceil((double) proj.size() / CHAR_BIT);
    newOffset += nulls;

    int attrCount = 0;
    for(int k = 0; k < nulls; k++) {
    		while()
    }


    //	byte nullDescriptor = 0x00;
//    for(int k = 0; k < proj.size(); k++) {
//		Attribute curr = proj[k];
//		if(k <)
//    }

    for(int k = 0; k < proj.size(); k++) {
    		Attribute curr = proj[k];
    		// set the bit values here...

    		/////////////////////////////
    		if(curr.type == TypeInt || curr.type == TypeReal) {
    			memcpy((char*) returnedData + newOffset, (char*) projAttrs.find(curr), sizeof(int));
    			newOffset += sizeof(int);
    		} else {
    			int len = *(int *)projAttrs.find(curr);
    			memcpy((char*) returnedData + newOffset, (char*) projAttrs.find(curr), sizeof(int) + len);
    			newOffset += (sizeof(int) + len);
    		}
    }

}






