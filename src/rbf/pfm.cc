//Author: Rahil Shah
#include "pfm.h"
//All methods in the PF and RBF components, except constructors and destructors, return integer codes.
//A return code of 0 indicates normal completion.
//A nonzero return code indicates that either an exception condition or an error has occurred.


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}

// passes test1
RC PagedFileManager::createFile(const string &fileName)
{
	//check if file already exists, if so then return -1 ?
	ifstream exists(fileName);
	if(exists.good()) {
		return -1;
	}
//    exists.close();
	ofstream myfile;
	myfile.open(fileName.c_str());
	if(myfile.is_open()){
	    void *data = malloc(PAGE_SIZE);
	    for(unsigned i = 0; i < PAGE_SIZE; i++)
	    {
	        *((char *)data+i) = '~';
	    }
	    myfile.write((char *) data, PAGE_SIZE);

	    myfile.seekp(0, ios::beg);

		int readCounter = 0;
		char* readPtr = (char *) malloc(sizeof(int));
		memcpy((char *)readPtr, &readCounter, sizeof(int));
//		cout << "RC: " << *(int *) readPtr << endl;

		myfile.write((char *) readPtr, sizeof(int));

	    int writeCounter = 0;
		char* writePtr = (char *) malloc(sizeof(int));
		memcpy((char *)writePtr, &writeCounter, sizeof(int));

		myfile.write((char *) writePtr, sizeof(int));

	    int appendCounter = 0;
		char* appendPtr = (char *) malloc(sizeof(int));
		memcpy((char *)appendPtr, &appendCounter, sizeof(int));

		myfile.write((char *) appendPtr, sizeof(int));

        free(data);
		free(readPtr);
		free(writePtr);
		free(appendPtr);

		myfile.close();
		return 0;
	}
    return -1;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	if( remove(fileName.c_str()) != 0 ){
		return -1;
	}
	return 0;
}

//https://stackoverflow.com/questions/4316442/stdofstream-check-if-file-exists-before-writing
RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	struct stat buf;
	bool exists = false;
	if (stat(fileName.c_str(), &buf) != -1) {
	        exists = true;
	}
	if(!exists) {
		return -1;
	}

	if(fileHandle.currFilePointer.is_open()) {
		return -1;
	}

	fileHandle.currFilePointer.open(fileName, std::fstream::in | std::fstream::out | std::fstream::binary);

	fileHandle.currFilePointer.seekg(sizeof(int) * 0, ios::beg);
	char* readPtr = (char *) malloc(sizeof(int));
	fileHandle.currFilePointer.read(readPtr, 4);
//	cout << "RC Persist: " << *((int *)readPtr) << endl;
	fileHandle.readPageCounter = *((int *) readPtr);

	fileHandle.currFilePointer.seekg(sizeof(int) * 1, ios::beg);
	char* writePtr = (char *) malloc(sizeof(int));
	fileHandle.currFilePointer.read(writePtr, 4);
//	cout << "WC Persist: " << *((int *)writePtr) << endl;
	fileHandle.writePageCounter = *((int *) writePtr);


	fileHandle.currFilePointer.seekg(sizeof(int) * 2, ios::beg);
	char* appendPtr = (char *) malloc(sizeof(int));
	fileHandle.currFilePointer.read(appendPtr, 4);
//	cout << "AC Persist: " << *((int *)appendPtr) << endl;
	fileHandle.appendPageCounter = *((int *) appendPtr);

	free(readPtr);
	free(writePtr);
	free(appendPtr);


	fileHandle.currFilePointer.clear();
	fileHandle.currFilePointer.seekp(0);

	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fileHandle.currFilePointer.close();
//	if(fileHandle.currFilePointer.good()) {
		return 0;
//	}
//	return -1;
}


FileHandle::FileHandle()
{
//	currFilePointer = new fstream();
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
//	currFilePointer.close();
//	delete(currFilePointer);
}

//https://www.tutorialspoint.com/c_standard_library/c_function_fread.htm
RC FileHandle::readPage(PageNum pageNum, void *data)
{

	currFilePointer.clear();
	currFilePointer.seekg(0);

	currFilePointer.seekg(0, ios::end);
	unsigned int maxPages = (currFilePointer.tellp() / (PAGE_SIZE)) - 1;
//	cout << currFilePointer.tellg() << endl;
//	cout << "Max pages: " << maxPages << endl;
	if(pageNum >= maxPages) {
//			cout << "trying to read a non existent page, fail :)" << endl;
			return -1;
	}
	currFilePointer.clear();
	currFilePointer.seekg((pageNum + 1)*(PAGE_SIZE), currFilePointer.beg);
	currFilePointer.read((char *)data, PAGE_SIZE);
//	cout << "curr pos: " << currFilePointer.cur << endl;

	int newReadPageCounter = getPersistedReadCounter();
	readPageCounter = newReadPageCounter + 1;
	setPersistedReadCounter(readPageCounter);

	// setting it back to beginnning, not sure why
//	currFilePointer.seekg(0, currFilePointer.beg);

	currFilePointer.clear();
	currFilePointer.seekg(0);

	return 0;

    //return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
////	void *dataToWrite = malloc(sizeof(data));
////	memcpy(dataToWrite, data, PAGE_SIZE);
//	currFilePointer.write((char *)data, sizeof(data));
//	writePageCounter++;
//	return 0;

	currFilePointer.clear();
	currFilePointer.seekp(0);
	//// --- based on read ---
//	cout << "In writeqwf" << endl;
	currFilePointer.seekp(0, ios::end);
	unsigned int maxPages = (currFilePointer.tellg() / (PAGE_SIZE)) - 1;
//	cout << currFilePointer.tellg() << endl;
//	cout << "Max pages: " << maxPages << endl;

	if(pageNum >= maxPages) {
//		cout << "failing" << endl;
		return -1;
	}
//	cout << "total len before write : " << currFilePointer.tellg() << endl;
//	cout << "before Write: " << currFilePointer.cur << endl;

//	cout << "Writing on page " << pageNum << endl;
	currFilePointer.seekp((pageNum + 1)*(PAGE_SIZE), currFilePointer.beg);
//	cout << "curr pos in Write: " << currFilePointer.cur << endl;

	currFilePointer.write((char *)data, PAGE_SIZE);
//	cout << "total len : " << currFilePointer.tellg() << endl;


	int newWritePageCounter = getPersistedWriteCounter();
	writePageCounter = newWritePageCounter + 1;
	setPersistedWriteCounter(writePageCounter);

	currFilePointer.clear();
	currFilePointer.seekp(0);

	return 0;

//    return -1;
}

// write always appends in the end of the file
RC FileHandle::appendPage(const void *data)
{
	currFilePointer.clear();
	currFilePointer.seekp(0);

	// go to the end of the page to append
	currFilePointer.seekp(0, ios::end);

	currFilePointer.write((char *) data, PAGE_SIZE);

	int newAppendCounter = getPersistedAppendCounter();
	appendPageCounter = newAppendCounter + 1;
	setPersistedAppendCounter(appendPageCounter);

	currFilePointer.clear();
	currFilePointer.seekp(0);

	return 0;

//    return -1;
}



int FileHandle::getPersistedReadCounter(){
	currFilePointer.seekg(sizeof(int) * 0, ios::beg);
	char* readPtr = (char *) malloc(sizeof(int));
	currFilePointer.read(readPtr, 4);
	int readPC = *((int *) readPtr);
	free(readPtr);
	return readPC;
}

int FileHandle::getPersistedWriteCounter(){
	currFilePointer.seekg(sizeof(int) * 1, ios::beg);
	char* writePtr = (char *) malloc(sizeof(int));
	currFilePointer.read(writePtr, 4);
	int writePC = *((int *) writePtr);
	free(writePtr);
	return writePC;
}

int FileHandle::getPersistedAppendCounter(){
//	return appendPageCounter;
	currFilePointer.seekg(sizeof(int) * 2, ios::beg);
	char* appendPtr = (char *) malloc(sizeof(int));
	currFilePointer.read(appendPtr, 4);
	int appendPC = *((int *) appendPtr);
	free(appendPtr);
	return appendPC;
}

void FileHandle::setPersistedReadCounter(int readPC){
    currFilePointer.seekp(sizeof(int) * 0, ios::beg);
	char* readPtr = (char *) malloc(sizeof(int));
	memcpy((char *)readPtr, &readPC, sizeof(int));
	currFilePointer.write((char *) readPtr, sizeof(int));
	free(readPtr);
}

void FileHandle::setPersistedWriteCounter(int writePC){
    currFilePointer.seekp(sizeof(int) * 1, ios::beg);
	char* writePtr = (char *) malloc(sizeof(int));
	memcpy((char *)writePtr, &writePC, sizeof(int));
	currFilePointer.write((char *) writePtr, sizeof(int));
	free(writePtr);
}

void FileHandle::setPersistedAppendCounter(int appendPC){
    currFilePointer.seekp(sizeof(int) * 2, ios::beg);
	char* appendPtr = (char *) malloc(sizeof(int));
	memcpy((char *)appendPtr, &appendPC, sizeof(int));
	currFilePointer.write((char *) appendPtr, sizeof(int));
	free(appendPtr);
}

unsigned FileHandle::getNumberOfPages()
{
	return appendPageCounter;
//    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}
