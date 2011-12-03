#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "minirel.h"
#include "heapfile.h"
#include "scan.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"
#include "join.h"


//--------------------------------------------------------------------
// JoinSpec::PrintRelation
// 
// Purpose :  Prints the relation represented by a JoinSpec.
// Input   :  filename  - The file to which to write the output. When NULL (default)
//                        output is written to the screen. 
// Output  :  None
// Return  :  None
//-------------------------------------------------------------------- 
void JoinSpec::PrintRelation(const char* filename) {
	Status s;
	Scan *scan = file->OpenScan(s);

	if (s != OK) {
		std::cerr << "Cannot open scan on HeapFile in PrintRelation."<< std::endl;
		return;
	}


	FILE* f;
	if(filename == NULL) {
		f = stdout;
	}
	else {
		f = fopen(filename, "w");
		if (f == NULL) {
			std::cerr << "Cannot open file " << filename << " for writing.\n";
			return;
		}
	}

	char* rec = new char[recLen];

	int len = recLen;
	RecordID rid;

	while(scan->GetNext(rid, rec, len) != DONE) {
		if(len != recLen) {
			std::cerr << "Unexpected record length in print method." << std::endl;
			return;
		}
		for(int i = 0; i < numOfAttr; i++) {
			fprintf(f, "%d ", *(((int*)(rec)) + i));
		}
		fprintf(f, "\n");
	}

	if(filename != NULL) {
		fclose(f);
	}

	delete [] rec;
}

//-------------------------------------------------------------------
// JoinMethod::toString
//
// Input   : n,   The number to convert
//           pad, The number of digits to use. Should be larger than the 
//                number of digits in n. Leading digits will be 0. 
// Output  : str, The converted number. 
// Return  : None
// Purpose : Converts a number of a strings so that it can be used as a 
//           key in a BTreeFile.
//-------------------------------------------------------------------
void JoinMethod::toString(const int n, char* str, int pad) {
	char format[200];
	sprintf(format, "%%0%dd", pad);
	sprintf(str, format, n);
}

//--------------------------------------------------------------------
// JoinMethod::MakeNewRecord
// 
// Purpose :  Concatenates two records to construct the output of a join. 
// Input   :  leftRec   - pointer to the left record.
//            rightRec  - pointer to the right record. 
//            leftSpec  - JoinSpec for relation containing leftRec.
//            rightSpec - JoinSpec for relation containing rightRec.
//            newRec    - A buffer with leftSpec.recLen + rightSpec.recLen space.
// Output  :  newRec    - The newly allocated record. 
// Return  :  None
//-------------------------------------------------------------------- 
void JoinMethod::MakeNewRecord(char* newRec, char* leftRec, char* rightRec,
	                           JoinSpec& leftSpec, JoinSpec& rightSpec) {
    memcpy(newRec, leftRec, leftSpec.recLen);
    memcpy(newRec + leftSpec.recLen, rightRec, rightSpec.recLen);
}


//--------------------------------------------------------------------
// JoinMethod::SortFile
// 
// Purpose :  Sorts a relation by an integer attribute. 
// Input   :  file - pointer to the HeapFile to be sorted.
//            len  - length of the records in the file. (assume fixed size).
//            offset - offset of the sort attribute from the beginning of the record.
// Method  :  We create a B+-Tree using that attribute as the key. Then
//            we scan the B+-Tree and insert the records into a new
//            HeapFile. he HeapFile guarantees that the order of 
//            insertion will be the same as the order of scan later.
// Return  :  The new sorted relation/HeapFile.
//-------------------------------------------------------------------- 
HeapFile* JoinMethod::SortHeapFile(HeapFile *file, int len, int offset) {

	Status s;

	Scan *scan;
	scan = file->OpenScan(s);
	if (s != OK) {
		std::cerr << "ERROR : cannot open scan on the heapfile to sort." << std::endl;
	}

	//
	// Scan the HeapFile S, create a new B+Tree and insert the records into B+Tree.
	// 

	BTreeFile *btree;
	btree = new BTreeFile (s, "BTree");

	char* recPtr = new char[len];
	int recLen = len;
	RecordID rid;

	char* recKey = new char[100];

	while (scan->GetNext(rid, recPtr, recLen) == OK)
	{
		int* valPtr = (int*)(recPtr+offset);
		int val = *valPtr;
		toString(val,recKey);
		btree->Insert(recKey, rid);
	}
	delete scan;
	delete [] recKey;
	//std::cout << "created B+ tree!" << std::endl;

	HeapFile *sorted = new HeapFile(NULL, s); // create a temp HeapFile
	if (s != OK)
	{
		std::cerr << "Cannot create new file for sortedS\n";
	}

	// Now scan the B+-Tree and insert the records into a 
	// new (sorted) HeapFile.

	BTreeFileScan* btreeScan = btree->OpenScan(NULL, NULL);

	//int key;
	char* keyPtr;
	while (btreeScan->GetNext(rid, keyPtr) == OK)
	{
		//std::cout << "scanning " << rid << " " << keyPtr << std::endl;

	    file->GetRecord (rid, recPtr, recLen);
	    sorted->InsertRecord (recPtr, recLen, rid);
	}
	btree->DestroyFile();

	delete btree;
	delete btreeScan;
	delete [] recPtr;

	return sorted;
}







//--------------------------------------------------------------------
// JoinMethod::Execute
// 
// Purpose :  Initializes all attributes of the out JoinSpec except the 
//            file. Individual join implementations should call this method. 
// Input   :  left   - The left relation to join. 
//            right  - The right relation to join. 
// Output  :  out    - The relation that will hold the join result. 
// Return  :  OK     - if the join succeeded. 
//            FAIL   - otherwise. 
//-------------------------------------------------------------------- 
Status JoinMethod::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	out.recLen = left.recLen + right.recLen;
	out.joinAttr = -1;
	out.offset = -1;
	out.numOfAttr = left.numOfAttr + right.numOfAttr;
	sprintf(out.relName, "OutputRel");
	return OK;
}