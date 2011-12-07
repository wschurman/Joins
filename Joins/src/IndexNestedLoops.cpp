#include "join.h"
#include "scan.h"
#include "bufmgr.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"


//---------------------------------------------------------------
// IndexNestedLoops::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an index nested loops join on the specified relations. 
// You should create a BTreeFile index on the join attribute of the right 
// relation, and then probe it for each record in the left relation. Remember 
// that the BTree expects string keys, so you will have to convert the integer
// attributes to a string using JoinMethod::toString. Note that the join may 
// not be a foreign key join, so there may be multiple records indexed by the 
// same key. Good thing our B-Tree supports this! Don't forget to destroy the 
// BTree when you are done. 
//---------------------------------------------------------------
Status IndexNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	Status s;
	HeapFile* tmpHeap = new HeapFile(NULL, s);
	if (s != OK) {
		std::cout << "Creating new Heap File Failed" << std::endl;
		return FAIL;
	}

	Scan * leftScan = left.file->OpenScan(s);
	if (s != OK) {
		std::cout << "Open scan left failed" << std::endl;
		return FAIL;
	}

	Scan * rightScan = right.file->OpenScan(s);
	if (s != OK) {
		std::cout << "Open scan left failed" << std::endl;
		return FAIL;
	}

	//store data to be joined
	RecordID leftRid, rightRid, rightFirstRid, outRid;
	int* leftRec = new int[left.numOfAttr];
	int* rightRec = new int[right.numOfAttr];
	int leftRecLen = left.recLen;
	int rightRecLen = right.recLen;

	char* newRec = new char[left.recLen + right.recLen];

	Status status;
	BTreeFile *rightTree;

	//create tree
	rightTree = new BTreeFile(status, "BTree1");

	//store key strings
	char* rkey = new char[9];
	char* lkey = new char[9];

	//store each entry in right in key
	while (rightScan->GetNext(rightRid, (char *)rightRec, rightRecLen) != DONE) {
		toString(rightRec[right.joinAttr], rkey);
		rightTree->Insert(rkey, rightRid);
	}

	//vairiable to store scan into tree
	BTreeFileScan *keyScan;
	char * keyPtr;

	//scan for each left key in tree
	while (leftScan->GetNext(leftRid, (char *)leftRec, leftRecLen) != DONE) {
		toString(leftRec[left.joinAttr], lkey);
		keyScan = rightTree->OpenScan(lkey, lkey); //scan on left key
		//insert joined record and continue to scan until key changes
		while (keyScan->GetNext(rightRid, keyPtr)  != DONE) {
			right.file->GetRecord(rightRid, (char *)rightRec, rightRecLen);
			MakeNewRecord(newRec, (char *)leftRec, (char *)rightRec, left, right);
			tmpHeap->InsertRecord(newRec, left.recLen + right.recLen, outRid);
		}
	}

	out.file = tmpHeap;
	//std::cout << "NUM INL: " << tmpHeap->GetNumOfRecords() << std::endl;

	//delete objects and clear tree
	delete leftScan;
	delete rightScan;
	delete leftRec;
	delete rightRec;
	delete newRec;
	rightTree->DestroyFile();
	delete rightTree; 
	delete lkey;
	delete rkey;
	delete keyScan;

	return OK;
}