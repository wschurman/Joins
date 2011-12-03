#include "join.h"
#include "scan.h"

//---------------------------------------------------------------
// TupleNestedLoop::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs a nested loop join on relations left and right
//          a tuple a time. You can assume that left is the outer
//          relation and right is the inner relation. 
//---------------------------------------------------------------
Status TupleNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
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

	RecordID leftRid, rightRid, rightFirstRid, outRid;
	int* leftRec = new int[left.numOfAttr];
	int* rightRec = new int[right.numOfAttr];
	int leftRecLen = left.recLen;
	int rightRecLen = right.recLen;

	char* newRec = new char[left.recLen + right.recLen];

	rightFirstRid = rightScan->currRid;

	while (leftScan->GetNext(leftRid, (char *)leftRec, leftRecLen) != DONE) {
		while (rightScan->GetNext(rightRid, (char *)rightRec, rightRecLen) != DONE) {
			if (leftRec[left.joinAttr] == rightRec[right.joinAttr]) {
				MakeNewRecord(newRec, (char *)leftRec, (char *)rightRec, left, right);
				tmpHeap->InsertRecord(newRec, left.recLen + right.recLen, outRid);
			}
		}
		rightScan->MoveTo(rightFirstRid);
	}

	delete leftRec;
	delete rightRec;
	delete newRec;

	out.file = tmpHeap;

	std::cout << "NUM TNL: " << tmpHeap->GetNumOfRecords() << std::endl;

	return OK;
}