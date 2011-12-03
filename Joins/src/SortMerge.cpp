#include "join.h"
#include "scan.h"


//---------------------------------------------------------------
// SortMerge::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs an sort merge join on the specified relations. 
// Please see the pseudocode on page 460 of your text for more info
// on this algorithm. You may use JoinMethod::SortHeapFile to sort
// the relations. 
//---------------------------------------------------------------
Status SortMerge::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
	JoinMethod::Execute(left, right, out);

	HeapFile * leftSorted = SortHeapFile(left.file, left.recLen, left.offset);
	HeapFile * rightSorted = SortHeapFile(right.file, right.recLen, right.offset);

	Status s;

	HeapFile* tmpHeap = new HeapFile(NULL, s);
	if (s != OK) {
		std::cout << "Creating new Heap File Failed" << std::endl;
		return FAIL;
	}

	Scan * leftScan = leftSorted->OpenScan(s);
	if (s != OK) {
		std::cout << "Open scan left failed" << std::endl;
		return FAIL;
	}

	Scan * rightScan = rightSorted->OpenScan(s);
	if (s != OK) {
		std::cout << "Open scan left failed" << std::endl;
		return FAIL;
	}

	RecordID leftRid, rightRid, outRid;
	int* leftRecT = new int[left.numOfAttr]; // Tr
	int* rightRecT = new int[right.numOfAttr]; // Ts
	int* rightRecS = new int[right.numOfAttr]; // Gs
	int leftRecLen = left.recLen;
	int rightRecLen = right.recLen;

	char* newRec = new char[left.recLen + right.recLen];

	Status leftRecTStatus, rightRecTStatus, rightRecSStatus;

	leftRecTStatus = leftScan->GetNext(leftRid, (char *)leftRecT, leftRecLen);
	rightRecTStatus = rightScan->GetNext(rightRid, (char *)rightRecT, rightRecLen);
	rightRecSStatus = rightRecTStatus;
	memcpy(rightRecS, rightRecT, right.recLen);

	while (leftRecTStatus != DONE && rightRecSStatus != DONE) {

		while (leftRecT[left.joinAttr] < rightRecS[right.joinAttr]) {
			leftRecTStatus = leftScan->GetNext(leftRid, (char *)leftRecT, leftRecLen);
		}

		while (leftRecT[left.joinAttr] > rightRecS[right.joinAttr]) {
			rightRecSStatus = rightScan->GetNext(rightRid, (char *)rightRecT, rightRecLen);
		}

		memcpy(rightRecT, rightRecS, right.recLen);

		while (leftRecT[left.joinAttr] == rightRecS[right.joinAttr]) {
			memcpy(rightRecT, rightRecS, right.recLen);

			while (rightRecT[right.joinAttr] == leftRecT[left.joinAttr]) {
				MakeNewRecord(newRec, (char *)leftRecT, (char *)rightRecT, left, right);
				tmpHeap->InsertRecord(newRec, left.recLen + right.recLen, outRid);

				rightRecTStatus = rightScan->GetNext(rightRid, (char *)rightRecT, rightRecLen);
			}

			leftRecTStatus = leftScan->GetNext(leftRid, (char *)leftRecT, leftRecLen);
		}

		memcpy(rightRecT, rightRecS, right.recLen);
	}

	out.file = tmpHeap;

	delete leftScan;
	delete rightScan;
	delete leftRecT;
	delete rightRecT;
	delete rightRecS;
	delete newRec;

	delete leftSorted;
	delete rightSorted;

	std::cout << "NUM TNL: " << tmpHeap->GetNumOfRecords() << std::endl;

	return OK;
}