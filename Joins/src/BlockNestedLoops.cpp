#include "join.h"
#include "scan.h"


//---------------------------------------------------------------
// BlockNestedLoop::Execute
//
// Input:   left  - The left relation to join. 
//          right - The right relation to join. 
// Output:  out   - The relation to hold the ouptut. 
// Return:  OK if join completed succesfully. FAIL otherwise. 
//          
// Purpose: Performs a block nested loops join on the specified relations. 
// You can find a specification of this algorithm on page 455. You should 
// choose the smaller of the two relations to be the outer relation, but you 
// should make sure to concatenate the tuples in order <left, right> when 
// producing output. The block size can be specified in the constructor, 
// and is stored in the variable blockSize. 
//---------------------------------------------------------------
Status BlockNestedLoops::Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out) {
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
	char* blockArray = new char[left.recLen * blockSize];
	int blockArraySize = 0;
	int* leftCurrRec = (int *)blockArray;
	int* leftRec = new int[left.numOfAttr];
	int* rightRec = new int[right.numOfAttr];
	int leftRecLen = left.recLen;
	int rightRecLen = right.recLen;

	char* newRec = new char[left.recLen + right.recLen];

	rightFirstRid = rightScan->currRid;

	Status st = OK;

	while (true) {
		st = leftScan->GetNext(leftRid, (char *)leftRec, leftRecLen);
		if (blockArraySize < blockSize) {
			memcpy(blockArray + left.recLen * blockArraySize, leftRec, left.recLen);
			blockArraySize++;
			if (st != DONE) {
				continue;
			}
		}

		while (rightScan->GetNext(rightRid, (char *)rightRec, rightRecLen) != DONE) {
			for (int j = 0; j < blockSize; j++) {
				if (j >= blockArraySize) {
					break;
				}
				leftCurrRec = (int *) (blockArray + left.recLen * j);
				if (leftCurrRec[left.joinAttr] == rightRec[right.joinAttr]) {
					MakeNewRecord(newRec, (char *)leftCurrRec, (char *)rightRec, left, right);
					tmpHeap->InsertRecord(newRec, left.recLen + right.recLen, outRid);
				}
			}
		}
		rightScan->MoveTo(rightFirstRid);
		blockArraySize = 0;

		if (st == DONE) {
			break;
		}
	}

	delete blockArray;
	delete leftRec;
	delete rightRec;
	delete newRec;

	out.file = tmpHeap;

	std::cout << "NUM BNL: " << tmpHeap->GetNumOfRecords() << std::endl;

	return OK;
}