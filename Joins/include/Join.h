#ifndef JOIN_H
#define JOIN_H

#include "minirel.h"
#include "heapfile.h"

#define MAX_REL_NAME_LENGTH 32 // MAX relation name length
#define MAX_ATTR 10 // Max # of attributes



// Representnts a relation for to join. 
struct JoinSpec {
	// relation name
	char relName[MAX_REL_NAME_LENGTH+1];

	// heapfile storing the relation. 
	HeapFile *file; 

	// # of arributes
	int numOfAttr;
	
	// length of each record (numOfAttr * sizeof(int)).
	int recLen; 

	// join attribute, = i means the ith attribute
	int joinAttr; 

	// offset: the offset of join attribute from the beginning of record
	int offset; 

	void PrintRelation(const char* filename = NULL);
};


// Base class for individual join algorithms. 
class JoinMethod {


public:		
	// Static methods that may be useful for implementing other join methods.
	static void toString(const int n, char* str, int pad = 8);
	static void MakeNewRecord(char* newRec, char* leftRec, char* rightRec, 
		                      JoinSpec& leftSpec, JoinSpec& rightSpec);
	static HeapFile* SortHeapFile(HeapFile *file, int len, int offset);

public:
	// Virtual method that all derived classes should implement. 
	virtual Status Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out);
};


//
// Classes for individual join methods. See the .cpp files for more info. 
//

class TupleNestedLoops : public JoinMethod {
public:
	Status Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out);
};

class BlockNestedLoops : public JoinMethod {
public:
	int blockSize;
	BlockNestedLoops(int _blockSize = 100) { blockSize = _blockSize; }

	Status Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out);
};

class IndexNestedLoops : public JoinMethod {
public:
	Status Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out);
};

class SortMerge : public JoinMethod {
public:
	Status Execute(JoinSpec& left, JoinSpec& right, JoinSpec& out);
};


#endif

