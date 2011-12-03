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
	return FAIL;
}