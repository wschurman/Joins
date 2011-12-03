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
	//Your code here. 
	return FAIL;
}