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
	return FAIL;
}