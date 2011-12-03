#ifndef _JOIN_TEST_
#define _JOIN_TEST_

#include "join.h"
#include "TestSchema.h"

class JoinTest {
private:

	static void RunTest(int i);

	static bool TestValid(JoinSpec& l, JoinSpec &r, JoinSpec &out, int count = -1);

	static bool CompareJoins(JoinMethod& j1, JoinMethod& j2, JoinSpec& leftSpec, JoinSpec& rightSpec);

	static bool GenAndCompareJoins(JoinMethod* j1, 
		                           JoinMethod* j2, 
								   int empSize, 
								   int projSize, 
								   bool foreignKey, 
								   GenOpts opts); 

	static bool GenAndTestCount(JoinMethod* j1,
		                        int empSize,
								int projSize, 
								bool foreignKey, 
								GenOpts opts, 
								int size);



	// Tests TupleNestedLoops join on several statically computed relations. 
	static bool Test1();
	static bool Test2();
	static bool Test3();
	static bool Test4();


public:

	static void RunTests(std::istream& in, int dbSize, int buffPoolSize);

	




};

#endif