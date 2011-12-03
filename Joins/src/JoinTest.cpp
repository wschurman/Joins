#include "JoinTest.h"
#include "scan.h"
#include "bufmgr.h"
#include "db.h"
#include "TestSchema.h"
#include <iostream>	
#include <ctime>


// Test Driver. 
void JoinTest::RunTests(std::istream& in, int dbSize, int buffPoolSize) {
	char *dbname="btdb";
	char *logname="btlog";

	remove(dbname);
	remove(logname);

	Status status;
	minibase_globals = new SystemDefs(status, dbname, logname, 
		                              dbSize, 500, buffPoolSize);
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}

	TestSchema::srand(time(NULL));

	char command[200];
	in >> command;

	while(in) {
		if(!strcmp(command, "test")) {
			int testNum; 
			in >> testNum;
			RunTest(testNum);
		}
		else if(!strcmp(command, "quit")) {
			break;
		}
		else if(!strcmp(command, "seed")) {
			unsigned int seed;
			in >> seed;
			TestSchema::srand(seed);
		}
		else {
			std::cerr << "Error: Unkown command" << std::endl;
		}
		in >> command;
	}

	delete minibase_globals;
	remove(dbname);
	remove(logname);
}


// Runs test i and prints out the result. 
void JoinTest::RunTest(int i) {
	std::cout << "Starting test " << i << " ..." << std::endl;
	bool res;

	srand(time(NULL));

	switch(i) {
	case 1:
		res = Test1();
		break;
	case 2:
		res = Test2();
		break;
	case 3:
		res = Test3();
		break;
	case 4:
		res = Test4();
		break;
	default:
		std::cerr << "Unknown test case!" << std::endl;
		return;
	}

	if(res) {
		std::cout << "PASSED Test " << i << std::endl;
	}
	else {
		std::cout << "FAILED Test " << i << std::endl;
	}
}

//--------------------------------------------------------------------
// JoinMethod::TestValid
// 
// Purpose :  Tests whether all output tuples are valid and optionally 
//            checks the number. 
// Input   :  l     - The left relation that was joined. 
//            r     - The right relation that was joined. 
//            out   - The output of the join. 
//            count - The number of expected output tuples. 
//                    If -1, the count won't be checked. 
//            newRec    - A buffer with leftSpec.recLen + rightSpec.recLen space.
// Output  :  None
// Return  :  True iff the test passed. 
//-------------------------------------------------------------------- 
bool JoinTest::TestValid(JoinSpec& l, JoinSpec &r, JoinSpec &out, int count) {
	Status s;

	Scan* scan = out.file->OpenScan(s);

	RecordID rid;
	char* rec = new char[out.recLen];
	int recLen = out.recLen;
	bool ret = true;

	int num = 0;
	while(scan->GetNext(rid, rec, recLen) == OK) {
		num++;
		int* intRec = (int*) rec;

		if(intRec[l.joinAttr] != intRec[l.numOfAttr + r.joinAttr]) {
			std::cerr << "Error: Join attributes don't match" << std::endl;
			std::cerr << "rec: ";
			for(int j = 0; j < out.numOfAttr; j++) {
				std::cerr << intRec[j] << " ";
			}
			std::cerr << std::endl;
			ret = false;
		}
	}

	if(count != -1 && num != count) {
		std::cerr << "Error: Expected " << count 
		       << " results, but got " << num << std::endl;
		ret = false;
	}

	return ret;
}


/**
*
*
* Requires the first attribute of both relations be a key. 
*
*
*
*/

//--------------------------------------------------------------------
// JoinMethod::CompareJoins
// 
// Purpose :  Runs and compares the output of two different joins.  
// Input   :  j1         - The first join method to compare.
//            j2         - The second join method to compare. 
//            leftSpec   - The left relation to join. 
//            rightSpec  - The right relation to join. 
// Output  :  None
// Return  :  True iff the test passed. 
// Note    :  This method assumes that the first attribute of both the
//            left and right relations is a key. 
//-------------------------------------------------------------------- 
bool JoinTest::CompareJoins(JoinMethod& j1, JoinMethod& j2, 
    	          JoinSpec& leftSpec, JoinSpec& rightSpec) {

    Status s;
    JoinSpec out1;
	s = j1.Execute(leftSpec, rightSpec, out1);

	if(s == FAIL) {
		return false;
	}


    JoinSpec out2;
	s = j2.Execute(leftSpec, rightSpec, out2);
	if(s == FAIL) {
		return false;
	}


	if(out1.recLen != out2.recLen) {
		std::cerr << "Error: Join results have different record length." 
			      << std::endl;
		return false;
	}

	if(out1.numOfAttr != out2.numOfAttr) {
		std::cerr << "Error: Join results have different number of attributes." 
			      << std::endl;
		return false;
	}

	if(out1.file->GetNumOfRecords() != out2.file->GetNumOfRecords()) {
		std::cerr << "Error: Join results have different number of records." 
			      << std::endl;
		return false;
	}

	//std::cout << "Number of ouptut records: " << out1.file->GetNumOfRecords() << std::endl;


	// First sort on left key, then right key, so the join results are 
	// totally ordered.
	HeapFile* sorted1 = JoinMethod::SortHeapFile(out1.file, out1.recLen, leftSpec.numOfAttr * sizeof(int));
	HeapFile* sorted2 = JoinMethod::SortHeapFile(out2.file, out2.recLen, leftSpec.numOfAttr * sizeof(int));

	HeapFile* sorted1final = JoinMethod::SortHeapFile(sorted1, out1.recLen, 0);
	HeapFile* sorted2final = JoinMethod::SortHeapFile(sorted2, out2.recLen, 0);

	delete sorted1;
	delete sorted2;

	// Compare the sorted relations.

	Scan* scan1 = sorted1final->OpenScan(s);
	if(s != OK) {
		std::cerr << "Error opening scan1 in CompareJoins." << std::endl;
	}

	Scan* scan2 = sorted2final->OpenScan(s);
	if(s != OK) {
		std::cerr << "Error opening scan2 in CompareJoins." << std::endl;
	}
	char* rec1 = new char[out1.recLen];
	char* rec2 = new char[out2.recLen];
	RecordID rid1, rid2;

	bool ans = true;
	// Loop through relation R, one tuple at a time
	while(scan1->GetNext(rid1, rec1, out1.recLen) == OK) {

		if(scan2->GetNext(rid2, rec2, out2.recLen) != OK) {
			std::cerr << "Error fetching record from scan2 in CompareJoins." 
				      << std::endl;
		}

		int* intRec1 = (int*)rec1;
		int* intRec2 = (int*)rec2;

		for(int i = 0; i < out1.numOfAttr; i++) {
			
			if(intRec1[i] != intRec2[i]) {
				std::cerr << "Output tuples don't match on attribute " << i 
 					      << std::endl;

				std::cerr << "rec1 is ";
				for(int j = 0; j < out1.numOfAttr; j++) {
					std::cerr << intRec1[j] << " ";
				}
				std::cerr << std::endl;
				std::cerr << "rec2 is ";
				for(int j = 0; j < out2.numOfAttr; j++) {
					std::cerr << intRec2[j] << " ";
				}
				std::cerr << std::endl;
				ans = false;
				break;
			}
		}

	}

	delete out1.file;
	delete out2.file;
	delete sorted1final;
	delete sorted2final;
	delete [] rec1;
	delete [] rec2;

	return ans;
}


//--------------------------------------------------------------------
// JoinMethod::CompareJoins
// 
// Purpose :  Generates test relations and compares two joins on them. 
// Input   :  j1         - The first join method to compare.
//            j2         - The second join method to compare. 
//            empSize    - The size of the first relation (Employee)
//            projSize   - The size of the second relation (Project)
//            foreignKey - Whether the join should be a foreign key join. 
//            opts       - Options. Whether the join should return no
//                         results or be a complete cross product. 
// Output  :  None
// Return  :  True iff the test passed. 
// Note    :  opts cannot be ALL_MATCH if foreignKey is true. 
//-------------------------------------------------------------------- 
bool JoinTest::GenAndCompareJoins(JoinMethod* j1,
	                              JoinMethod* j2,
								  int empSize,
								  int projSize,
								  bool foreignKey,
								  GenOpts opts) {

	JoinSpec emp;
	JoinSpec proj;

	Status s;
	s = TestSchema::CreateRandomEmployeeRelation(emp, empSize, projSize,
		                                         foreignKey, opts);

	if(s == FAIL) {
		std::cerr << "Error creating employee relation." << std::endl;
		return false;
	}

	s = TestSchema::CreateRandomProjectRelation(proj, empSize, projSize,
		                                        foreignKey, opts);

	if(s == FAIL) {
		std::cerr << "Error creating project relation." << std::endl;
		return false;
	}

	bool ret;
	ret = CompareJoins(*j1, *j2, emp, proj);
	emp.file->DeleteFile();
	proj.file->DeleteFile();
	delete emp.file;
	delete proj.file;

	return ret;
}

//--------------------------------------------------------------------
// JoinMethod::CompareJoins
// 
// Purpose :  Generates test relations and counts the output of a single join. 
// Input   :  j1         - The join method.
//            empSize    - The size of the first relation (Employee)
//            projSize   - The size of the second relation (Project)
//            foreignKey - Whether the join should be a foreign key join. 
//            opts       - Options. Whether the join should return no
//                         results or be a complete cross product. 
//            size       - The expected size of the output. 
// Output  :  None
// Return  :  True iff the test passed. 
// Note    :  opts cannot be ALL_MATCH if foreignKey is true. 
//-------------------------------------------------------------------- 
bool JoinTest::GenAndTestCount(JoinMethod* j1, 
	                           int empSize,
							   int projSize,
							   bool foreignKey,
							   GenOpts opts,
							   int size) {
	JoinSpec emp;
	JoinSpec proj;

	Status s;
	s = TestSchema::CreateRandomEmployeeRelation(emp, empSize, projSize,
		                                         foreignKey, opts);

	if(s == FAIL) {
		std::cerr << "Error creating employee relation." << std::endl;
		return false;
	}

	s = TestSchema::CreateRandomProjectRelation(proj, empSize, projSize,
		                                        foreignKey, opts);

	if(s == FAIL) {
		std::cerr << "Error creating project relation." << std::endl;
		return false;
	}
	bool ret;
	JoinSpec out;
	s = j1->Execute(emp, proj, out);
	if(s == FAIL) {
		return false;
	}

	//std::cout << "size: " << out.file->GetNumOfRecords() << std::endl;
	ret = TestValid(emp, proj, out, size);


	emp.file->DeleteFile();
	proj.file->DeleteFile();

	delete emp.file;
	delete proj.file;
	delete out.file;

	return ret;
}






//--------------------------------------------------------------------
// Tests TupleNestedLoops join. Note that this will reset the random seed. 
//--------------------------------------------------------------------
bool JoinTest::Test1() {
	TupleNestedLoops tl;

    // Seed random number generator deterministically so we know the output size. 
	TestSchema::srand(151235);
	bool ret = GenAndTestCount(&tl, 100, 100, false, RANDOM, 23);
	ret = ret && GenAndTestCount(&tl, 100, 100, true, RANDOM, 100);

	TestSchema::srand(97525);
	ret = ret && GenAndTestCount(&tl, 1000, 1000, true, RANDOM, 1000);
	ret = ret && GenAndTestCount(&tl, 1000, 1000, false, RANDOM, 3256);
	ret = ret && GenAndTestCount(&tl, 3000, 1000, false, RANDOM, 10074);

	// Joins have empty results. 
	ret = GenAndTestCount(&tl, 1000, 100, true, NONE_MATCH, 0);
	ret = ret && GenAndTestCount(&tl, 1000, 1000, false, NONE_MATCH, 0);

	// Join is complete cross product
	ret = ret && GenAndTestCount(&tl, 100, 100, false, ALL_MATCH, 10000);


	//Reseed the time to be NULL. 
	TestSchema::srand(time(NULL));


	return ret;
}


//--------------------------------------------------------------------
// Tests BlockNestedLoops join by comparing with TupleNestedLoopsJoin. 
//--------------------------------------------------------------------
bool JoinTest::Test2() {
	TupleNestedLoops tl;

	//int blockSize = (MINIBASE_BM->GetNumOfUnpinnedBuffers() - 3 * 3)
	//* MINIBASE_PAGESIZE;

	BlockNestedLoops* bl = new BlockNestedLoops(100);

	bool ret = GenAndCompareJoins(&tl, bl, 100, 100, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, bl, 100, 100, false, RANDOM);


	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, false, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, bl, 3000, 1000, false, RANDOM);

	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, true, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, false, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, bl, 100, 100, false, ALL_MATCH);

	delete bl;
	// Try a different block size. 
	bl = new BlockNestedLoops(50); 
	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, bl, 1000, 1000, false, RANDOM);

	delete bl;
	
	//MINIBASE_DB->dump_space_map();
	return ret;
}


//--------------------------------------------------------------------
// Tests IndexNestedLoops join by comparing with TupleNestedLoopsJoin. 
//--------------------------------------------------------------------
bool JoinTest::Test3() {
	TupleNestedLoops tl;
	IndexNestedLoops inl;

	bool ret = GenAndCompareJoins(&tl, &inl, 100, 100, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &inl, 100, 100, false, RANDOM);


	ret = ret && GenAndCompareJoins(&tl, &inl, 1000, 1000, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &inl, 1000, 1000, false, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &inl, 3000, 1000, false, RANDOM);

	ret = ret && GenAndCompareJoins(&tl, &inl, 1000, 1000, true, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, &inl, 1000, 1000, false, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, &inl, 100, 100, false, ALL_MATCH);

	//MINIBASE_DB->dump_space_map();
	return ret;
}

//--------------------------------------------------------------------
// Tests SortMerge join by comparing with TupleNestedLoopsJoin. 
//--------------------------------------------------------------------
bool JoinTest::Test4() {
	TupleNestedLoops tl;
	SortMerge sm;

	bool ret = GenAndCompareJoins(&tl, &sm, 100, 100, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &sm, 100, 100, false, RANDOM);


	ret = ret && GenAndCompareJoins(&tl, &sm, 1000, 1000, true, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &sm, 1000, 1000, false, RANDOM);
	ret = ret && GenAndCompareJoins(&tl, &sm, 3000, 1000, false, RANDOM);

	ret = ret && GenAndCompareJoins(&tl, &sm, 1000, 1000, true, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, &sm, 1000, 1000, false, NONE_MATCH);
	ret = ret && GenAndCompareJoins(&tl, &sm, 100, 100, false, ALL_MATCH);


	return ret;
}
