#ifndef _TEST_SCHEMA_H_
#define _TEST_SCHEMA_H_

#include "minirel.h"


enum GenOpts { RANDOM, ALL_MATCH, NONE_MATCH};

class TestSchema {
private:
	static void RandomPermute (short *permutation, int n);

public:
	struct Employee {
		int id;
		int age;
		int proj;
		int salary;
		int rating;
		int dept;
	};
	
	static const int NUM_EMPLOYEE_ATTRS = 6;

	struct Project {
		int id;
		int fund;
		int manager;
		int status;
		int budget;
	};

	static const int NUM_PROJECT_ATTRS = 5;




	static Status CreateRandomEmployeeRelation(JoinSpec& spec,
	                                           int numEmployeeRecs,
	                                           int numProjectRecs,
											   bool foreignKey,
											   GenOpts opts);
	

	static Status CreateRandomProjectRelation(JoinSpec& spec,
	                                          int numEmployeeRecs,
	                                          int numProjectRecs,
											  bool foreignKey,
											  GenOpts opts);




	// Simple pseudorandom number generator defined in ISO standard
	//     www.open-std.org/jtc1/sc22/wg14/www/docs/n1256.pdf
	// We use this instead of rand() to ensure that it produces deterministic 
	// output across multpile calls. 
	//
	// Note: This is a terrible random number generator. You should not use it anywhere 
	// that randomness is important. It's sufficient for simple tests, however. 
	static const int MY_RAND_MAX = 32767;
	static unsigned long int next;
	static int rand() {
		next = next * 1103515245 + 12345;
		return (unsigned int)(next/65536) % 32768;
	}

	static void srand(unsigned int seed) {
		next = seed;
	}




};



#endif
