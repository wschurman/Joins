#ifndef _TEST_SCHEMA_H
#define _TEST_SCHEMA_H

#include "join.h"

enum GenOpts {ALL_MATCH, NONE_MATCH, RANDOM}; 

class TestSchema {

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
		int budget;
		int status;
	};

	static const int NUM_PROJECT_ATTRS = 5;
public:
	static Status CreateRandomEmployeeRelation(JoinSpec& spec, 
		                                       int numEmployeeRecs,
		                                       int numProjectRecs,
											   bool foreignKey = true,
											   GenOpts opts = RANDOM);

	static Status CreateRandomProjectRelation(JoinSpec& spec, 
		                                      int numEmployeeRecs, 
											  int numProjectRecs, 
											  bool foreignKey = true,
											  GenOpts opts = RANDOM);




};
#endif