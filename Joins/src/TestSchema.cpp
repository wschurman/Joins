#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "heapfile.h"
#include "scan.h"
#include "join.h"
#include "TestSchema.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"



//--------------------------------------------------------
// Used in to generate random permutations.
//--------------------------------------------------------
void TestSchema::RandomPermute (short *permutation, int n)
{
	int temp, random;
	for (int j = 0; j < n; j++)
	{
		permutation[j] = j;
	}

	for (int j = n - 1; j > 0; j--)
	{
		random = TestSchema::rand() % j;
		temp = permutation[j];
		permutation[j] = permutation[random];
		permutation[random] = temp;
	}
}



unsigned long int TestSchema::next = 1;

//--------------------------------------------------------
// Create a random employee relation. 
//--------------------------------------------------------
Status TestSchema::CreateRandomEmployeeRelation(JoinSpec& spec, 
	                                            int numEmployeeRecs,
	                                            int numProjectRecs,
												bool foreignKey,
												GenOpts opts) {

													
    if(foreignKey && opts == ALL_MATCH) {
		std::cerr << "Error: Cannot create a foreign key join in which" 
			      << "all records match!" << std::endl;
		return FAIL;
	}

	Status s;
	HeapFile *F = new HeapFile ("Employees", s); // new HeapFile storing records of R
	if (s != OK) {
		std::cerr << "Cannot create new HeapFile Employees" << std::endl;
		return FAIL;
	}

	Employee e;
	RecordID rid;
	short* permutation = new short[numEmployeeRecs];

	RandomPermute(permutation, numEmployeeRecs); // generate a random array of integer

	for (int i = 0; i < numEmployeeRecs; i++)
	{
		e.id   = permutation[i];
		e.age  = TestSchema::rand() % 20 + 20;

		if(foreignKey && opts == NONE_MATCH) {
			e.proj = i;
		}
		else {		
			e.proj = TestSchema::rand() % numProjectRecs;
			//std::cout << "e.proj " << e.proj << std::endl;
		}

		if(!foreignKey && opts == ALL_MATCH) {
			e.salary = 100000;
		}
		else if(!foreignKey && opts == NONE_MATCH){
			e.salary = i;
		}
		else {
			e.salary = (TestSchema::rand() % 300)*100;
		}

		e.rating = TestSchema::rand() % 5;
		e.dept  = TestSchema::rand() % 30;

		s = F->InsertRecord((char *)&e, sizeof(Employee), rid); // insert records into heapfile
		if (s != OK)
		{
			std::cerr << "Cannot insert record " << i << " into R\n";
			return FAIL;
		}
	}

	strcpy(spec.relName, "Employees");
	spec.numOfAttr = NUM_EMPLOYEE_ATTRS;
	if(foreignKey)
		spec.joinAttr = 2;
	else
		spec.joinAttr = 3;


	spec.recLen = sizeof(Employee);
	spec.file = F;
	spec.offset = spec.joinAttr*sizeof(int);

	delete [] permutation;
	return OK;
}

//--------------------------------------------------------
// Create a random Project relation. 
//--------------------------------------------------------
Status TestSchema::CreateRandomProjectRelation(JoinSpec& spec,
	                                           int numEmployeeRecs,
	                                           int numProjectRecs,
											   bool foreignKey,
											   GenOpts opts) {
	Status s;
	HeapFile *F = new HeapFile ("Projects", s); // new HeapFile storing records of R
	if (s != OK) {
		std::cerr << "Cannot create new HeapFile Projects\n";
		return FAIL;
	}

	Project p;
	RecordID rid;
	short* permutation = new short[numProjectRecs];

	RandomPermute (permutation, numProjectRecs); // generate a random array of integer

	for (int i = 0; i < numProjectRecs; i++)
	{

		if(foreignKey && opts == NONE_MATCH) {
			p.id = i + 1000000;
		}
		else {
			p.id = permutation[i];
			//std::cout << "p.id " << p.id << std::endl;
		}

		p.manager  = TestSchema::rand() % numEmployeeRecs;
		p.fund = (TestSchema::rand() % 500)*10;
		p.status = TestSchema::rand() % 5;


		if(!foreignKey && opts == ALL_MATCH) {
			p.budget = 100000;
		}
		else if(!foreignKey && opts == NONE_MATCH) {
			p.budget = i + 10000000;
		}
		else {
			p.budget = (TestSchema::rand() % 300)*100;
		}



		s = F->InsertRecord((char *)&p, sizeof(Project), rid); // insert records into heapfile
		if (s != OK)
		{
			std::cerr << "Cannot insert record " << i << " into Project heapfile\n";
			return FAIL;
		}
	}

	strcpy(spec.relName, "Project");
	spec.numOfAttr = NUM_PROJECT_ATTRS;
	if(foreignKey)
		spec.joinAttr = 0;
	else
		spec.joinAttr = 4;

	spec.recLen = sizeof(Project);
	spec.file = F;
	spec.offset = spec.joinAttr * sizeof(int);

	delete [] permutation;
	return OK;
}


