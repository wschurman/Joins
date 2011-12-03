#include <stdlib.h>
#include <time.h>
#include <conio.h>
#include <ctype.h>

#include "minirel.h"
#include "bufmgr.h"
#include "heapfile.h"
#include "join.h"
#include "JoinTest.h"

int MINIBASE_RESTART_FLAG = 0;// used in minibase part


void printHelp() {
	std::cout << "Enter one of the following commands:"<<std::endl;
	std::cout << "test <testnum>"<<std::endl;
	std::cout << "\ttest 1: Test TupleNestedLoops Join." << std::endl;
	std::cout << "\ttest 2: Compare BlockNestedLoops with TupleNestedLoops."
		      << std::endl;
	std::cout << "\ttest 3: Compare IndexNestedLoops with TupleNestedLoops."
		      << std::endl;
	std::cout << "\ttest 4: Compare SortMerge with TupleNestedLoops."
		      << std::endl;
	std::cout << "seed <num>: Seeds the random number generator" << std::endl;
	std::cout << "quit" << std::endl;
}


int main()
{
	printHelp();
	JoinTest::RunTests(std::cin, 5000, 200);
	return 0;
}
