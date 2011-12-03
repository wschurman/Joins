#ifndef _B_TREE_FILE_H_
#define _B_TREE_FILE_H_

#include "BTreeheaderPage.h"
#include "BTreeFileScan.h"
#include "BTreeInclude.h"


#include <vector>



class BTreeFile {

public:	friend class BTreeDriver;

	BTreeFile(Status& status, const char *filename);

	Status DestroyFile();

	~BTreeFile();
	
	Status Insert(const char *key, const RecordID rid);
	//Status Delete(const char *key, const RecordID rid);

	BTreeFileScan* OpenScan(const char* lowKey, const char* highKey);

	Status PrintTree (PageID pageID, bool printContents);
	Status PrintWhole (bool printContents = false);	


private:
	char* dbname;

	BTreeHeaderPage* header;
	PageID headerID;


	Status DestroyRecursive(PageID);

	Status UpdateHeader(PageID newRootID);


/*	
	Status DeleteFromIndex(PageID pid, std::vector<PageID>& path, int pathIndex);
	Status GetSiblingsInParent(PageID parent, 
		                       PageID childID,
		                       PageID& leftSibling, 
							   PageID& rightSibling);

	bool CanBeMergedLeaf(LeafPage* left, LeafPage* right);
	bool CanBeMergedIndex(IndexPage* left, IndexPage* right, IndexPage* parent);
	Status MergeLeafPages(LeafPage* left, LeafPage* right);
	Status GetDeletePath(const char* key, RecordID val, std::vector<PageID>& path, PageID& leafPid);
	Status MergeIndexPages(IndexPage* left, IndexPage* right);
*/

	//template<typename ValType>
	//Status ScanToValue(PageID startPage, char* key, ValType val);

	Status Split(const char* key, RecordID rid, LeafPage* leafPage, std::vector<PageID>& path);
	Status SplitLeaf(const char* newKey, 
		             RecordID newRid, 
					 LeafPage* leafPage, 
					 LeafPage* newPage, 							
					 char*& splitKey);

	Status SplitIndex(const char* newKey, 
		              PageID newPid,
              		  IndexPage* leafPage,
					  IndexPage* newPage, 							
					  char* splitKey);



	Status FindLeafPage(const char* key, 
		                PageID curPid,
						PageID& leafPid,
						std::vector<PageID>& path, 
						bool rightMost);

/*	Status FindLeafPage(const char* key, 
		                PageID curPid, 
						PageID& leafPage, 
						PageID* path, 
						int depth);
*/

	PageID GetLeftLeaf();



};


#endif