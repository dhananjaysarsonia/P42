#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "FileHandler.h"
#include "SortedFileHandler.h"
#include "HeapHandler.h"


typedef enum {heap, sorted} fType;

class DBFile {
	
private:
	//this is our filehandler with virtual functions
    //we have overriden it's methods based on the file types in different classes.
	FileHandler *fileHandler;         

public:
	
	DBFile ();
	~DBFile ();
	
	int Create (const char *fpath, fType ftype, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};


class SortBus {
public:
    
    OrderMaker *myOrder;
    int runLength;
    
};

#endif
