//
//  FileHandler.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef FileHandler_h
#define FileHandler_h

#include <stdio.h>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

class FileHandler {

    
//we have modified our Filehandler from ealier submission to support virtual function now
//now this method will be inherited to HeapHandler or SortedFileHandler and the functionality will be overridden
//by those classes as required
protected:
    
    File *file;
    Page *currentPage;
    
    char *fpath;
    bool writingMode;
    
    off_t pIndex;
    
public:
    //virtual functions which can be overridden later
    virtual int Create (const char *fpath) = 0;
    virtual int Open (char *fpath) = 0;
    virtual int Close () = 0;
    
    virtual void Load (Schema &myschema, const char *loadpath) = 0;
    
    virtual void MoveFirst () = 0;
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
    virtual ~FileHandler ();
    
};

#endif /* FileHandler_h */
