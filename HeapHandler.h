//
//  HeapHandler.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef HeapHandler_h
#define HeapHandler_h

#include <stdio.h>
#include "FileHandler.h"
#include <cstring>
using namespace std;


class HeapHandler : public FileHandler {
//this is our heap handler to handle heap fiels
public:
    
    HeapHandler ();
    ~HeapHandler ();
    
    int Create (const char *fpath);
    int Open (char *fpath);
    int Close ();
    
    void Load (Schema &myschema, const char *loadpath);
    
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

#endif /* HeapHandler_h */
