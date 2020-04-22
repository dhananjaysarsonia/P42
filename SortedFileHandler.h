//
//  SortedFileHandler.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef SortedFileHandler_h
#define SortedFileHandler_h

#include <stdio.h>
#include "FileHandler.h"
#include "HeapHandler.h"


class SortedFileHandler : public FileHandler {
//this class extends filehandler and sorts the elements
private:
    //used to sort the order
    OrderMaker *order;
    //used to query the records later
    OrderMaker *query;
    //bigq object to perform operations related to sorted file. Like before we will work with pipes through this
    BigQ *bigq;
    //input and output pipe to fetch and feed data
    Pipe *inPipe, *outPipe;
                                     
    //size of the Runs in pages
    int runLength;
    //size of the pipes
    int buffsize;

public:
    
    SortedFileHandler (OrderMaker *order, int runLength);
    ~SortedFileHandler ();
    
    int Create (const char *fpath);
    int Open (char *fpath);
    int Close ();
    
    void Load (Schema &myschema, const char *loadpath);
    
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    
    int GetNextWithCNF (Record &fetchme, CNF &cnf, Record &literal);
    
    int GetNextInSequence (Record &fetchme, CNF &cnf, Record &literal);
    
    int BinarySearchInSorted(Record &fetchme, CNF &cnf, Record &literal);
    
    void startSortingThread ();
    
    void FlushPipeToFile ();
    
    int NewOrderGenerator (OrderMaker &query, OrderMaker &order, CNF &cnf);
    
};









#endif /* SortedFileHandler_h */
