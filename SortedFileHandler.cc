//
//  SortedFileHandler.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "SortedFileHandler.h"



SortedFileHandler :: SortedFileHandler (OrderMaker *order, int runLength) {
    //intializing our FileHandler for sorted file
    //in this class we will be overriding various methods of FileHandler which are specific for sorted files
    this->query = NULL;
    this->bigq = NULL;
    this->buffsize = 100;
    this->file = new File ();
    this->order = order;
    this->currentPage = new Page ();
    this->runLength = runLength;
   
    
}

SortedFileHandler :: ~SortedFileHandler () {
    //we free up the memory by deleting the query, file and page objects
    delete query;
    delete file;
    delete currentPage;
    
}

int SortedFileHandler :: Create (const char *fpath) {
 //we create the sorted file
    //first we reset the writing flag
    writingMode = false;
    //we set the page index to 0
    pIndex = 0;
    //we copy the file path in our object, as some of our methods don't accept const files
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    //we flush out our page to make sure there are no dangling pages in the memory.
    currentPage->EmptyItOut ();
    //we open the file
    file->Open (0, this->fpath);

    return 1;
    
}

int SortedFileHandler :: Open (char *fpath) {
    //we reset the writing mode
    writingMode = false;
    //we reset the page index
    pIndex = 0;
    
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    //we flush out the page
    currentPage->EmptyItOut ();
    file->Open (1, this->fpath);
    //we get the page from our file
    if (file->GetLength () > 0) {
        
        file->GetPage (currentPage, pIndex);
        
    }
    
    
    return 1;
    
}

int SortedFileHandler :: Close () {
    //if our object was in writing mode then we flush our outpipe to file.
    if (writingMode) {
        
        FlushPipeToFile ();
        
    }
    //we then close the file
    file->Close ();
    return 1;
    
}

void SortedFileHandler :: Add (Record &addme) {
    //we check if we are not in writing mode
    if (!writingMode) {
        //we start intialize our bigq and start the sorting thread here
        startSortingThread();
        
    }
    //we insert the record into inpipe
    inPipe->Insert (&addme);
    
}

void SortedFileHandler :: Load (Schema &myschema, const char *loadpath) {
    //we load the tbl file from the path provided
    FILE *tableFile = fopen(loadpath, "r");
    //we create a temp recrod object
    Record temp;
    //generate error if object is null
    if (tableFile == NULL) {
        
        cout << "Sorry! Check your Path: " << loadpath << "!" << endl;
        exit (0);
        
    }
    //continue loading when file is found
    //reset the page index to 0
    pIndex = 0;
    //flush out the page to make sure there is no dangling page object
    currentPage->EmptyItOut ();
    //load the records from file
    while (temp.SuckNextRecord (&myschema, tableFile)) {
        
        Add (temp);
        
    }
    
    fclose (tableFile);
    
}

void SortedFileHandler :: MoveFirst () {
    //first check if we were in writing mode, then first flush the pipe to file
    if (writingMode) {
        
        FlushPipeToFile ();
        
    } else {
        //if not then reset the current page
        currentPage->EmptyItOut ();
        pIndex = 0;
        //and get the first page if file is not empty
        if (file->GetLength () > 0) {
            
            file->GetPage (currentPage, pIndex);
            
        }
        //delete the query object
        if (query) {
            
            delete query;
            
        }
        
    }
    
}

int SortedFileHandler :: GetNext (Record &fetchme) {
    //if file is in writing mode, then flush the pipe to file first
    if (writingMode) {
        
        FlushPipeToFile ();
        
    }
    //get the next record in page, if record is not found then try for next page
    if (currentPage->GetFirst (&fetchme)) {
        return 1;
        
    } else {
        //here we try for the next page
        pIndex++;
        //if there is no next page then then we will return, otherwise we will get the new page and then get the record
        if (pIndex < file->GetLength () - 1) {
            file->GetPage (currentPage, pIndex);
            currentPage->GetFirst (&fetchme);
            
            return 1;

        } else {
            // if already reach EOF
            return 0;
            
        }
        
    }
    
}

int SortedFileHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    //here we get the next record with the cnf provided
    if (writingMode) {
        //we flush the pipe if there are records pending
        FlushPipeToFile ();
    }
    //if Ordermaker object is null then we create Ordermaker
    if (!query) {

        query = new OrderMaker;
        //here we search for the items based on the CNF
        if (NewOrderGenerator (*query, *order, cnf) > 0) {
        
            query->Print ();
            //we binary search the items asked by the user to return the records optimally
            if (BinarySearchInSorted (fetchme, cnf, literal)) {

                return 1;
                
            } else {
    
                return 0;
                
            }
            
        } else {
            //we get the next object in sequence otherwise
            return GetNextInSequence(fetchme, cnf, literal);
            
        }
        
    } else {
        
        //if the query can get no records
        if (query->numAtts == 0) {
            return GetNextInSequence (fetchme, cnf, literal);
            
        } else {
            // if the query is valid, then we continue returning next records
            return GetNextWithCNF (fetchme, cnf, literal);
            
        }
        
    }
    
}

int SortedFileHandler :: GetNextWithCNF(Record &fetchme, CNF &cnf, Record &literal) {
    //we first create our Comparison Engine object provided in the starter code, this will help us to
    //compare objects with the CNF
    ComparisonEngine engine;
    //we get the next item untill it is available
    while (GetNext (fetchme)) {
        //we compare and return the records
        if (!engine.Compare (&literal, query, &fetchme, order)){
            
            if (engine.Compare (&fetchme, &literal, &cnf)){
                
                return 1;
                
            }
        
        } else {
            
            break;
            
        }
        
    }
    
    return 0;
    
}

int SortedFileHandler :: GetNextInSequence(Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine engine;
    
    while (GetNext (fetchme)) {
        if (engine.Compare (&fetchme, &literal, &cnf)){
            return 1;
        }
    }
    
    return 0;
    
}

int SortedFileHandler :: BinarySearchInSorted(Record &fetchme, CNF &cnf, Record &literal) {
    //we binary search from our sorted files
    //we get the starting index
    off_t start = pIndex;
    //we get the end index
    off_t end = file->GetLength () - 1;
    //we put the mid as first for now, and calculate mid later on
    off_t mid = pIndex;
    Page *page = new Page;
    
    ComparisonEngine engine;
    
    while (true) {
        //we calculate the mid value
        mid = (start + end) / 2;
        //we get the middle page
        file->GetPage (page, mid);
        //we get the record of the page
        if (page->GetFirst (&fetchme)) {
            //we check if the record is greater or smaller than our query
            if (engine.Compare (&literal, query, &fetchme, order) <= 0) {
                //we move to left if the record is greater
                end = mid - 1;
                if (end <= start) break;
                
            } else {
                //we move to right if the record is smaller than our query
                start = mid + 1;
                if (end <= start) break;
                
            }
            
        } else {
            
            break;
            
        }
        
    }
    //if we get the record
    if (engine.Compare (&fetchme, &literal, &cnf)) {
        //we delete the page to freeup the memory
        delete currentPage;
        //set the index of the page
        pIndex = mid;
        currentPage = page;
        
        return 1;
        
    } else {
    
        delete page;
        
        return 0;
        
    }
    
}

void SortedFileHandler :: startSortingThread() {
    //this will initialize our bigq and start our sorting thread later
    writingMode = true;
    //we will read the data from input pipe
    inPipe = new Pipe (buffsize);
    //we will put the data to output pipe
    outPipe = new Pipe (buffsize);
    //we will create our bigq object
    bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
    
}

void SortedFileHandler :: FlushPipeToFile () {
    //we first shutdown our inpipe
    inPipe->ShutDown ();
    //reset the writing mode flag
    writingMode = false;
    //if file had object already
    if (file->GetLength () > 0) {
        
        MoveFirst ();
        
    }

    Record *recFromPipe = new Record;
    Record *recFromFile = new Record;
    
    HeapHandler *newFile = new HeapHandler;
    //we create a temp file
    newFile->Create ("bin/temp.bin");
    //we get records from the outpipe to write in our temp file
    int flagPipe = outPipe->Remove (recFromPipe);
    int flagFile = GetNext (*recFromFile);
    
    ComparisonEngine comp;
    //if both the file and pipe has existing objects
    while (flagFile && flagPipe) {
        //we compare which object is greater or smaller
        //technically we are merging the records already existing with the ones in the input pipe
        if (comp.Compare (recFromPipe, recFromFile, order) > 0) {
            //we add the record
            newFile->Add (*recFromFile);
            flagFile = GetNext (*recFromFile);
            
        } else {
        
            newFile->Add (*recFromPipe);
            flagPipe = outPipe->Remove (recFromPipe);
            
        }
        
    }
    //if file still has objects we add those records
    while (flagFile) {
        
        newFile->Add (*recFromFile);
        flagFile = GetNext (*recFromFile);
        
    }
    //if pipe still has records, then we add those
    while (flagPipe) {
        
        newFile->Add (*recFromPipe);
        flagPipe = outPipe->Remove (recFromPipe);
        
    }
    //we shutdhow the output pipe
    outPipe->ShutDown ();
    //we close our file and free up the memory
    newFile->Close ();
    delete newFile;
    
    file->Close ();
    //we rename our file to the required file from temp file
    remove (fpath);
    rename ("bin/temp.bin", fpath);
    
    file->Open (1, fpath);
    
    MoveFirst ();
    
}

int SortedFileHandler :: NewOrderGenerator(OrderMaker &query, OrderMaker &order, CNF &cnf) {
    //Query was failing order generation with default method provided
    //After a lot of discussion with others, we have made some changes and pasted here
    //the code is from the started code provided
    query.numAtts = 0;
    bool found = false;
    
    for (int i = 0; i < order.numAtts; ++i) {
        
        
        for (int j = 0; j < cnf.numAnds; ++j) {
            
            if (cnf.orLens[j] != 1) {
                
                continue;
                
            }
            
            if (cnf.orList[j][0].op != Equals) {
                
                continue;
                
            }
            
            if ((cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
               (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right) ||
               (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
               (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left)) {
                
                continue;
                
            }

            
            if (cnf.orList[j][0].operand1 == Left &&
                cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {
                
                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
                query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
                
                query.numAtts++;
                
                found = true;
                
                break;
                
            }
            
            if (cnf.orList[j][0].operand2 == Left &&
                cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {
                
                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
                query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
                
                query.numAtts++;
                
                found = true;
                
                break;
                
            }
            
        }
        
        if (!found) {
            
            break;
            
        }
        
    }
    
    return query.numAtts;
    
}



