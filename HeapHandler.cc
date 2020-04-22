//
//  HeapHandler.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "HeapHandler.h"

HeapHandler :: HeapHandler () {
    
    file = new File ();
    currentPage = new Page ();
    
}

HeapHandler :: ~HeapHandler () {
    //destructor to free up the memory
    delete currentPage;
    delete file;
    
}

int HeapHandler :: Create (const char *fpath) {
    //while creating we set the writing flag to true
    writingMode = true;
    //we copy the file path as some of our methods won't accept const char
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    //we set the page index to 0
    pIndex = 0;
    //we flush the page
    currentPage->EmptyItOut ();
    //we open the file using the starter code.
    file->Open (0, this->fpath);
    
    return 1;
    
}

void HeapHandler :: Load (Schema &f_schema, const char *loadpath) {
    //we load the tbl file from the path provided
    FILE *tblfile = fopen(loadpath, "r");
    //we create a temp record object
    Record temp;
    //if file is null then then we return error
    if (tblfile == NULL) {
        
        cout << "Sorry! the file doesn't exist. Please check the path " << loadpath << "!" << endl;
        //exit the program
        exit (0);
        
    }
    //if file is found we reset page index
    pIndex = 0;
    //we flush out our page object
    currentPage->EmptyItOut ();
    //we sucl the records from the file
    while (temp.SuckNextRecord (&f_schema, tblfile) == 1)
        Add (temp);
    
    file->AddPage (currentPage, pIndex);
    //we flush out our page
    currentPage->EmptyItOut ();
    //close the file
    fclose (tblfile);
    
}

int HeapHandler :: Open (char *fpath) {
    //we copy the path
    this->fpath = new char[100];
    
    strcpy (this->fpath, fpath);
    //set the page index to 0
    pIndex = 0;
    //flush out the current page
    currentPage->EmptyItOut ();
    //call normal open from the starter code
    file->Open (1, this->fpath);
    
    return 1;
    
}

void HeapHandler :: MoveFirst () {
    //if the file was in writing mode, and the page has records
    if (writingMode && currentPage->GetNumRecs () > 0) {
        //we flush out the page and reset writingmode to false
        file->AddPage (currentPage, pIndex++);
        writingMode = false;
        
    }
    //we flush out the page
    currentPage->EmptyItOut ();
    //we set the pageindex to 0
    pIndex = 0;
    //we get the page
    file->GetPage (currentPage, pIndex);
    
}

int HeapHandler :: Close () {
    //if file was in writing mode and page had records then we first flush the page
    if (writingMode && currentPage->GetNumRecs () > 0) {
        
        file->AddPage (currentPage, pIndex++);
        currentPage->EmptyItOut ();
        writingMode = false;
        
    }
    //we close the file
    file->Close ();
    
    return 1;
    
}

void HeapHandler :: Add (Record &rec) {
    //we add record
    if (! (currentPage->Append (&rec))) {
        //if the page is full
        //then add another page
        file->AddPage (currentPage, pIndex++);
        //flush out the page and append records
        currentPage->EmptyItOut ();
        currentPage->Append (&rec);
        
    }
    
}

int HeapHandler :: GetNext (Record &fetchme) {
    
    if (currentPage->GetFirst (&fetchme)) {
        //if there was record in the currentpage then return 1
        return 1;
        
    } else {
        //if there was no record, then increment the pageindex and check another page
        pIndex++;
        if (pIndex < file->GetLength () - 1) {
            //return the record from the new page if there are more page
            file->GetPage (currentPage, pIndex);
            currentPage->GetFirst (&fetchme);
            
            return 1;

        } else {
            //otherwise return null

            return 0;
            
        }
        
    }
    
}

int HeapHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine comp;
    
    while (GetNext (fetchme)) {
        
        if (comp.Compare (&fetchme, &literal, &cnf)){
            
            return 1;
            
        }
        
    }
    
    return 0;
    
}
