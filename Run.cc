//
//  Run.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "Run.h"


Run :: Run (int run_length, int page_offset, File *file, OrderMaker* order) {
    //we store the length of the run
    runSize = run_length;
    pOffset = page_offset;
    currentRecord = new Record ();
    runsFile = file;
    sortedOrder = order;
    runsFile->GetPage (&currentPage, pOffset);
    GetFirstRecord ();
    
}

Run :: Run (File *file, OrderMaker *order) {
    //Run constructor
    //saves current record as NULL. It will be generated later
    currentRecord = NULL;
    //file object where run is stored
    runsFile = file;
    //the order through which the file was sorted
    sortedOrder = order;
    
}

Run :: ~Run () {
    //freeing up the record memory when destructor is called
    delete currentRecord;
    
}

int Run :: GetFirstRecord () {
    //we return 0 when the Run size has reached 0, indicating there are no more records to return from the run
    if(runSize <= 0) {
        return 0;
    }
    //if not then we create a new record
    Record* record = new Record();
    
    //GetFirst returns 0 when the page is reached it's end
    if (currentPage.GetFirst(record) == 0) {
        pOffset++;
        //we increment the page number, and check the next page
        runsFile->GetPage(&currentPage, pOffset);
        currentPage.GetFirst(record);
    }
    //we decrement the runSize
    runSize--;
    //we copy the record 
    currentRecord->Consume(record);
    
    return 1;
}
