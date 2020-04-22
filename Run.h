//
//  Run.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef Run_h
#define Run_h

#include <stdio.h>
#include "File.h"


class Run {
    
private:
    
    Page currentPage;
    
public:
    
    Record* currentRecord;
    OrderMaker* sortedOrder;
    File *runsFile;
    
    int pOffset;
    int runSize;
    
    Run (int runSize, int pageOffset, File *file, OrderMaker *order);
    Run (File *file, OrderMaker *order);
    ~Run();
    
    int GetFirstRecord();
    
};


#endif /* Run_h */
