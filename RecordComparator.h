//
//  RecordComparator.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef RecordComparator_h
#define RecordComparator_h

#include <stdio.h>
#include "Comparison.h"
#include "ComparisonEngine.h"


class RecordComparator {
    
private:
    
    OrderMaker *sortorder;
    
    
public:
    
    RecordComparator (OrderMaker *order);
    bool operator() (Record* left, Record* right);
    
};

#endif /* RecordComparator_h */
