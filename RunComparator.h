//
//  RunComparator.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef RunComparator_h
#define RunComparator_h

#include <stdio.h>
#include "Run.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
class RunComparator {
    
private:
    
    OrderMaker *sortorder;
    
public:
    
    bool operator() (Run* left, Run* right);
    
};


#endif /* RunComparator_h */
