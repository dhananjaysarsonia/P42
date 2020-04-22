//
//  RunComparator.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 2/16/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "RunComparator.h"


bool RunComparator :: operator() (Run* left, Run* right) {
    //Custom comparator to compare runs. The comparator compares the top elements of the sorted runs and return results according to that
    ComparisonEngine engine;
    
    if (engine.Compare (left->currentRecord, right->currentRecord, left->sortedOrder) < 0) {
        
        return false;
        
    } else {
        
        return true;
        
    }
    
}
