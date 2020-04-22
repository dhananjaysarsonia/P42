//
//  RecordComparator.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 2/16/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "RecordComparator.h"
RecordComparator :: RecordComparator (OrderMaker *order) {
    //Record Comparator is a custom comparator used to compare records in the record vector when sort is called
    sortorder = order;
    
    
}

bool RecordComparator::operator() (Record* left, Record* right) {
    //comparison engine used to compare the elements in the comparator
    ComparisonEngine engine;

    
    if (engine.Compare (left, right, sortorder) < 0) {
        
        return true;
        
    } else {
        
        return false;
        
    }
    
}
