//
//  RelationHelper.cpp
//  P41
//
//  Created by Dhananjay Sarsonia on 4/6/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "RelationHelper.h"


RelationHelper :: RelationHelper () {
    
    joinFlag = false;
    
}

RelationHelper :: RelationHelper (string name, int tuples) {
    
    joinFlag = false;
    relationsName = name;
    numOfTuples = tuples;
    
}

RelationHelper :: RelationHelper (const RelationHelper &copyMe) {
    
    joinFlag = copyMe.joinFlag;
    relationsName = copyMe.relationsName;
    numOfTuples = copyMe.numOfTuples;
    attrmapList.insert (copyMe.attrmapList.begin (), copyMe.attrmapList.end ());
    
}

RelationHelper &RelationHelper :: operator= (const RelationHelper &copyMe) {
    
    joinFlag = copyMe.joinFlag;
    relationsName = copyMe.relationsName;
    numOfTuples = copyMe.numOfTuples;
    attrmapList.insert (copyMe.attrmapList.begin (), copyMe.attrmapList.end ());
    
    return *this;
    
}

bool RelationHelper :: isRelationPresent (string relName) {
    
    if (relName == relName) {
        
        return true;
        
    }
    
    auto it = rJoin.find(relName);
    
    if (it != rJoin.end ()) {
        
        return true;
        
    }
    
    return false;
    
}
