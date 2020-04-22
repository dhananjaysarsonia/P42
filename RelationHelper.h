//
//  RelationHelper.hpp
//  P41
//
//  Created by Dhananjay Sarsonia on 4/6/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef RelationHelper_h
#define RelationHelper_h


#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <string>
#include <fstream>
#include "AttributeHelper.h"
#include <iostream>
using namespace std;
typedef map<string, AttributeHelper> AttrMap;
//typedef map<string, RelationHelper> RelMap;

class RelationHelper {

public:
    
    double numOfTuples;
    
    bool joinFlag;
    string relationsName;
    
    AttrMap attrmapList;
    map<string, string> rJoin;
    
    RelationHelper ();
    RelationHelper (string name, int tuples);
    RelationHelper (const RelationHelper &copyMe);
    
    RelationHelper &operator= (const RelationHelper &copyMe);
    
    bool isRelationPresent (string relName);
    
};

#endif /* RelationHelper_h */
