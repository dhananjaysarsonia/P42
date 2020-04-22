//
//  AttributeHelper.hpp
//  P41
//
//  Created by Dhananjay Sarsonia on 4/6/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef AttributeHelper_h
#define AttributeHelper_h

#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
using namespace std;
//typedef map<string, AttributeHelper> AttrMap;
//typedef map<string, RelationHelper> RelMap;
class AttributeHelper {

public:
    
    string attrName;
    int distinctTuples;
    
    AttributeHelper ();
    AttributeHelper (string name, int num);
    AttributeHelper (const AttributeHelper &copyMe);
    
    AttributeHelper &operator= (const AttributeHelper &copyMe);
    
};




#endif /* AttributeHelper_h */
