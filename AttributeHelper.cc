//
//  AttributeHelper.cc
//  P41
//
//  Created by Dhananjay Sarsonia on 4/6/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "AttributeHelper.h"


AttributeHelper :: AttributeHelper () {}

AttributeHelper :: AttributeHelper (string name, int number) {
    
    attrName = name;
    distinctTuples = number;
    
}

AttributeHelper :: AttributeHelper (const AttributeHelper &copyMe) {
    
    attrName = copyMe.attrName;
    distinctTuples = copyMe.distinctTuples;
    
}

AttributeHelper &AttributeHelper :: operator= (const AttributeHelper &copyMe) {
    
    attrName = copyMe.attrName;
    distinctTuples = copyMe.distinctTuples;
    
    return *this;
    
}
