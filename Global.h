//
//  Global.hpp
//  DBIp2
//
//  Created by Dhananjay Sarsonia on 2/17/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef Global_h
#define Global_h

#include <stdio.h>

class Global{
private:
    Global(){};
    Global(Global const&){};
    //Global& operator=(Global const&){};
    static Global* m_pInstance;
    
public:
    static Global* Instance();
    int inCounter;
    int outCounter;
    bool isRunMerge = false;
    bool isRunGenerated = false;
    bool isDuplicateDone = false;
    
    
};

#endif /* Global_h */
