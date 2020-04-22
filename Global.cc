//
//  Global.cpp
//  DBIp2
//
//  Created by Dhananjay Sarsonia on 2/17/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "Global.h"
Global* Global::m_pInstance = NULL;

Global* Global::Instance(){
    if(!m_pInstance)
    {
        m_pInstance = new Global;
        m_pInstance->inCounter = 0;
        m_pInstance->outCounter = 0;
        
    }
    return m_pInstance;
}
