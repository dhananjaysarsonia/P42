//
//  Util.cpp
//  DBI P1
//
//  Created by Dhananjay Sarsonia on 2/1/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include <stdio.h>
#include "iostream"
#include <cstdio>
#include "string"
using namespace std;
namespace Util{
static const string RELATION_NOT_FOUND_ERROR = "Sorry Relationship not found";
static const string ERROR = "Error";
static const string JOIN_ERROR = "Join error";
bool static checkIfFileExists(const std::string& path)
{
    //function citation
    //https://stackoverflow.com/questions/12774207/fastest-way-to-check-if-a-file-exist-using-standard-c-c11-c
    //it opens file in read only mode, if the file exists then it returns true, otherwise false
    if(FILE *file = fopen(path.c_str(),"r"))
    {
        fclose(file);
        return true;
    }
    return false;
    
}

}
