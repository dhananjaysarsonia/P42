#include "Statistics.h"


Statistics :: Statistics () {}

Statistics :: Statistics (Statistics &copyMe) {
	//it will create a copy
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
	
}

Statistics :: ~Statistics () {}

Statistics Statistics :: operator= (Statistics &copyMe) {
	//overloading = operator
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
	return *this;
	
}

double Statistics :: AND (AndList *aList, char *rName[], int nJoin) {
    //method will perform AND operation on the list
    //returns 1 if list is null
    if(aList != NULL){
        //initialize left and right values
        double l = 1.0;
        double r = 1.0;
        //call OR for left
        l = OR (aList->left, rName, nJoin);
        //calling AND for right
        r = AND (aList->rightAnd, rName, nJoin);
        //returning product
        return l * r;
    }
	else{
		return 1.0;
	}
    
    

}

double Statistics :: OR (OrList *oList, char *rName[], int nJoin) {
    //will perform OR operation
    //this function will return 0 if list is null
    if(oList != NULL){
        //initialize l and r values
        double l = 0.0;
        double r = 0.0;
        //comparing left attributes and storing result in l
        l = COMPARE (oList->left, rName, nJoin);
        //intializing count
        int count = 1;
        //temp object or our list holder
        OrList *tem = oList->rightOr;
        //storing attribute name
        char *attributeName = oList->left->left->value;
        //iterating temp and incremening count
        while (tem) {
            if (strcmp (tem->left->left->value, attributeName) == 0) {
                count++;
            }
            tem = tem->rightOr;
        }
        //if count > 1 then return multiplication of count and l
        if (count > 1) {
            return (double) count * l;
        }else
        {
        //else it will comput the value and return
        r = OR (oList->rightOr, rName, nJoin);
        return (double) (1.0 - (1.0 - l) * (1.0 - r));
        }
        
    }
    
	else {
        
		return 0.0;
	}
	
	
	
}



int Statistics :: GetRelation (Operand *operand, char *rNames[], int nJoin, RelationHelper &relHelper) {
    //it will return -1 if operand is null
    if (operand == NULL) {
        return -1;
    }
    //it will return -1 if the relation name array is null
    else if (rNames == NULL) {
        return -1;
    }
    //iterating through the relationship map
    string strValue (operand->value);
    for (auto item = relMap.begin (); item != relMap.end (); item++) {

        //it will return 0 if the condition satisfies
        if (item->second.attrmapList.find (strValue) != item->second.attrmapList.end()) {
            relHelper = item->second;
            return 0;
        }
        
    }
    //else it will return -1
    return -1;
    
}

double Statistics :: COMPARE (ComparisonOp *compOperator, char *relName[], int nJoin) {
    //function used to compare
    //initializing left and right values to 0
	double l = 0.0;
	double r = 0.0;

    RelationHelper lRelation, rRelation;
    //first we get the relationshio for left and right operators
	int lResult = GetRelation (compOperator->left, relName, nJoin, lRelation);
	int rResult = GetRelation (compOperator->right, relName, nJoin, rRelation);

    //we get the code from the operator class
    int code = compOperator->code;
    
	if (compOperator->left->code == NAME) {
		if (lResult == -1) {
            //if the left result is -1 then we throw the relation not found error
			cout << compOperator->left->value << Util::RELATION_NOT_FOUND_ERROR << endl;
			l = 1.0;
		} else {
            //else we take out distinct tuples
			string buffer (compOperator->left->value);
			l = lRelation.attrmapList[buffer].distinctTuples;
		}
		
	} else {
        //we return -1 otherwise
		l = -1.0;
	}
	//if the right code is Name type, then it will throw an error if result is -1
	if (compOperator->right->code == NAME) {
		
		if (rResult == -1) {
			
			cout << compOperator->right->value << Util::RELATION_NOT_FOUND_ERROR << endl;
			r = 1.0;
			
		} else {
            //else it convert the value into string and get distinc tuples
			string strValue (compOperator->right->value);
			r = rRelation.attrmapList[strValue].distinctTuples;
		}
	} else {
		r = -1.0;
	}
	//if code is less than or greater than, then divide and return
	if (code == LESS_THAN || code == GREATER_THAN) {
		return 1.0 / 3.0;
		
        // if it is equal, then divide 1 bu l or r, depending which is greater
	} else if (code == EQUALS) {
		if (l > r) {
			return 1.0 / l;
		} else {
			return 1.0 / r;
		}
	}
	cout << Util::ERROR << endl;
	return 0.0;
}


void Statistics :: AddRel (char *relName, int numTuples) {
	//add releationship adds another relationship
	string strName (relName);
	
	RelationHelper rHelper(strName, numTuples);
	//it will map the tuple with the relationship name.
	relMap[strName] = rHelper;
	
}

void Statistics :: AddAtt (char *relName, char *attrName, int numDistincts) {
	//add attribute whill add attributes to our relationship name
    //depending on the name of the relationship, it will add attribute against the name in our dictionary
	string stringName (relName);
	string stringAttr (attrName);
	
	AttributeHelper attributeHelper(stringAttr, numDistincts);
	
	relMap[stringName].attrmapList[stringAttr] = attributeHelper;
	
}

void Statistics :: CopyRel (char *oldName, char *newName) {
	//this creates a copy of the relationsp with a new name
	string stringOld (oldName);
	string stringNew (newName);
	
	relMap[stringNew] = relMap[stringOld];
	relMap[stringNew].relationsName = stringNew;
	
	AttrMap newAttrMap;
	//we will manually iterate through the old relationship and add it to a new relationship name
	for(auto item = relMap[stringNew].attrmapList.begin (); item != relMap[stringNew].attrmapList.end (); item++) {
		string newAttributeStr = stringNew;
		newAttributeStr.append (".");
		newAttributeStr.append(item->first);
        
		AttributeHelper temp (item->second);
		temp.attrName = newAttributeStr;
		newAttrMap[newAttributeStr] = temp;
	}
    //adding the new relationship to our map
	relMap[stringNew].attrmapList = newAttrMap;
	
}
	
void Statistics :: Read (char *fromWhere) {
	//we read the attribute file from the file name given
    int n_attributes;
    int n_rel;
    int n_joins;
    int n_tuples;
    int n_distincts;
    string relName;
    string jointName;
    string attrName;
	//we intialize instream object from the path
	ifstream inStream (fromWhere);
	//if instream is null the we print an error that the file does not exists
	if (!inStream) {
		cout << fromWhere << "\" does not exist!" << endl;
	}
	//we clear our relationship mapping dictionary
	relMap.clear ();
	//we read first value. First value is number of relationships
	inStream >> n_rel;
	//based on the value of the number of relationship we loop through the lines to get the relationships
	for (int i = 0; i < n_rel; i++) {
		//here based on the number of relationships we read line by line
		inStream >> relName;
		inStream >> n_tuples;
		//we store the number of tuples
        //we intialize our relationship helper
		RelationHelper relation (relName, n_tuples);
        //we map the relation
		relMap[relName] = relation;
		//we check if the relelation cosnsts of join
		inStream >> relMap[relName].joinFlag;
		//if it does consist of join then we read the join attributes
		if (relMap[relName].joinFlag) {
			
			inStream >> n_joins;
			for (int j = 0; j < n_joins; j++) {
				inStream >> jointName;
				relMap[relName].rJoin[jointName] = jointName;
			}
		}
		
        //we read the number of attributes, and loop through to read the name of the attributes and distinct values in them
		inStream >> n_attributes;
        int index = 0;
		while (index < n_attributes) {
			inStream >> attrName;
			inStream >> n_distincts;
			AttributeHelper attrInfo (attrName, n_distincts);
			relMap[relName].attrmapList[attrName] = attrInfo;
            index++;
		}
		
	}
	
}

void Statistics :: Write (char *toWhere) {
	//here we write our file, this is were we right our statistics file
    //first we get the name and initialize offstream
	ofstream out (toWhere);
	//we output number of relationship first
	out << relMap.size () << endl;
	
	for (auto item = relMap.begin (); item != relMap.end (); item++) {
		//for each relationship we out the name
		out << item->second.relationsName << endl;
        //then we put the number of tuples in the relationshop
        out << item->second.numOfTuples << endl;
        //then we turn on the join flag
		out << item->second.joinFlag << endl;
		//if the joinflag was true
		if (item->second.joinFlag) {
			//we get the size of the join
			out << item->second.rJoin.size () << endl;
			//we iterate through to get the number of relationship the attribute is joined with
			for (auto it = item->second.rJoin.begin (); it != item->second.rJoin.end (); it++) {
				out << it->second << endl;
			}
		}
		//we get the number of attributes in the relation
		out << item->second.attrmapList.size () << endl;
		for (
             //we iterate through the attributes and take out the distinct number of values in the attribute
			auto it = item->second.attrmapList.begin ();
			it != item->second.attrmapList.end ();
			it++
		) {
			
			out << it->second.attrName << endl;
			out << it->second.distinctTuples << endl;
			
		}
		
	}
    //we close the file
	
	out.close ();
	
}

void Statistics :: Apply (struct AndList *parseTree, char *relNames[], int numToJoin) {
    //we intialize number of joins to zero
	int numJoin = 0;
    char *names[100];
	int iterIndex = 0;
	
	RelationHelper rHelper;
	//we start from 0 to number to join
	while (iterIndex < numToJoin) {
		string strNames (relNames[iterIndex]);
		auto item = relMap.find (strNames);
        
        
//here we validate if the koin can be performed. We through error otherwise
		if (item != relMap.end ()) {
			rHelper = item->second;
			names[numJoin++] = relNames[iterIndex];
			if (rHelper.joinFlag) {
				int s = rHelper.rJoin.size();
				if (s <= numToJoin) {
					for (int i = 0; i < numToJoin; i++) {
						string buf (relNames[i]);
						if (rHelper.rJoin.find (buf) == rHelper.rJoin.end () &&
							rHelper.rJoin[buf] != rHelper.rJoin[strNames]) {
                            //if true then thow an error
							cout << Util::JOIN_ERROR << endl;
							return;
						}
					}
				} else {
					cout <<Util::JOIN_ERROR<< endl;
				}
			} else {
				iterIndex++;
				continue;
			}
		}
        //keep on incrementing index to get attribute from next relation
		iterIndex++;
	}
	//estimate will calculate the number of tuples that would result from a join
	double estimation = Estimate (parseTree, names, numJoin);
	
	iterIndex = 1;
	string firstRelName (names[0]);
	RelationHelper firstRel = relMap[firstRelName];
	RelationHelper temp;
	//here we insert into first relation name. We save the name of the relationship and perform join operatin
	relMap.erase (firstRelName);
    //we turn on the join flag for the relationship
	firstRel.joinFlag = true;
    //we save the number of tuples in the relationshop
	firstRel.numOfTuples = estimation;
//we insert attributes into our relationship of join operation
	while (iterIndex < numJoin) {
		string buffer (names[iterIndex]);
		firstRel.rJoin[buffer] = buffer;
		temp = relMap[buffer];
		relMap.erase (buffer);
		firstRel.attrmapList.insert (temp.attrmapList.begin (), temp.attrmapList.end ());
		iterIndex++;
		
	}
	//we insert the new joined relation in the relationship map
	relMap.insert (pair<string, RelationHelper> (firstRelName, firstRel));
	
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
	double f = 1.0;
    double r = 1.0;
	int iterIndex = 0;
	//estimate helps us to perform join operation without any modification
	while (iterIndex < numToJoin) {
		//for each relationship we calculate the product
        string buffer (relNames[iterIndex]);
        
		if (relMap.find (buffer) != relMap.end ()) {
			r *= (double) relMap[buffer].numOfTuples;
		}
        //then we increment the index
		iterIndex++;
	}
    //we return the product directly if parsetree is null
	if (parseTree == NULL) {
		return r;
	}
	//otherwise we perform AND operation, and return multiplication of factor and product
	f = AND (parseTree, relNames, numToJoin);
	return f * r;
	
}
