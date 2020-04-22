#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include "Util.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <string>
#include <fstream>
#include <iostream>
#include "AttributeHelper.h"
#include "RelationHelper.h"
using namespace std;

//class AttributeHelper;
//class RelationHelper;

//typedef map<string, AttributeHelper> AttrMap;
typedef map<string, RelationHelper> RelMap;

//class AttributeHelper {
//
//public:
//
//	string attrName;
//	int distinctTuples;
//
//	AttributeHelper ();
//	AttributeHelper (string name, int num);
//	AttributeHelper (const AttributeHelper &copyMe);
//
//	AttributeHelper &operator= (const AttributeHelper &copyMe);
//
//};
//
//class RelationHelper {
//
//public:
//	
//	double numOfTuples;
//	
//	bool joinFlag;
//	string relationsName;
//	
//	AttrMap attrmapList;
//	map<string, string> rJoin;
//	
//	RelationHelper ();
//	RelationHelper (string name, int tuples);
//	RelationHelper (const RelationHelper &copyMe);
//	
//	RelationHelper &operator= (const RelationHelper &copyMe);
//	
//	bool isRelationPresent (string relName);
//	
//};

class Statistics {

private:
	
	double AND (AndList *andList, char *relName[], int numJoin);
	double OR (OrList *orList, char *relName[], int numJoin);
	double COMPARE (ComparisonOp *comOp, char *relName[], int numJoin);
	int GetRelation (Operand *operand, char *relName[], int numJoin, RelationHelper &relInfo);
	
public:
	
	RelMap relMap;
	
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();
	
	Statistics operator= (Statistics &copyMe);
	
	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attrName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *toWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	
	

};

#endif
