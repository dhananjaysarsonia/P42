#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"

extern "C" {
    int yyparse (void);   // defined in y.tab.c
}

using namespace std;

extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect; // attributes to select
extern struct FuncOperator *finalFunction; // To be used to aggregate
extern struct TableList *tables; //list of tables
extern int distinctFunc;



//extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
//extern struct TableList *tables; // the list of tables and aliases in the query
//extern struct AndList *boolean; // the predicate in the WHERE clause
//extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
//extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
//extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
//extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query







//approximation of number of rows of our table
const int n_customer = 150000;
const int n_lineitem = 6001215;
const int n_nation = 25;
const int n_orders = 1500000;
const int n_part = 200000;
const int n_partsupp = 800000;
const int n_region = 5;
const int n_supplier = 10000;

//names of our tables
char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";


static int pipeId = 0;
int getPid () {
    
    return ++pipeId;
    
}

enum NodeType {
     SelectFileType, SelectPipeType, ProjectionType, DistinctType, SumType, GroupByType, JoinType
};

class QueryNode {

public:
    
    int pipeId;
    
    NodeType nodeType;
    Schema schema;
    
    QueryNode ();
    QueryNode (NodeType type) : nodeType (type) {}
    
    ~QueryNode () {}
    virtual void PrintNodes () {};
    
};

class JoinNode : public QueryNode {

public:
    
    QueryNode *l;
    QueryNode *r;
    CNF cnf;
Record recordLiteral;
    
    JoinNode () : QueryNode (JoinType) {}
    ~JoinNode () {
        
        if (l) delete l;
        if (r) delete r;
        
    }
    
    void PrintNodes () {
        l->PrintNodes ();
        r->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Join Operation" << endl;
        cout << "Left Input Pipe: " << l->pipeId << endl;
        cout << "Right Input Pipe: " << r->pipeId << endl;
        cout << "Output Pipe: " << pipeId << endl;
        cout << "Output Schema: " << endl;
        schema.Print ();
        cout << "CNF : " << endl;
        cnf.PrintWithSchema(&l->schema,&r->schema,&recordLiteral);
        cout << "*********************" << endl;
        
    }
    
};

class ProjectNode : public QueryNode {

public:
    
    int numAttrsInput;
    int numAttrsOutput;
    int *attrsToKeep;
    
    QueryNode *fromNode;
    
    ProjectNode () : QueryNode (ProjectionType) {}
    ~ProjectNode () {
        
        if (attrsToKeep) delete[] attrsToKeep;
        
    }
    
    void PrintNodes () {
        fromNode->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Project Operation" << endl;
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        cout << "Output Pipe" << pipeId << endl;
        cout << "NumAtts: " << numAttrsInput << endl;
        cout << "Atts To Keep :" << endl;
        for (int i = 0; i < numAttrsOutput; i++) {
            
            cout << attrsToKeep[i] << endl;
            
        }
        cout << "Number Attrs Output : " << numAttrsOutput << endl;
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "*********************" << endl;
        
        
    }
    
};

class SelectFileNode : public QueryNode {

public:
    
    bool isOpen;
    
    CNF cnf;
    DBFile dbfile;
    Record recLiteral;
    
    SelectFileNode () : QueryNode (SelectFileType) {}
    ~SelectFileNode () {
        if (isOpen) {
            dbfile.Close ();
        }
    }
    void PrintNodes () {
        cout << "*********************" << endl;
        cout << "Select File Operation" << endl;
        cout << "Output Pipe " << pipeId << endl;
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "CNF:" << endl;
        cnf.PrintWithSchema(&schema,&schema,&recLiteral);
        cout << "*********************" << endl;
        
    }
    
};


class SumNode : public QueryNode {

public:
    
    Function funcCompute;
    QueryNode *fromNode;
    
    SumNode () : QueryNode (SumType) {}
    ~SumNode () {
        
        if (fromNode) delete fromNode;
        
    }
    
    void PrintNodes () {
        
        fromNode->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Sum Operation" << endl;
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        cout << "Output Pipe: " << pipeId << endl;
        cout << "Function:" << endl;
        funcCompute.Print ();
        cout << "*********************" << endl;
    }
};

class DistinctNode : public QueryNode {

public:
    QueryNode *fromNode;
    DistinctNode () : QueryNode (DistinctType) {}
    ~DistinctNode () {
        if (fromNode) delete fromNode;
    }
    void PrintNodes () {
        fromNode->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Duplication Elimation Operation" << endl;
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        cout << "Output Pipe: " << pipeId << endl;
        cout << "*********************" << endl;
    }
};

class GroupByNode : public QueryNode {

public:
    
    QueryNode *from;
    
    Function computeFunc;
    OrderMaker group;
    
    GroupByNode () : QueryNode (GroupByType) {}
    ~GroupByNode () {
        
        if (from) delete from;
        
    }
    
    void PrintNodes () {
        
        from->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Group By Operation" << endl;
        cout << "Input Pipe ID : " << from->pipeId << endl;
        cout << "Output Pipe ID : " << pipeId << endl;
        cout << "Output Schema : " << endl;
        schema.Print ();
        cout << "Function : " << endl;
        computeFunc.Print ();
        cout << "OrderMaker : " << endl;
        group.Print ();
        cout << "*********************" << endl;
            
    }
    
};



typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;

void initializeSchemaMap (SchemaMap &schemaMap) {
    
    schemaMap[string(region)] = Schema ("catalog", region);
    schemaMap[string(part)] = Schema ("catalog", part);
    schemaMap[string(partsupp)] = Schema ("catalog", partsupp);
    schemaMap[string(nation)] = Schema ("catalog", nation);
    schemaMap[string(customer)] = Schema ("catalog", customer);
    schemaMap[string(supplier)] = Schema ("catalog", supplier);
    schemaMap[string(lineitem)] = Schema ("catalog", lineitem);
    schemaMap[string(orders)] = Schema ("catalog", orders);
    
}

void initializeStat (Statistics &statist) {
    
    statist.AddRel (region, n_region);
    statist.AddRel (nation, n_nation);
    statist.AddRel (part, n_part);
    statist.AddRel (supplier, n_supplier);
    statist.AddRel (partsupp, n_partsupp);
    statist.AddRel (customer, n_customer);
    statist.AddRel (orders, n_orders);
    statist.AddRel (lineitem, n_lineitem);

    // region
    statist.AddAtt (region, "r_regionkey", n_region);
    statist.AddAtt (region, "r_name", n_region);
    statist.AddAtt (region, "r_comment", n_region);
    
    // nation
    statist.AddAtt (nation, "n_nationkey",  n_nation);
    statist.AddAtt (nation, "n_name", n_nation);
    statist.AddAtt (nation, "n_regionkey", n_region);
    statist.AddAtt (nation, "n_comment", n_nation);
    
    // part
    statist.AddAtt (part, "p_partkey", n_part);
    statist.AddAtt (part, "p_name", n_part);
    statist.AddAtt (part, "p_mfgr", n_part);
    statist.AddAtt (part, "p_brand", n_part);
    statist.AddAtt (part, "p_type", n_part);
    statist.AddAtt (part, "p_size", n_part);
    statist.AddAtt (part, "p_container", n_part);
    statist.AddAtt (part, "p_retailprice", n_part);
    statist.AddAtt (part, "p_comment", n_part);
    
    // supplier
    statist.AddAtt (supplier, "s_suppkey", n_supplier);
    statist.AddAtt (supplier, "s_name", n_supplier);
    statist.AddAtt (supplier, "s_address", n_supplier);
    statist.AddAtt (supplier, "s_nationkey", n_nation);
    statist.AddAtt (supplier, "s_phone", n_supplier);
    statist.AddAtt (supplier, "s_acctbal", n_supplier);
    statist.AddAtt (supplier, "s_comment", n_supplier);
    
    // partsupp
    statist.AddAtt (partsupp, "ps_partkey", n_part);
    statist.AddAtt (partsupp, "ps_suppkey", n_supplier);
    statist.AddAtt (partsupp, "ps_availqty", n_partsupp);
    statist.AddAtt (partsupp, "ps_supplycost", n_partsupp);
    statist.AddAtt (partsupp, "ps_comment", n_partsupp);
    
    // customer
    statist.AddAtt (customer, "c_custkey", n_customer);
    statist.AddAtt (customer, "c_name", n_customer);
    statist.AddAtt (customer, "c_address", n_customer);
    statist.AddAtt (customer, "c_nationkey", n_nation);
    statist.AddAtt (customer, "c_phone", n_customer);
    statist.AddAtt (customer, "c_acctbal", n_customer);
    statist.AddAtt (customer, "c_mktsegment", 5);
    statist.AddAtt (customer, "c_comment", n_customer);
    
    // orders
    statist.AddAtt (orders, "o_orderkey", n_orders);
    statist.AddAtt (orders, "o_custkey", n_customer);
    statist.AddAtt (orders, "o_orderstatus", 3);
    statist.AddAtt (orders, "o_totalprice", n_orders);
    statist.AddAtt (orders, "o_orderdate", n_orders);
    statist.AddAtt (orders, "o_orderpriority", 5);
    statist.AddAtt (orders, "o_clerk", n_orders);
    statist.AddAtt (orders, "o_shippriority", 1);
    statist.AddAtt (orders, "o_comment", n_orders);
    
    // lineitem
    statist.AddAtt (lineitem, "l_orderkey", n_orders);
    statist.AddAtt (lineitem, "l_partkey", n_part);
    statist.AddAtt (lineitem, "l_suppkey", n_supplier);
    statist.AddAtt (lineitem, "l_linenumber", n_lineitem);
    statist.AddAtt (lineitem, "l_quantity", n_lineitem);
    statist.AddAtt (lineitem, "l_extendedprice", n_lineitem);
    statist.AddAtt (lineitem, "l_discount", n_lineitem);
    statist.AddAtt (lineitem, "l_tax", n_lineitem);
    statist.AddAtt (lineitem, "l_returnflag", 3);
    statist.AddAtt (lineitem, "l_linestatus", 2);
    statist.AddAtt (lineitem, "l_shipdate", n_lineitem);
    statist.AddAtt (lineitem, "l_commitdate", n_lineitem);
    statist.AddAtt (lineitem, "l_receiptdate", n_lineitem);
    statist.AddAtt (lineitem, "l_shipinstruct", n_lineitem);
    statist.AddAtt (lineitem, "l_shipmode", 7);
    statist.AddAtt (lineitem, "l_comment", n_lineitem);
    
}

void CopyTbNamesAliases (TableList *tbList, Statistics &stat, vector<char *> &tbNames, AliaseMap &alMap) {
    while (tbList) {
        stat.CopyRel (tbList->tableName, tbList->aliasAs);
        alMap[tbList->aliasAs] = tbList->tableName;
        tbNames.push_back (tbList->aliasAs);
        tbList = tbList->next;
    }
}


void CopyNames(NameList *nameList, vector<string> &names) {
    while (nameList) {
        names.push_back (string (nameList->name));
        nameList = nameList->next;
    }
}

void PrintFunction (FuncOperator *funcOp) {
    if (funcOp) {
        cout << "(";

        PrintFunction (funcOp->leftOperator);
        
        cout << funcOp->leftOperand->value << " ";
        if (funcOp->code) {
            cout << " " << funcOp->code << " ";
        }
        PrintFunction (funcOp->right);
        cout << ")";
        
    }
    
}

int main () {
    
    cout << "SQL>>" << endl;;
    yyparse ();
    cout << endl;
    AliaseMap aliaseMap;
    vector<char *> tbNames;
    vector<char *> jOrder;
    vector<char *> buff (2);
    SchemaMap schemaMap;
    Statistics stat;
    
    initializeSchemaMap (schemaMap);
    initializeStat (stat);
    CopyTbNamesAliases (tables, stat, tbNames, aliaseMap);
    
    sort (tbNames.begin (), tbNames.end ());
    
    int minimum = INT_MAX;
    int currentCost = 0;
    do {
        
        Statistics temp (stat);
        
        auto tbItem = tbNames.begin ();
        buff[0] = *tbItem;
        tbItem++;
        
        while (tbItem != tbNames.end ()) {

            buff[1] = *tbItem;
            currentCost += temp.Estimate (boolean, &buff[0], 2);
            temp.Apply (boolean, &buff[0], 2);
            
            if (currentCost <= 0 || currentCost > minimum) {
                break;
            }
            tbItem++;
        }
        if (currentCost > 0 && currentCost < minimum) {
            minimum = currentCost;
            jOrder = tbNames;
        }

        currentCost = 0;
    } while (next_permutation (tbNames.begin (), tbNames.end ()));
    
    if (jOrder.size()==0){
        jOrder = tbNames;
    }
    QueryNode *rootNode;
    auto jItem = jOrder.begin ();
    SelectFileNode *selectFileNode = new SelectFileNode ();
    
    selectFileNode->isOpen = true;
    selectFileNode->pipeId = getPid ();
    selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
    selectFileNode->schema.Reset(*jItem);
    selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
    
    jItem++;
    if (jItem == jOrder.end ()) {
        
        rootNode = selectFileNode;
        
    } else {
        
        JoinNode *joinNode = new JoinNode ();
        
        joinNode->pipeId = getPid ();
        joinNode->l = selectFileNode;
        
        selectFileNode = new SelectFileNode ();
        
        selectFileNode->isOpen = true;
        selectFileNode->pipeId = getPid ();
        selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
        
        selectFileNode->schema.Reset (*jItem);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
        
        joinNode->r = selectFileNode;
        joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
        joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);
        
        jItem++;
        
        while (jItem != jOrder.end ()) {
            
            JoinNode *p = joinNode;
            
            selectFileNode = new SelectFileNode ();
            selectFileNode->isOpen = true;
            selectFileNode->pipeId = getPid ();
            selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
            selectFileNode->schema.Reset (*jItem);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
            
            joinNode = new JoinNode ();
            
            joinNode->pipeId = getPid ();
            joinNode->l = p;
            joinNode->r = selectFileNode;
            
            joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);
            
            jItem++;
            
        }
        
        rootNode = joinNode;
        
    }
    
    QueryNode *temp = rootNode;
    
    if (groupingAtts) {
        
        if (distinctFunc) {
            rootNode = new DistinctNode ();
            rootNode->pipeId = getPid ();
            rootNode->schema = temp->schema;
            ((DistinctNode *) rootNode)->fromNode = temp;
            temp = rootNode;

        }
        
        rootNode = new GroupByNode ();
        
        vector<string> groupAtts;
        CopyNames (groupingAtts, groupAtts);
        
        rootNode->pipeId = getPid ();
        ((GroupByNode *) rootNode)->computeFunc.GrowFromParseTree (finalFunction, temp->schema);
        rootNode->schema.GroupBySchema (temp->schema, ((GroupByNode *) rootNode)->computeFunc.ReturnInt ());
        ((GroupByNode *) rootNode)->group.growFromParseTree (groupingAtts, &(rootNode->schema));
        
        ((GroupByNode *) rootNode)->from = temp;
        
    } else if (finalFunction) {
        
        rootNode = new SumNode ();
        
        rootNode->pipeId = getPid ();
        ((SumNode *) rootNode)->funcCompute.GrowFromParseTree (finalFunction, temp->schema);
        
        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        rootNode->schema = Schema (NULL, 1, ((SumNode *) rootNode)->funcCompute.ReturnInt () ? atts[0] : atts[1]);
        
        ((SumNode *) rootNode)->fromNode = temp;
        
    }
    else if (attsToSelect) {
        
        rootNode = new ProjectNode ();
        
        vector<int> attsToKeep;
        vector<string> atts;
        CopyNames (attsToSelect, atts);
        
        rootNode->pipeId = getPid ();
        rootNode->schema.ProjectSchema (temp->schema, atts, attsToKeep);
        ((ProjectNode *) rootNode)->attrsToKeep = &attsToKeep[0];
        ((ProjectNode *) rootNode)->numAttrsOutput = atts.size ();
        ((ProjectNode *) rootNode)->numAttrsInput = temp->schema.GetNumAtts ();
       
        ((ProjectNode *) rootNode)->fromNode = temp;
        
    }
    cout << "Parse Tree : " << endl;
    rootNode->PrintNodes ();
    
    return 0;
    
}
