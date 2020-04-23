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

//initializing pipe to zero
static int pipeId = 0;
int getPipeId () {
    //we will increment the pipeId before assigning it
    return ++pipeId;
    
}

enum NodeType {
    //enum to define NodeType.
    //ToDo: remove the enums
     SelectFileType, SelectPipeType, ProjectionType, DistinctType, SumType, GroupByType, JoinType
};

class BaseNode {
//base node class of our node. We will be other n
    
public:
    
    int pipeId;
    
    NodeType nodeType;
    Schema schema;
    
    BaseNode ();
    BaseNode (NodeType type) : nodeType (type) {}
    
    ~BaseNode () {}
    virtual void PrintNodes () {};
    
};

class NodeJoin : public BaseNode {

public:
    
    BaseNode *l;
    BaseNode *r;
    CNF cnf;
Record recordLiteral;
    
    NodeJoin () : BaseNode (JoinType) {}
    ~NodeJoin () {
        
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

class NodeProject : public BaseNode {

public:
    
    int numAttrsInput;
    int numAttrsOutput;
    int *attrsToKeep;
    
    BaseNode *fromNode;
    
    NodeProject () : BaseNode (ProjectionType) {}
    ~NodeProject () {
        
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

class NodeSelectFile : public BaseNode {

public:
    
    bool isOpen;
    
    CNF cnf;
    DBFile dbfile;
    Record recLiteral;
    
    NodeSelectFile () : BaseNode (SelectFileType) {}
    ~NodeSelectFile () {
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


class NodeSum : public BaseNode {
public:
    Function funcCompute;
    BaseNode *fromNode;
    NodeSum () : BaseNode (SumType) {}
    ~NodeSum () {
        if (fromNode) delete fromNode;
    }
    void PrintNodes () {
        fromNode->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Sum Operation" << endl;
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        cout << "Output Pipe: " << pipeId << endl;
        cout << "Function:" << endl;
        funcCompute.Print (&fromNode->schema);
        cout << "*********************" << endl;
    }
};

class NodeDistinct : public BaseNode {

public:
    BaseNode *fromNode;
    NodeDistinct () : BaseNode (DistinctType) {}
    ~NodeDistinct () {
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

class NodeGroupBy : public BaseNode {

public:
    
    BaseNode *from;
    
    Function computeFunc;
    OrderMaker group;
    
    NodeGroupBy () : BaseNode (GroupByType) {}
    ~NodeGroupBy () {
        
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
        computeFunc.Print (&from->schema);
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
    BaseNode *rootNode;
    auto jItem = jOrder.begin ();
    NodeSelectFile *selectFileNode = new NodeSelectFile ();
    
    selectFileNode->isOpen = true;
    selectFileNode->pipeId = getPipeId ();
    selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
    selectFileNode->schema.Reset(*jItem);
    selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
    
    jItem++;
    if (jItem == jOrder.end ()) {
        
        rootNode = selectFileNode;
        
    } else {
        
        NodeJoin *joinNode = new NodeJoin ();
        
        joinNode->pipeId = getPipeId ();
        joinNode->l = selectFileNode;
        
        selectFileNode = new NodeSelectFile ();
        
        selectFileNode->isOpen = true;
        selectFileNode->pipeId = getPipeId ();
        selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
        
        selectFileNode->schema.Reset (*jItem);
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
        
        joinNode->r = selectFileNode;
        joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
        joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);
        
        jItem++;
        
        while (jItem != jOrder.end ()) {
            
            NodeJoin *p = joinNode;
            
            selectFileNode = new NodeSelectFile ();
            selectFileNode->isOpen = true;
            selectFileNode->pipeId = getPipeId ();
            selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
            selectFileNode->schema.Reset (*jItem);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
            
            joinNode = new NodeJoin ();
            
            joinNode->pipeId = getPipeId ();
            joinNode->l = p;
            joinNode->r = selectFileNode;
            
            joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);
            
            jItem++;
            
        }
        
        rootNode = joinNode;
        
    }
    
    BaseNode *temp = rootNode;
    
    if (groupingAtts) {
        
        if (distinctFunc) {
            rootNode = new NodeDistinct ();
            rootNode->pipeId = getPipeId ();
            rootNode->schema = temp->schema;
            ((NodeDistinct *) rootNode)->fromNode = temp;
            temp = rootNode;

        }
        
        rootNode = new NodeGroupBy ();
        
        vector<string> groupAtts;
        CopyNames (groupingAtts, groupAtts);
        
        rootNode->pipeId = getPipeId ();
        ((NodeGroupBy *) rootNode)->computeFunc.GrowFromParseTree (finalFunction, temp->schema);
        
        rootNode->schema.GroupBySchema (temp->schema, ((NodeGroupBy *) rootNode)->computeFunc.ReturnInt (), groupAtts);
        
        
        ((NodeGroupBy *) rootNode)->group.growFromParseTree (groupingAtts, &(temp->schema));
        
        ((NodeGroupBy *) rootNode)->from = temp;
        
    } else if (finalFunction) {
        
        rootNode = new NodeSum ();
        
        rootNode->pipeId = getPipeId ();
        ((NodeSum *) rootNode)->funcCompute.GrowFromParseTree (finalFunction, temp->schema);
        
        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        rootNode->schema = Schema (NULL, 1, ((NodeSum *) rootNode)->funcCompute.ReturnInt () ? atts[0] : atts[1]);
        
        ((NodeSum *) rootNode)->fromNode = temp;
        
    }
    else if (attsToSelect) {
        
        rootNode = new NodeProject ();
        vector<int> attsToKeep;
        vector<string> atts;
        CopyNames (attsToSelect, atts);
        
        rootNode->pipeId = getPipeId ();
        rootNode->schema.ProjectSchema (temp->schema, atts, attsToKeep);
        ((NodeProject *) rootNode)->attrsToKeep = &attsToKeep[0];
        ((NodeProject *) rootNode)->numAttrsOutput = atts.size ();
        ((NodeProject *) rootNode)->numAttrsInput = temp->schema.GetNumAtts ();
       
        ((NodeProject *) rootNode)->fromNode = temp;
        
    }
    cout << "Parse Tree : " << endl;
    rootNode->PrintNodes ();
    
    return 0;
    
}
