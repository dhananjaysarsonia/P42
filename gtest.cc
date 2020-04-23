#include "gtest.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Comparison.h"
//header file consisting of the definition of our nodes
#include "NodeHandler.h"



extern "C" {
    int yyparse (void);   // defined in y.tab.c
}

using namespace std;

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


bool isFinished = false;
bool isRootNotNull = false;
bool isTableNotEmpty = false;
typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;





void FindMinCostJoin( vector<char *> &jOrder, vector<char *> &tbNames, Statistics &stat, vector<char *> &buff){
      int minimum = INT_MAX;
      int currentCost = 0;
      do {
          Statistics temp (stat);

          auto tbItem = tbNames.begin ();
          buff[0] = *tbItem;
          tbItem++;
//          for(int i = 0 ; i < tbNames.size(); i++){
//
//              cout << tbNames[i] << ", ";
//          }
//
        //  cout << endl;
          while (tbItem != tbNames.end ()) {

              buff[1] = *tbItem;
              currentCost += temp.Estimate (boolean, &buff[0], 2);
              temp.Apply (boolean, &buff[0], 2);
//             cout << "Buffer 0" << buff[0] << endl;
//             cout << "Buffer 1" << buff[1] << endl;
//              cout << currentCost <<endl;
              if (currentCost <= 0 || currentCost > minimum) {
                  break;
              }
              tbItem++;
          }
          if (currentCost > 0 && currentCost < minimum) {
              minimum = currentCost;
              jOrder = tbNames;
          //    cout << "Minumum Found" << minimum << endl;
          }

          currentCost = 0;
      } while (next_permutation (tbNames.begin (), tbNames.end ()));
    
}

void printTree(BaseNode *rootNode){
    cout << "Parse Tree : " << endl;
    rootNode->PrintNodes ();
    
}

void initializeSchemaMap (SchemaMap &schemaMap) {
    //intializing schma mapping for our all the schema in dictionary
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
    //initialzing for
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



void finalRun(){
    
       cout << "SQL>>" << endl;
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
       
       FindMinCostJoin(jOrder, tbNames, stat, buff);

       
       if (jOrder.size()==0){
           jOrder = tbNames;
       }
       else{
           isTableNotEmpty = true;
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
       
       
    if(rootNode != nullptr){
        isRootNotNull = true;
    }

       printTree(rootNode);
    isFinished = true;

       
    
    
}




//
//TEST (STATS, FILE_CREATION_READING_TEST) {
//
//
//    Statistics s;
//    char *relName[] = {"supplier","partsupp"};
//
//
//    s.AddRel(relName[0],10000);
//    s.AddAtt(relName[0], "s_suppkey",10000);
//
//    s.AddRel(relName[1],800000);
//    s.AddAtt(relName[1], "ps_suppkey", 10000);
//
//    char *cnf = "(s_suppkey = ps_suppkey)";
//
//    yy_scan_string(cnf);
//    yyparse();
//    double result = s.Estimate(final, relName, 2);
//    /*
//    if(fabs(result - 800000) > 0.1) {
//        cout<< result << endl;
//        cout<<"error in estimating Q1 before apply \n ";
//    }
//    */
//
//    ASSERT_NEAR (800000, result, 0.1);
//
//    s.Apply(final, relName, 2);
//
//    // test write and read
//    s.Write(fileName);
//
//    //reload the statistics object from file
//    Statistics s1;
//    s1.Read(fileName);
//    cnf = "(s_suppkey>1000)";
//    yy_scan_string(cnf);
//    yyparse();
//    double dummy = s1.Estimate(final, relName, 2);
//    /*
//    if(fabs(dummy*3.0-result) >0.1)
//    {
//        cout<<"Read or write or last apply is not correct\n";
//    }
//    */
//
//    ASSERT_NEAR (result, dummy * 3.0, 0.1);
//
//
//}
//
//TEST (STATS, Q1) {
//
//
//    Statistics s;
//        char *relName[] = {"lineitem"};
//
//    s.AddRel(relName[0], 6001215);
//    s.AddAtt(relName[0], "l_returnflag",3);
//    s.AddAtt(relName[0], "l_discount",11);
//    s.AddAtt(relName[0], "l_shipmode",7);
//
//    char *cnf = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";
//
//    yy_scan_string(cnf);
//    yyparse();
//
//    double result = s.Estimate(final, relName, 1);
//    /*
//    cout<<"Your estimation Result  " <<result;
//    cout<<"\n Correct Answer: 8.5732e+5" << endl;
//    */
//
//    ASSERT_NEAR (8.5732e+5, result, 5.0);
//
//    s.Apply(final, relName, 1);
//
//    // test write and read
//    s.Write(fileName);
//
//
//}
//
//
//
//TEST (STATS, Q2) {
//
//
//    Statistics s;
//    char *relName[] = {"orders","customer","nation"};
//
//
//    s.AddRel(relName[0],1500000);
//    s.AddAtt(relName[0], "o_custkey",150000);
//
//    s.AddRel(relName[1],150000);
//    s.AddAtt(relName[1], "c_custkey",150000);
//    s.AddAtt(relName[1], "c_nationkey",25);
//
//    s.AddRel(relName[2],25);
//    s.AddAtt(relName[2], "n_nationkey",25);
//
//    char *cnf = "(c_custkey = o_custkey)";
//    yy_scan_string(cnf);
//    yyparse();
//
//    // Join the first two relations in relName
//    s.Apply(final, relName, 2);
//
//    cnf = " (c_nationkey = n_nationkey)";
//    yy_scan_string(cnf);
//    yyparse();
//
//    double result = s.Estimate(final, relName, 3);
//    /*
//    if(fabs(result-1500000)>0.1)
//        cout<<"error in estimating Q2\n";
//    */
//
//    ASSERT_NEAR (1500000, result, 0.1);
//
//    s.Apply(final, relName, 3);
//
//    s.Write(fileName);
//
//
//}
//
//
//TEST (STATS, Q4) {
//
//
//    Statistics s;
//    char *relName[] = { "part", "partsupp", "supplier", "nation", "region"};
//
//    s.AddRel(relName[0],200000);
//    s.AddAtt(relName[0], "p_partkey",200000);
//    s.AddAtt(relName[0], "p_size",50);
//
//    s.AddRel(relName[1], 800000);
//    s.AddAtt(relName[1], "ps_suppkey",10000);
//    s.AddAtt(relName[1], "ps_partkey", 200000);
//
//    s.AddRel(relName[2],10000);
//    s.AddAtt(relName[2], "s_suppkey",10000);
//    s.AddAtt(relName[2], "s_nationkey",25);
//
//    s.AddRel(relName[3],25);
//    s.AddAtt(relName[3], "n_nationkey",25);
//    s.AddAtt(relName[3], "n_regionkey",5);
//
//    s.AddRel(relName[4],5);
//    s.AddAtt(relName[4], "r_regionkey",5);
//    s.AddAtt(relName[4], "r_name",5);
//
//    s.CopyRel("part","p");
//    s.CopyRel("partsupp","ps");
//    s.CopyRel("supplier","s");
//    s.CopyRel("nation","n");
//    s.CopyRel("region","r");
//
//    char *cnf = "(p.p_partkey=ps.ps_partkey) AND (p.p_size = 2)";
//    yy_scan_string(cnf);
//    yyparse();
//    s.Apply(final, relName, 2);
//
//    //s.Write(fileName);
//
//    cnf ="(s.s_suppkey = ps.ps_suppkey)";
//    yy_scan_string(cnf);
//    yyparse();
//    s.Apply(final, relName, 3);
//
//    //s.Write(fileName);
//    cnf =" (s.s_nationkey = n.n_nationkey)";
//    yy_scan_string(cnf);
//    yyparse();
//    s.Apply(final, relName, 4);
//
//    cnf ="(n.n_regionkey = r.r_regionkey) AND (r.r_name = 'AMERICA') ";
//    yy_scan_string(cnf);
//    yyparse();
//
//    //s.Write(fileName);
//
//    double result = s.Estimate(final, relName, 5);
//    /*
//    if(fabs(result-3200)>0.1) {
//        cout<< result << endl;
//        cout<<"error in estimating Q4\n";
//    }
//    */
//
//    ASSERT_NEAR (3200, result, 0.1);
//
//    s.Apply(final, relName, 5);
//
//    s.Write(fileName);
//
//
//}
//




TEST (QUERY_OPTIMIZER, QUERY_RUN) {


    finalRun();
    
    
    ASSERT_EQ(isFinished, true);


}



TEST (QUERY_OPTIMIZER, ROOT_CHECK) {
    
    
    ASSERT_EQ(isRootNotNull, true);


}

TEST (QUERY_OPTIMIZER, FILTERED_JOIN_CHECK) {
    
    
    ASSERT_EQ(isTableNotEmpty, true);


}



int main(int argc, char *argv[]) {
	
	testing::InitGoogleTest(&argc, argv); 
	
	return RUN_ALL_TESTS ();
	
}
