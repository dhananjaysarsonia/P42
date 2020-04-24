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
//header file consisting of the definition of our nodes
#include "NodeHandler.h"
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
typedef map<string, AndList> booleanMap;
typedef map<string,bool> Loopkup;




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



typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;


 booleanMap JoinFilter(AndList *parseTree) {
    booleanMap bmap;
    string delimiter = ".";
    vector <string> fKey;
    AndList * head = NULL;
    AndList * root = NULL;

    // now we go through and build the comparison structure
    for (int andIndex = 0; 1; andIndex++, parseTree = parseTree->rightAnd) {
        
        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            // done
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int orIndex = 0; 1; orIndex++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                // done with parsing
                break;
            }

            // we have not run off the list, so add the current OR in!
            
            // these store the types of the two values that are found
            Type tLeft;
            Type tRight;

            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {
                if (myOr->left->right->code == NAME)
                {
                    string key1,key2;

                    // left table string
                    string lts = myOr->left->left->value;
                    string pushlts = lts.substr(0, lts.find(delimiter));

                    // right table string
                    string rts = myOr->left->right->value;
                    string pushrts = rts.substr(0, rts.find(delimiter));
                    
                    key1= pushlts+pushrts;
                    key2 = pushrts+pushlts;
                    fKey.push_back(pushlts);
                    fKey.push_back(pushrts);

                    AndList pushAndList;
                    pushAndList.left=parseTree->left;
                    pushAndList.rightAnd=NULL;
                    
                    if(head==NULL){
                        root = new AndList;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                        head = root;
                    }
                    else{
                        root->rightAnd =  new AndList;
                        root = root->rightAnd ;
                        root->left=parseTree->left;
                        root->rightAnd=NULL;
                    }
                    
                    bmap[key1] = pushAndList;
                    bmap[key2] = pushAndList;

                }
                else if (myOr->left->right->code == STRING  ||
                        myOr->left->right->code == INT      ||
                        myOr->left->right->code == DOUBLE)
                {
                    continue;
                }
                else
                {
                    cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                    //return -1;
                }
            }
            else if (myOr->left->left->code == STRING   ||
                    myOr->left->left->code == INT       ||
                    myOr->left->left->code == DOUBLE)
            {
                continue;
            }
            // catch-all case
            else
            {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                //return -1;
            }

            // now we check to make sure that there was not a type mismatch
            if (tLeft != tRight) {
                cerr<< "ERROR! Type mismatch in Boolean  "
                << myOr->left->left->value << " and "
                << myOr->left->right->value << " were found to not match.\n";
            }
        }
    }
    
    if (fKey.size()>0){
        Loopkup h;
        vector <string> keyf;
        for (int k = 0; k<fKey.size();k++){
            if (h.find(fKey[k]) == h.end()){
                keyf.push_back(fKey[k]);
                h[fKey[k]]=true;
            }
        }

        sort(keyf.begin(), keyf.end());
        do
        {
            string str="";
            for (int k = 0; k<keyf.size();k++){
                str+=keyf[k];
            }
            bmap[str] = *head;
        }while(next_permutation(keyf.begin(),keyf.end()));
    }
    return bmap;
}


void FindMinCostJoin( vector<char *> &jOrder, vector<char *> &tbNames, Statistics &stat, vector<char *> &buff){
      int minimum = INT_MAX;
      int currentCost = 0;
        booleanMap bmap = JoinFilter(boolean);

      do {
          Statistics temp (stat);
          int bfIndexer = 0;

          auto tbItem = tbNames.begin ();
          buff[bfIndexer] = *tbItem;
          tbItem++;
          bfIndexer++;
//          for(int i = 0 ; i < tbNames.size(); i++){
//
//              cout << tbNames[i] << ", ";
//          }
//
        //  cout << endl;
          while (tbItem != tbNames.end ()) {

              buff[bfIndexer] = *tbItem;
              string key = "";
              
              for ( int c = 0; c<=bfIndexer;c++){
                                key += string(buff[c]);
                            }
                            
                            if (bmap.find(key) == bmap.end()) {
                                break;
                            }
              currentCost += temp.Estimate (boolean, &buff[0], 2);
              temp.Apply (boolean, &buff[0], 2);
//             cout << "Buffer 0" << buff[0] << endl;
//             cout << "Buffer 1" << buff[1] << endl;
//              cout << currentCost <<endl;
              if (currentCost <= 0 || currentCost > minimum) {
                  break;
              }
              tbItem++;
              bfIndexer++;
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
        //here we copy the aliases of the table names
        stat.CopyRel (tbList->tableName, tbList->aliasAs);
        alMap[tbList->aliasAs] = tbList->tableName;
        tbNames.push_back (tbList->aliasAs);
        tbList = tbList->next;
    }
}


void CopyNames(NameList *nameList, vector<string> &names) {
    while (nameList) {
        //we create a copy of name into a vector
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
    //getting the sql from the user
    cout << "SQL>>" << endl;
    //parsing the sql
    yyparse ();
    cout << endl;
    //initializing alias map
    AliaseMap aliaseMap;
    vector<char *> tbNames;
    vector<char *> jOrder;
    vector<char *> buff(2);
    SchemaMap schemaMap;
    Statistics stat;
    //initializing schema map with the schemas given
    initializeSchemaMap (schemaMap);
    //adding relations to statistic object
    initializeStat (stat);
    //copying aliases of tbnames from the sql query given
    CopyTbNamesAliases (tables, stat, tbNames, aliaseMap);
        //sorting the table names
    sort (tbNames.begin (), tbNames.end ());
    //we will find the minimum cost join from the tables. FindMinCostJoin will try all the possible permutations and find the min cost combination
    //we will use the table sequence with min cost
    FindMinCostJoin(jOrder, tbNames, stat, buff);

    if (jOrder.size()==0){
        jOrder = tbNames;
    }
    BaseNode *rootNode;
    auto jItem = jOrder.begin ();
    //we are building node tree from bottom to top
    //we will first create select file node
    NodeSelectFile *selectFileNode = new NodeSelectFile ();

    
    selectFileNode->isOpen = true;
    selectFileNode->pipeId = getPipeId ();
    selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
    selectFileNode->schema.Reset(*jItem);
    selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);

    //then we check if we are working on more than one table. If we are working on single table, then we don't need to put join and we can proceed with select nodes
    jItem++;
    if (jItem == jOrder.end ()) {

        rootNode = selectFileNode;

    } else {

        //we create a join node. the join node will be the parent of our select file nodes
        NodeJoin *joinNode = new NodeJoin ();

        //we allocate pipe id to join node. Here also pipes are used to process rows
        joinNode->pipeId = getPipeId ();
    
        //we set our current select file node on the lect
        joinNode->l = selectFileNode;
        //then we create new file node for our other table
        selectFileNode = new NodeSelectFile ();

        //ispen indicates the status of the file node
        selectFileNode->isOpen = true;
        //we allocate the id of the pipe
        selectFileNode->pipeId = getPipeId ();
        //we attach schema to the select file node
        selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
        
        selectFileNode->schema.Reset (*jItem);
        //we create our and list from the schema
        selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
        //we attach this file node to the right of our join node
        joinNode->r = selectFileNode;
        joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
        joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);

        jItem++;

        //we continue the operation for all the other remaining tables
        while (jItem != jOrder.end ()) {
            // we point store a pointer to our previous join node
            NodeJoin *p = joinNode;
            //then we create a new select file node for our table and allocate it a new pipe id
            selectFileNode = new NodeSelectFile ();
            selectFileNode->isOpen = true;
            selectFileNode->pipeId = getPipeId ();
            selectFileNode->schema = Schema (schemaMap[aliaseMap[*jItem]]);
            selectFileNode->schema.Reset (*jItem);
            selectFileNode->cnf.GrowFromParseTree (boolean, &(selectFileNode->schema), selectFileNode->recLiteral);
            // then we create a new join node which will be the parent of our previous join node and new select file node
            joinNode = new NodeJoin ();
            //we allocate it a pipe id
            joinNode->pipeId = getPipeId ();
            //we put our previous join node as left and select file node as right of the join node
            joinNode->l = p;
            joinNode->r = selectFileNode;

            joinNode->schema.JoinSchema (joinNode->l->schema, joinNode->r->schema);
            joinNode->cnf.GrowFromParseTreeForJoin (boolean, &(joinNode->l->schema), &(joinNode->r->schema), joinNode->recordLiteral);

            jItem++;

        }

        //we store the top most join node as our root node
        rootNode = joinNode;

    }

    //now we check if we have to apply group, aggregate or project operation
    BaseNode *temp = rootNode;
    //we check if we have group attributes
    if (groupingAtts) {
        if (distinctFunc) {
            //we create distinct function for our join node
            rootNode = new NodeDistinct ();
            //we allocate the pipe id then
            rootNode->pipeId = getPipeId ();
            //we put the schema
            rootNode->schema = temp->schema;
            ((NodeDistinct *) rootNode)->fromNode = temp;
            temp = rootNode;
        }
        
        //then we create our groupby node
        rootNode = new NodeGroupBy ();

        vector<string> groupAtts;
        CopyNames (groupingAtts, groupAtts);

        //allocate new pipe id
        rootNode->pipeId = getPipeId ();
        //generate parse tree
        ((NodeGroupBy *) rootNode)->computeFunc.GrowFromParseTree (finalFunction, temp->schema);
        rootNode->schema.GroupBySchema (temp->schema, ((NodeGroupBy *) rootNode)->computeFunc.ReturnInt (), groupAtts);
        ((NodeGroupBy *) rootNode)->group.growFromParseTree (groupingAtts, &(temp->schema));

        ((NodeGroupBy *) rootNode)->from = temp;

    } else if (finalFunction) {

        //if node is the aggregate function then we create a sum node
        rootNode = new NodeSum ();

       //allocate pipe id
        rootNode->pipeId = getPipeId ();
        //create parse tree
        ((NodeSum *) rootNode)->funcCompute.GrowFromParseTree (finalFunction, temp->schema);

        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        rootNode->schema = Schema (NULL, 1, ((NodeSum *) rootNode)->funcCompute.ReturnInt () ? atts[0] : atts[1]);

        ((NodeSum *) rootNode)->fromNode = temp;

    }
    //if attributes are to be selected then we create a project node
    else if (attsToSelect) {

        rootNode = new NodeProject ();
        //create a vector which will keep the attributes to keep
        vector<int> attsToKeep;
        vector<string> atts;
        CopyNames (attsToSelect, atts);
        rootNode->pipeId = getPipeId ();
        //generate  the schema
        rootNode->schema.ProjectSchema (temp->schema, atts, attsToKeep);
        //set the attribute to keep and input and output values
        ((NodeProject *) rootNode)->attrsToKeep = &attsToKeep[0];
        ((NodeProject *) rootNode)->numAttrsOutput = atts.size ();
        ((NodeProject *) rootNode)->numAttrsInput = temp->schema.GetNumAtts ();

        //attach the node to root node
        ((NodeProject *) rootNode)->fromNode = temp;

    }
    
    //print the whole tree from root node
    printTree(rootNode);

    
    return 0;
    
}
