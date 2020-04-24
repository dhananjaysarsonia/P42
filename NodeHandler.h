#ifndef NODEHANDLER_H
#define NODEHANDLER_H

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

using namespace std;




enum NodeType {
    //enum to define NodeType.
    //ToDo: remove the enums
     SelectFileType, SelectPipeType, ProjectionType, DistinctType, SumType, GroupByType, JoinType
};

class BaseNode {
//base node class of our node. We will be other n
    
public:
    //pipe id
    int pipeId;
    //type of node from our enum
    NodeType nodeType;
    Schema schema;
    //constructor for base node accepting type
    BaseNode ();
    BaseNode (NodeType type) : nodeType (type) {}

    ~BaseNode () {}
    //virtual function for print overridden by children classes to print different kind of nodes
    virtual void PrintNodes () {};
    
};

class NodeJoin : public BaseNode {
//join node will be created for join operation
public:
    //node from left
    BaseNode *l;
    //node from right
    BaseNode *r;
    //cnf for join
    CNF cnf;
Record recordLiteral;
    //initializing node with the join type
    //ToDo: now it is irrelevant to pass the join type
    NodeJoin () : BaseNode (JoinType) {}
    ~NodeJoin () {
        // freeing up values in the destructor
        if (l) delete l;
        if (r) delete r;
        
    }
    
    void PrintNodes () {
        //calling print recursively
        //first calling print on left node
        l->PrintNodes ();
        //then on right node
        r->PrintNodes ();
        //then printing the current parent node
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
//node for our project operation
public:
    //number of attributes in the input
    int numAttrsInput;
    //number of attributes in the output
    int numAttrsOutput;
    //the attributes which we need to keep from the data
    int *attrsToKeep;
    //node on which operation needs to be applied
    BaseNode *fromNode;
    
    NodeProject () : BaseNode (ProjectionType) {}
    ~NodeProject () {
        //destructor to free up attrsToKeep
        if (attrsToKeep) delete[] attrsToKeep;
        
    }
    //printing node recursively
    void PrintNodes () {
        //calling print node on the from node
        fromNode->PrintNodes ();
        //printing the final project operation results
        cout << "*********************" << endl;
        cout << "Project Operation" << endl;
        //printing input pipe id
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        //printing output pipe ID
        cout << "Output Pipe" << pipeId << endl;
        //printing number of attributes
        cout << "NumAtts: " << numAttrsInput << endl;
        //printing attributes to keep
        cout << "Atts To Keep :" << endl;
        //printing the attributes to keep
        for (int i = 0; i < numAttrsOutput; i++) {
            
            cout << attrsToKeep[i] << endl;
            
        }
        //printing number of attributes in the final output
        cout << "Number Attrs Output : " << numAttrsOutput << endl;
        //printing the output schema
        cout << "Output Schema:" << endl;
        schema.Print ();
        cout << "*********************" << endl;
        
        
    }
    
};

class NodeSelectFile : public BaseNode {
//select file supports CNF for select pipe also. Our both the operations are same
public:
    //flag to check if file is open
    bool isOpen;
    //cnf supporting our select pipe operation
    CNF cnf;
    //dbfile for select file operation
    DBFile dbfile;
    Record recLiteral;
    
    NodeSelectFile () : BaseNode (SelectFileType) {}
    ~NodeSelectFile () {
        if (isOpen) {
            //closing the file if file is open
            dbfile.Close ();
        }
    }
    void PrintNodes () {
        //select file print operation
        cout << "*********************" << endl;
        cout << "Select File Operation" << endl;
        //out pipe id
        cout << "Output Pipe " << pipeId << endl;
        //schema from output pipe
        cout << "Output Schema:" << endl;
        //printing the schema
        schema.Print ();
        //printing the CNF
        cout << "CNF:" << endl;
        cnf.PrintWithSchema(&schema,&schema,&recLiteral);
        cout << "*********************" << endl;
        
    }
    
};


class NodeSum : public BaseNode {
    //Node for sum operation.
public:
    
    Function funcCompute;
    BaseNode *fromNode;
    NodeSum () : BaseNode (SumType) {}
    ~NodeSum () {
        if (fromNode) delete fromNode;
    }
    void PrintNodes () {
        //printing our sum node
        fromNode->PrintNodes ();
        cout << "*********************" << endl;
        cout << "Sum Operation" << endl;
        //printing the input pipe id
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        //output pipe id
        cout << "Output Pipe: " << pipeId << endl;
        //our final function schema
        cout << "Function:" << endl;
        funcCompute.Print (&fromNode->schema);
        cout << "*********************" << endl;
    }
};

class NodeDistinct : public BaseNode {
//node for distinct operation
public:
    BaseNode *fromNode;
    //passing distinct type enum to our node
    NodeDistinct () : BaseNode (DistinctType) {}
    ~NodeDistinct () {
        if (fromNode) delete fromNode;
    }
    void PrintNodes () {
        //calling print from the from node
        fromNode->PrintNodes ();
        //printing our distinct node
        cout << "*********************" << endl;
        cout << "Duplication Elimation Operation" << endl;
        //printing input pipe id
        cout << "Input Pipe: " << fromNode->pipeId << endl;
        //printing output pipe id
        cout << "Output Pipe: " << pipeId << endl;
        cout << "*********************" << endl;
    }
};

class NodeGroupBy : public BaseNode {

public:
    //group by node
    BaseNode *from;
    Function computeFunc;
    //order maker for group
    OrderMaker group;
    
    NodeGroupBy () : BaseNode (GroupByType) {}
    ~NodeGroupBy () {
        //deleting node
        if (from) delete from;
        
    }
    
    void PrintNodes () {
        //printing group by node
        from->PrintNodes ();
        
        cout << "*********************" << endl;
        cout << "Group By Operation" << endl;
        //printing input pipe id
        cout << "Input Pipe ID : " << from->pipeId << endl;
        //output pipe id
        cout << "Output Pipe ID : " << pipeId << endl;
        //schema print
        cout << "Output Schema : " << endl;
        schema.Print ();
        cout << "Function : " << endl;
        computeFunc.Print (&from->schema);//then we finally print the order maker
        cout << "OrderMaker : " << endl;
        group.Print ();
        cout << "*********************" << endl;
            
    }
    
};



#endif
