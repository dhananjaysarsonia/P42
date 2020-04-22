#include "gtest.h"
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

void PrintOperand(struct Operand *pOperand)
{
        if(pOperand!=NULL)
        {
                cout<<pOperand->value<<" ";
        }
        else
                return;
}

void PrintComparisonOp(struct ComparisonOp *pCom)
{
        if(pCom!=NULL)
        {
                PrintOperand(pCom->left);
                switch(pCom->code)
                {
                        case 1:
                                cout<<" < "; break;
                        case 2:
                                cout<<" > "; break;
                        case 3:
                                cout<<" = ";
                }
                PrintOperand(pCom->right);

        }
        else
        {
                return;
        }
}
void PrintOrList(struct OrList *pOr)
{
        if(pOr !=NULL)
        {
                struct ComparisonOp *pCom = pOr->left;
                PrintComparisonOp(pCom);

                if(pOr->rightOr)
                {
                        cout<<" OR ";
                        PrintOrList(pOr->rightOr);
                }
        }
        else
        {
                return;
        }
}
void PrintAndList(struct AndList *pAnd)
{
        if(pAnd !=NULL)
        {
                struct OrList *pOr = pAnd->left;
                PrintOrList(pOr);
                if(pAnd->rightAnd)
                {
                        cout<<" AND ";
                        PrintAndList(pAnd->rightAnd);
                }
        }
        else
        {
                return;
        }
}

char *fileName = "Statistics.txt";



TEST (STATS, FILE_CREATION_READING_TEST) {
	
	
	Statistics s;
    char *relName[] = {"supplier","partsupp"};

	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);	

	char *cnf = "(s_suppkey = ps_suppkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);
	/*
	if(fabs(result - 800000) > 0.1) {
		cout<< result << endl;
		cout<<"error in estimating Q1 before apply \n ";
	}
	*/
	
	ASSERT_NEAR (800000, result, 0.1);
	
	s.Apply(final, relName, 2);

	// test write and read
	s.Write(fileName);

	//reload the statistics object from file
	Statistics s1;
	s1.Read(fileName);	
	cnf = "(s_suppkey>1000)";	
	yy_scan_string(cnf);
	yyparse();
	double dummy = s1.Estimate(final, relName, 2);
	/*
	if(fabs(dummy*3.0-result) >0.1)
	{
		cout<<"Read or write or last apply is not correct\n";
	}
	*/
	
	ASSERT_NEAR (result, dummy * 3.0, 0.1);
	
	
}

TEST (STATS, Q1) {
	
	
	Statistics s;
        char *relName[] = {"lineitem"};

	s.AddRel(relName[0], 6001215);
	s.AddAtt(relName[0], "l_returnflag",3);
	s.AddAtt(relName[0], "l_discount",11);
	s.AddAtt(relName[0], "l_shipmode",7);
	
	char *cnf = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";

	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 1);
	/*
	cout<<"Your estimation Result  " <<result;
	cout<<"\n Correct Answer: 8.5732e+5" << endl;
	*/
	
	ASSERT_NEAR (8.5732e+5, result, 5.0);

	s.Apply(final, relName, 1);

	// test write and read
	s.Write(fileName);
	
	
}



TEST (STATS, Q2) {
	
	
	Statistics s;
    char *relName[] = {"orders","customer","nation"};

	
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	char *cnf = "(c_custkey = o_custkey)";
	yy_scan_string(cnf);
	yyparse();

	// Join the first two relations in relName
	s.Apply(final, relName, 2);
	
	cnf = " (c_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 3);
	/*
	if(fabs(result-1500000)>0.1)
		cout<<"error in estimating Q2\n";
	*/
	
	ASSERT_NEAR (1500000, result, 0.1);
	
	s.Apply(final, relName, 3);

	s.Write(fileName);

	
}


TEST (STATS, Q4) {
	

	Statistics s;
    char *relName[] = { "part", "partsupp", "supplier", "nation", "region"};

	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_size",50);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	s.AddAtt(relName[1], "ps_partkey", 200000);
	
	s.AddRel(relName[2],10000);
	s.AddAtt(relName[2], "s_suppkey",10000);
	s.AddAtt(relName[2], "s_nationkey",25);
	
	s.AddRel(relName[3],25);
	s.AddAtt(relName[3], "n_nationkey",25);
	s.AddAtt(relName[3], "n_regionkey",5);

	s.AddRel(relName[4],5);
	s.AddAtt(relName[4], "r_regionkey",5);
	s.AddAtt(relName[4], "r_name",5);

	s.CopyRel("part","p");
	s.CopyRel("partsupp","ps");
	s.CopyRel("supplier","s");
	s.CopyRel("nation","n");
	s.CopyRel("region","r");

	char *cnf = "(p.p_partkey=ps.ps_partkey) AND (p.p_size = 2)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);
	
	//s.Write(fileName);

	cnf ="(s.s_suppkey = ps.ps_suppkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 3);
	
	//s.Write(fileName);
	cnf =" (s.s_nationkey = n.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 4);

	cnf ="(n.n_regionkey = r.r_regionkey) AND (r.r_name = 'AMERICA') ";
	yy_scan_string(cnf);
	yyparse();

	//s.Write(fileName);
	
	double result = s.Estimate(final, relName, 5);
	/*
	if(fabs(result-3200)>0.1) {
		cout<< result << endl;
		cout<<"error in estimating Q4\n";
	}
	*/
	
	ASSERT_NEAR (3200, result, 0.1);
		
	s.Apply(final, relName, 5);	
	
	s.Write(fileName);
	
	
}
    

int main(int argc, char *argv[]) {
	
	testing::InitGoogleTest(&argc, argv); 
	
	return RUN_ALL_TESTS ();
	
}
