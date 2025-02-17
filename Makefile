
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif


gtest: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Statistics.o RelationHelper.o AttributeHelper.o gtest-all.o gtest.o
	$(CC) -o gtest y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Statistics.o RelationHelper.o AttributeHelper.o gtest-all.o gtest.o -ll -lpthread
#gtest-all.o: gtest-all.cc
#	$(CC)  -g -DGTEST_HAS_PTHREAD=0 -c gtest-all.cc
gtest-all.o: gtest-all.cc
	$(CC)  -g -c gtest-all.cc
#-DGTEST_HAS_PTHREAD=0

#test: Statistics.o RelationHelper.o AttributeHelper.o y.tab.o lex.yy.o test.o
#	$(CC) -o test Statistics.o RelationHelper.o AttributeHelper.o y.tab.o lex.yy.o test.o -lfl -lpthread


main: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Statistics.o RelationHelper.o AttributeHelper.o main.o
	$(CC) -o a42.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Statistics.o RelationHelper.o AttributeHelper.o main.o -ll -lpthread
#
#	main:   y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o Statistics.o main.o
#	$(CC) -o main y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o Statistics.o main.o -ll -lpthread
#


a3test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o Function.o RelOp.o RecordComparator.o RunComparator.o BigQ.o Global.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o
	$(CC) -o a3test Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o Function.o RelOp.o RecordComparator.o Global.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o a3test.o -lfl -lpthread
	
	
a22test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2-test.o
	$(CC) -o a22test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2-test.o -lfl -lpthread



#gtest.o: gtest.cc
#	$(CC) -g -c gtest.cc

test.o: test.cc
	$(CC) -g -c test.cc
	
main.o : main.cc
	$(CC) -g -c main.cc
	
a2-1-test.o: a2-1-test.cc
	$(CC) -g -c a2-1-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc
	
Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc


Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

	
y.tab.o: Parser.y
	yacc -d Parser.y
	gsed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c
	
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c


lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c
	
lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
