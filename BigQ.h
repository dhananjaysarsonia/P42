#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Run.h"
#include "RecordComparator.h"
#include "RunComparator.h"

using namespace std;
class BigQ {

private:
	int maxPages;
    Pipe *inPipe;
    Pipe *outpipe;
    pthread_t sortingThread;
    char *fileName;
    friend bool RecordComparator (Record* left, Record* right);
    priority_queue<Run*, vector<Run*>, RunComparator> priorityQueue;
    OrderMaker *sortorder;
	
	bool FlushRuns (int runLocation);
 
    

public:
	
	File runFile;
    vector<Record*> recordVector;
    int runlength;
    
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ () {};
    static void *sortingFunction (void *start) {
        
        BigQ *bigQ = (BigQ *)start;
        bigQ->RunGeneration();
        bigQ->RunMerge ();
        return 0;
        
    }
	
	void RunGeneration();
    void RunMerge ();
	

	
};

#endif
