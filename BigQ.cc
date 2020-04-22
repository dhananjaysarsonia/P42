#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    this->sortorder = &sortorder;
    inPipe = &in;
    outpipe = &out;
    //we get the runlength from the user
    runlength = runlen;
    //this will be our number of pages
    maxPages = 1;
    //we start our thread
    pthread_create(&sortingThread, NULL, sortingFunction, (void *)this);
    
}



void BigQ :: RunGeneration() {
    //our fun function
    //this method generates our runs
    Page* p = new Page();
    //first we create a page counter
    int pCounter = 0;
    //set our run location to zero
    int runLocation = 0;
    //create our record object
    Record* record = new Record();
    
    srand (time(NULL));
    fileName = new char[100];
    sprintf (fileName, "%d.txt", (int)(size_t)sortingThread);
    
    runFile.Open (0, fileName);
    //we remove record from in pipe, and repeates the loop while the record exists in inpipe
    while (inPipe->Remove(record)) {
        //we make copy of the record
        Record* tempCopy = new Record ();
        tempCopy->Copy (record);
        //we append our the record to page, page is full then we append the page counter
        if (p->Append (record) == 0) {
            
            pCounter++;
            //if the page counter has reached run length
            if (pCounter == runlength) {
                //we sort the record vector
                sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
                //we get the run location
                if(runFile.GetLength() == 0)
                {
                    
                    runLocation = 0;
                }
                else{
                    runLocation = runFile.GetLength() - 1;
                }
                //we flush our run
                FlushRuns(runLocation);
                
                pCounter = 0;
                
            }
            p->EmptyItOut ();
            p->Append (record);
            
        }
    
        recordVector.push_back (tempCopy);
        
    }
    
    
    //when our pipe is empty we will still have records to write in recordvector
    if(recordVector.size () > 0) {
        //we sort the vector
        sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
        if(runFile.GetLength() == 0)
        {
            runLocation = 0;
        }
        else
        {
            runLocation = runFile.GetLength() - 1;
        }
        //we flush the run
        FlushRuns(runLocation);
        //we flush the page
        p->EmptyItOut ();
        
    }
    //free up memory!
    delete record;
    delete p;
    
}

void BigQ :: RunMerge () {
    //we merge the runs here
    //we give the sort order and file object to our run object here
    Run* run = new Run (&runFile, sortorder);
    //while the priority queue is empty, we merge the runs
    while (!priorityQueue.empty ()) {
    
        
        Record* record = new Record ();
        //we get the run on top of priority queue based on our custom comparators, which compares only top
        //objects of the priority queue
        run = priorityQueue.top ();
        //we pop the run
        priorityQueue.pop ();
        //we take the top record from the run
        record->Copy (run->currentRecord);
        //we insert it into our output pipe
        outpipe->Insert (record);
        //if the run is not empty then we push the run back to the priority queue
        if (run->GetFirstRecord () > 0) {
            
            priorityQueue.push(run);
        
        }
        
        delete record;
        
    }
    //we close the file
    runFile.Close();
    
    remove(fileName);
    
    outpipe->ShutDown();
    delete run;
    
}

bool BigQ :: FlushRuns (int runLocation) {
    //here we flush the run and add the object into priority queue
    Page* p = new Page();
    //we get the size of the our record vector
    int listSize = recordVector.size();
    //maxpages gives us the starting index for the run
    int pageOffset = maxPages;
    int counter = 1;
    //we run the loop for the size of the list
    for (int i = 0; i < listSize; i++) {
		//we get the ith run in the vector
        Record* record = recordVector[i];
		//we append the record in the page, if the record can't be appended
        //then we add the page and pull out another page
        if ((p->Append (record)) == 0) {
            
            counter++;
            runFile.AddPage (p, maxPages);
            maxPages++;
            //we flush the page
            p->EmptyItOut ();
            //append the record to the new page
            p->Append (record);
			
        }
		
        delete record;
		
    }
	//there might be pending page which needs to be flushed
    runFile.AddPage(p, maxPages);
    //we flush that page
    maxPages++;
    p->EmptyItOut();
    //we free up our vector
    recordVector.clear();
    delete p;
	
    //we add the run to our priority queue
    Run* run = new Run(listSize, pageOffset, &runFile, sortorder);
    priorityQueue.push(run);
    
    return true;
	
}


