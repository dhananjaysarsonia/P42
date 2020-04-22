#include "RelOp.h"
using namespace std;
int bufferSize = 100;



// Set runLen
void RelOp :: Use_n_Pages (int n) {
    
    runLength = n;
    
}

// Wait Until pthread Done
void RelOp :: WaitUntilDone () {
    
    pthread_join (myThread, NULL);
    
}


// Operation Starter
void *StartThread (void *arg) {
    
    ((RelOp *)arg)->Start ();
    return 0;
    
}



//this method will setup the initial variables and spawn our worker thread for Project
void Project :: Run (
    Pipe &inPipe,
    Pipe &outPipe,
    int *keepMe,
    int numAttsInput,
    int numAttsOutput
) {
    
    in = &inPipe;
    out = &outPipe;
    attsToKeep = keepMe;
    numAttsIn = numAttsInput;
    numAttsOut = numAttsOutput;
    //spawns our thread
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

    //main worker method called by our worker thread.
//this method will keep the attribute list provided by the user, and discard the remaining ones
void Project :: Start () {
    Record *temp = new Record ();
    //remove the record
    while (in->Remove (temp)) {
        //keep the attributes needed
        temp->Project (attsToKeep, numAttsOut, numAttsIn);
        //stuff it into the output pipe
        out->Insert (temp);
    }
    //shut down the pipe
    out->ShutDown ();
    //free up memory from temp.
    delete temp;
}



//method for duplicate removal
//this method initializes our variables spawns our thread for duplicate removal
void DuplicateRemoval :: Run (
                              
    Pipe &inPipe,
    Pipe &outPipe,
    Schema &mySchema
) {
    //setting up pipes and schema and starting our thread
    in = &inPipe;
    out = &outPipe;
    schema = &mySchema;
    
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

void DuplicateRemoval :: Start () {
    //initializing our comparison engine
    ComparisonEngine engine;
    //current and previous records to compare
    Record *current = new Record ();
    Record *previous = new Record ();
    //getting the order for the schema
    OrderMaker sortOrder (schema);
    //initializng pipe to sort the records first
    Pipe pipe (bufferSize);
    BigQ bigq (*in, pipe, sortOrder, runLength);
    
    pipe.Remove (previous);
    //loop runs while there are records in the pipe
    while (pipe.Remove (current)) {
        //compare the records with our compairsonEngine
        if (engine.Compare (previous, current, &sortOrder)) {
            //insert the record in output pipe
            out->Insert (previous);
            //copy the record in the previous field
            previous->Copy (current);
        }
    }
    if (current->bits != NULL && !engine.Compare (current, previous, &sortOrder)) {
        
        out->Insert (previous);
        previous->Copy (current);
        
    }

    
    //shut down the popes
    out->ShutDown ();
    Global::Instance()->isDuplicateDone = true;
    //free up variables
    delete current;
    delete previous;
    
}




//method for join operation
//this will setup the initial variables and spawn our worker thread
void Join :: Run (
    Pipe &inPipeL,
    Pipe &inPipeR,
    Pipe &outPipe,
    CNF &selOp,
    Record &literal
) {
    
    inL = &inPipeL;
    inR = &inPipeR;
    out = &outPipe;
    cnf = &selOp;
    litrl = &literal;
    //spawning our worker thread
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}
//this method is called by our worker thread. it does the main join operation
void Join :: Start () {
    

    
    //intializing comparison engine
    ComparisonEngine engine;
    Record *recordFromLeft = new Record ();
    Record *recordFromRight = new Record ();
    OrderMaker leftOrder, rightOrder;
    
    cnf->GetSortOrders(leftOrder, rightOrder);
    
    int leftNumberOfAttributes, rightNumberOfAttributes;
    int totalNumberOfAttributes;
    int *attsToKeep;
    //this returns true if we are handling for sorted files
    if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
        
    //we use our BIGQ class if files are sorted
    //we first initialize our left and right pipes
        Pipe left (bufferSize);
        Pipe right (bufferSize);
        //we initialize our left and right bigqs
        BigQ leftBigQ (*inL, left, leftOrder, runLength);
        BigQ rightBigQ (*inR, right, rightOrder, runLength);
        
        //we turn down the isfinished flag which will be used later
        bool isFinished = false;
        //if left has no records, then isFinished is true
        if (!left.Remove (recordFromLeft)) {
            
            isFinished = true;
            
        } else {
            //get the length of the left record
            leftNumberOfAttributes = recordFromLeft->GetLength ();
            
        }
        //same thing done for the right
        if (!isFinished && !right.Remove (recordFromRight)) {
            isFinished = true;
            
        } else {
            //get the length of right record
            rightNumberOfAttributes = recordFromRight->GetLength ();
            //get total length
            totalNumberOfAttributes = leftNumberOfAttributes + rightNumberOfAttributes;
            //get the attributes to keep
            attsToKeep = new int[totalNumberOfAttributes];
        
            for (int i = 0; i < leftNumberOfAttributes; i++) {
                attsToKeep[i] = i;
            }
            for (int i = 0; i < rightNumberOfAttributes; i++) {
                attsToKeep[leftNumberOfAttributes + i] = i;
            }
            
        }
        
        while (!isFinished) {
            //compare with the comparator engine
            while (engine.Compare (recordFromLeft, &leftOrder, recordFromRight, &rightOrder) > 0) {
                //finish the loop if right is empty
                if (!right.Remove (recordFromRight)) {
        
                    isFinished = true;
                    break;
                    
                }
                
            }
            //do it for the left
            while (!isFinished && engine.Compare (recordFromLeft, &leftOrder, recordFromRight, &rightOrder) < 0) {
                //finish when left is empty
                if (!left.Remove (recordFromLeft)) {
                    isFinished = true;
                    break;
                }
            }
            //join the records
            while (!isFinished && engine.Compare (recordFromLeft, &leftOrder, recordFromRight, &rightOrder) == 0) {
                Record *temp = new Record ();
                //join record and get merged record
                temp->MergeRecords (
                    recordFromLeft,
                    recordFromRight,
                    leftNumberOfAttributes,
                    rightNumberOfAttributes,
                    attsToKeep,
                    totalNumberOfAttributes,
                    leftNumberOfAttributes
                );
                //put the record into output pipe
                out->Insert (temp);
        
                if (!right.Remove (recordFromRight)) {
                    
                    isFinished = true;
                    break;
                    
                }
                
            }
            
        }
        //remove the records from left
        while (right.Remove (recordFromLeft));
        //remove the record from right
        while (left.Remove (recordFromLeft));
        
    } else {
        //unsorted file version
        //we will be handling heap files in here
        char fileName[100];
        //create a temp file
        sprintf (fileName, "temp.tmp");
        //create the heaphandler
        HeapHandler dbFile;
        //create the temp file
        dbFile.Create (fileName);
        //put the flag to false
        bool isFinished = false;
        //remove the record from left pipe, if there are no records then return true
        
        if (!inL->Remove (recordFromLeft)) {
            //turn up the isFinished if there are no more records in the left pipe
            isFinished = true;
            
        } else {
            //get the length of record
            leftNumberOfAttributes = recordFromLeft->GetLength ();
            
        }
        //remove the record from the right pipe
        if (!inR->Remove (recordFromRight)) {
            //turn up the isfinished flag if there are no records
            isFinished = true;
            
        } else {
            //get length from the right record
            rightNumberOfAttributes = recordFromRight->GetLength ();
            //calculate total length
            totalNumberOfAttributes = leftNumberOfAttributes + rightNumberOfAttributes;
            //find out the attributes to keep
            attsToKeep = new int[totalNumberOfAttributes];
            for (int i = 0; i < leftNumberOfAttributes; i++) {
                attsToKeep[i] = i;
            }
            for (int i = 0; i < rightNumberOfAttributes; i++) {
                attsToKeep[leftNumberOfAttributes + i] = i;
            }
        }
        //if the operation is finished then proceed
        if (!isFinished) {
            
            do{
                //add record to the file while there are records in the left pipe
                dbFile.Add (*recordFromLeft);
                
            } while (inL->Remove (recordFromLeft));
            
            do{
               //this loop will run while there are next records in the right pipe
                //move to the first
                dbFile.MoveFirst ();
                
                Record *newrecord = new Record ();
                //get record from dbfile
                while (dbFile.GetNext (*recordFromLeft)) {
                    //compare the records
                    if (engine.Compare (recordFromLeft, recordFromRight, litrl, cnf)) {
                        //merge the records
                        newrecord->MergeRecords (
                            recordFromLeft,
                            recordFromRight,
                            leftNumberOfAttributes,
                            rightNumberOfAttributes,
                            attsToKeep,
                            totalNumberOfAttributes,
                            leftNumberOfAttributes
                        );
                        
                    //here we insert the merged record to the output pipe
                        out->Insert (newrecord);
                    }
                    
                }
                //free up memory from new rec
                delete newrecord;
                //do this while records from right are removed
            } while (inR->Remove (recordFromRight));
            
        }
        
        dbFile.Close ();
        remove ("temp.tmp");
        
    }
//shut down the pipe
    out->ShutDown ();
    // free up the memory
    delete recordFromLeft;
    delete recordFromRight;
    delete attsToKeep;
    
}



//method for run
void Sum :: Run (
//intializes pipes and spawns our worker thread
    Pipe &inPipe,
    Pipe &outPipe,
    Function &computeMe
) {
    
    in = &inPipe;
    out = &outPipe;
    compute = &computeMe;
    
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

void Sum :: Start () {
    
    stringstream ss;
    Attribute atts;
    Record *temp = new Record ();
    
    //if the values are integer, we use integer sum. If the values are double we use the double sum
    int integerSum = 0, integerRec;
    double doubleSum = 0.0, doubleRec;
    //create attribute named sum
    atts.name = "SUM";
    if (!in->Remove (temp)) {
        //if there are no records, there is something wrong
        cout << "Something is wrong" << endl;
        out->ShutDown ();
        return ;
    }
    atts.myType = compute->Apply (*temp, integerRec, doubleRec);
    //check the tupe of attribute
    if (atts.myType == Int) {
        //add it into integersum if int type
        integerSum += integerRec;
    } else {
        //otherwise add it to double sum
        doubleSum += doubleRec;
    }
    //do a while till the pipe is not empty
    while (in->Remove (temp)) {
        
        compute->Apply (*temp, integerRec, doubleRec);
        //add to integer sum if integer type
        if (atts.myType == Int) {
            integerSum += integerRec;
        } else {
        //otherwise add to double sum
            doubleSum += doubleRec;
        }
    }
    //if attribute type is int then write integersum
        if (atts.myType == Int) {
        Schema sumSchema (NULL, 1, &atts);
        ss << integerSum << '|';
        temp->ComposeRecord (&sumSchema, ss.str ().c_str ());
        
    } else {
     //otherwise write the double sum
        Schema sumSchema (NULL, 1, &atts);
        ss << doubleSum << '|';
        temp->ComposeRecord (&sumSchema, ss.str ().c_str ());
    }
    //insert the record with sum into output pipe
    out->Insert (temp);
    //shut down the pipe
    out->ShutDown ();
    
}

//this will setup our pipes and variables for groupby and spawn our worker thread
void GroupBy :: Run (
    Pipe &inPipe,
    Pipe &outPipe,
    OrderMaker &groupAtts,
    Function &computeMe
) {
    
    in = &inPipe;
    out = &outPipe;
    order = &groupAtts;
    compute = &computeMe;
    //worker thread will call start method
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

//method called by the worker thread
void GroupBy :: Start () {
    //the idea behind this method is that we are first sorting the records based on the group asked by user
    //then we compare the previous and next record. if they are same, then they are of the same group
    //we sum the records of the same group then
    //if they are different then they are of different group
    Schema *sumSchema;
    Attribute att;
    stringstream ss;
    Type type;

    //initializing our comparison engine
    ComparisonEngine engine;
    //initializing our pipe which will contain record into sorted from ordered by groupby attributes
    Pipe sortPipe (bufferSize);
    
    //previous record
    Record *prev = new Record ();
    //current record
    Record *curr = new Record ();
    //sum
    Record *sum = new Record ();
    //new record
    Record *newRec = new Record ();
    //first we sort our pipe with groupby order given by the user
    BigQ bigq (*in, sortPipe, *order, runLength);
    
    int count = 0;
    //initializing integer sum to be used when attributes are int type
    int integerSum = 0, integerRec;
    //dobule sum to be used when attribute is double type
    double doubleSum = 0.0, doubleRec;

    char *sumString = new char[bufferSize];
    
    int numAtts = order->numAtts;
    int *atts = order->whichAtts;
    //attributes to keep
    int *attsToKeep = new int[numAtts + 1];
    
    attsToKeep[0] = 0;
    
    for (int i = 0; i < numAtts; i++) {
        attsToKeep[i + 1] = atts[i];
    }
    //remove the first record from the sorted pipe
    if (sortPipe.Remove (prev)) {
        //apply compute with our previous record
        type = compute->Apply (*prev, integerRec, doubleRec);
        
        if (type == Int) {
            //add to the integer sum if attribute is integer type
            integerSum += integerRec;
            
        } else {
            //otherwise add to doublesum
            doubleSum += doubleRec;
        }
        
    } else {
        //error message, shutdown and free up memory
        cout << "There is some error, no records in the output pipe!" << endl;
        //shutdown the pipe
        out->ShutDown();
        //free up memory from the variables
        delete sumString;
        delete sumSchema;
        delete prev;
        delete curr;
        delete sum;
        delete newRec;
        
        exit (-1);
        
    }
    //naming the attribute sum
    att.name = "SUM";
    //attribute type is set by the type we got from the record
    att.myType = type;
    //setting up our sum schema with our attribute
    sumSchema = new Schema (NULL, 1, &att);
    //remove the records while pipe is not empty
    while (sortPipe.Remove (curr)) {
        //compare the previous and current record
        if (engine.Compare (prev, curr, order) != 0) {
            //if the record is different then write the integer sum, this means that new group has started now
            if (type == Int) {
                //write the int
                sprintf (sumString, "%d|", integerSum);
                
            } else {
                //write the double
                sprintf (sumString, "%f|", doubleSum);
                
            }
            sum->ComposeRecord (sumSchema, sumString);
            //merge the records
            newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
            //write the record to the output pipe
            out->Insert (newRec);
            //increment the group count
            count++;
            
            //start a new double sum from 0
            doubleSum = 0.0;
            //start a new integer sum 0
            integerSum = 0;
            compute->Apply (*curr, integerRec, doubleRec);
        
            if (type == Int) {
            //sum the integer to the current sum
                integerSum += integerRec;
                
            } else {
                //if double then sum the double to the double sum
                doubleSum += doubleRec;
                
            }
            //make a copy of the current record to previous
            prev->Consume (curr);
            
            
        } else {
//when the records are equal
            compute->Apply (*curr, integerRec, doubleRec);
            //we keep on adding the sum of the group until there is a mismatch, and then we start a new group
            if (type == Int) {
                //add to the integersum
                integerSum += integerRec;
                
            } else {
                //add to the double sum if double
                doubleSum += doubleRec;
                
            }
        }
    }

    if (type = Int) {
        
        sprintf (sumString, "%d|", integerSum);
        
    } else {
        
        sprintf (sumString, "%f|", doubleSum);
        
    }
    
    count++;
    sum->ComposeRecord (sumSchema, sumString);
    newRec->MergeRecords (sum, prev, 1, prev->GetLength (), attsToKeep, numAtts + 1, 1);
    //insert the record
    out->Insert (newRec);
//shut down pipe
    out->ShutDown ();
    //free up the memory
    delete sumString;
    delete sumSchema;
    delete prev;
    delete curr;
    delete sum;
    delete newRec;
    
}
//this will setup the pipe and file and spawn our worker thread
void WriteOut :: Run (
    Pipe &inPipe,
    FILE *outFile,
    Schema &mySchema
) {

    in = &inPipe;
    file = outFile;
    schema = &mySchema;
    //our worker thread will call the start method
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}
//this function will simply read the records and write it into a text file
void WriteOut :: Start () {
    //create temp record to read
    Record *temp = new Record ();
    //run the loop while there are records in the input pipe
    while (in->Remove (temp)) {
        //write the record into the output pipe
        temp->WriteToFile (file, schema);
    }
    //close the file
    fclose (file);
    //ofcourse, free up the memory
    delete temp;
    
}


//this function spawns our thread after setting up the required variable
void SelectPipe :: Run (
    Pipe &inPipe,
    Pipe &outPipe,
    CNF &selOp,
    Record &literal
) {
    in = &inPipe;
    out = &outPipe;
    cnf = &selOp;
    litrl = &literal;
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

//method called by the worker thread to do the work
void SelectPipe :: Start () {
    Record *temp = new Record ();
    ComparisonEngine engine;
//here we are comparing each record with the CNF and literal.
    
    //remove the record from pipe
    while (in->Remove (temp)) {
        // if the record satisfies the cnf with the literal provided, then we proceed
        if (engine.Compare (temp, litrl, cnf)) {
            //insert the satisfied record into output pipe
            out->Insert (temp);
        }
    }
    //shut down output pipe
    out->ShutDown ();
    //free up temp variable
    delete temp;
    
}

//this function spawns our thread after setting up the required variable
void SelectFile :: Run (
    DBFile &inFile,
    Pipe &outPipe,
    CNF &selOp,
    Record &literal
) {
    //setting up the values provided
    file = &inFile;
    out = &outPipe;
    cnf = &selOp;
    litrl = &literal;
    //create our worker thread
    pthread_create (&myThread, NULL, StartThread, (void *) this);
    
}

//method called by the worker thread
void SelectFile :: Start () {
    Record *temp = new Record ();
    //here we get the next record satisfied by the cnf
    while (file->GetNext (*temp, *cnf, *litrl)) {
        //we stuff it in the output pipe until there are no other records
        out->Insert (temp);
    }
    //we shutdown the pipe
    out->ShutDown ();
    //free up the memory from temp
    delete temp;
    
}

