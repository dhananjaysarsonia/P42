#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedFileHandler.h"


DBFile :: DBFile () {}

DBFile :: ~DBFile () {
	
	delete fileHandler;
	
}

int DBFile :: Create (const char *fpath, fType f_type, void *startup) {
	//we create a meta data file to store the details of our file created
    ofstream metadata;
	char metadataPath[100];
	
	sprintf (metadataPath, "%s.md", fpath);
    //we open the meta data file in the file path
	metadata.open (metadataPath);
    //we check the file type
    switch (f_type) {
        case heap:
        {
            //if the filetype is heap then we point to our heapHandler where methods for heaps are overriden
            metadata << "heap" << endl;
            fileHandler = new HeapHandler;
            break;
        }
          
            
        case sorted:
        {
            //if the filetype is sorted then we save metadata with sorted keyword, and we also store runlength, and order
            //this helps in reading and searching records in our sorted file later
            metadata << "sorted" << endl;
            // if the file is sorted,
            metadata << ((SortBus *) startup)->runLength << endl;
            metadata << ((SortBus *) startup)->myOrder->numAtts << endl;
            ((SortBus *) startup)->myOrder->PrintInOfstream (metadata);
            //we point our filehandler to sortedfilehandler
            fileHandler = new SortedFileHandler (((SortBus *) startup)->myOrder, ((SortBus *) startup)->runLength);
            break;
            
        }
        default:
            //close the metadata file
            metadata.close();
            return 0;
    }
    
    
    

	//now our filehandler will magivally
	fileHandler->Create (fpath);
	metadata.close ();
	
	return 1;
	
}

int DBFile :: Open (char *filepath) {
	//we create object for our metadata
	ifstream metadata;
    //we will load the attributes through this objects
	string inputString;
	
	int attNum;
	char *mdpath = new char[100];
	//we open our metadata file
	sprintf (mdpath, "%s.md", filepath);
	metadata.open (mdpath);
	//if the files is opened
	if (metadata.is_open ()) {
		//we read the file
		metadata >> inputString;
		//if it is heap type we point the filehandler to heaphandler
		if (inputString.compare ("heap") == 0) {
		
			fileHandler = new HeapHandler;
			
		} else if (inputString.compare ("sorted") == 0){
        //if sorted type, then we point file handler to SortedHandler
			int runLength;
			//we create the ordermaker object
			OrderMaker *order = new OrderMaker;
			//we read the runlength from metadata file
			metadata >> runLength;
            //we read the attribute from metadata
			metadata >> order->numAtts;
        
			for (int i = 0; i < order->numAtts; i++) {
				//we parse the attributes from metadata here
				metadata >> attNum;
				metadata >> inputString;
				
				order->whichAtts[i] = attNum;
				
				if (!inputString.compare ("Int")) {
					
					order->whichTypes[i] = Int;
					
				} else if (!inputString.compare ("Double")) {
					
					order->whichTypes[i] = Double;
					
				} else if (!inputString.compare ("String")) {
					
					order->whichTypes[i] = String;
					
				} else {
					
					delete order;
					
					metadata.close ();
					
					cout << "Some error occurred (" << filepath << ")" << endl;
					
					return 0;
					
				}
				
			}
			
			fileHandler = new SortedFileHandler (order, runLength);
			
			
		} else {
			//if the file type is different, return error
			metadata.close ();
			
			cout << "Bad file type, it should be sorted or heap only(" << filepath << ")" << endl;
			
			return 0;
			
		}
		
	} else {
		//if file doesn't open then return error
        //close the metadatafile object
		metadata.close ();
		
		cout << "Check the path, there is some error (" << filepath << ")!" << endl;
		return 0;
		
	}
	
	fileHandler->Open (filepath);
	metadata.close ();
	
	return 1;
	
}

int DBFile :: Close () {
	//close the file
	return fileHandler->Close ();
	
}

void DBFile :: Load (Schema &myschema, const char *loadpath) {
	//load the file from filehandler which will point to the right subclass
	fileHandler->Load (myschema, loadpath);
	
}

void DBFile :: MoveFirst () {
	//move first is called for the file type here
	fileHandler->MoveFirst ();
	
}

void DBFile :: Add (Record &addme) {
	//record is added based on the file type
	fileHandler->Add (addme);
	
}

int DBFile :: GetNext (Record &fetchme) {
	//next record is fetched based on the file type
	return fileHandler->GetNext (fetchme);
	
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//next order is fetched based on the file type and CNF
	return fileHandler->GetNext (fetchme, cnf, literal);
	
}
