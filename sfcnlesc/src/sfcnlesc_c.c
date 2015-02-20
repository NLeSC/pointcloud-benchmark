/*
 ============================================================================
 Name        : sfcnlesc.c
 Author      : Oscar Martinez Rubi
 Version     :
 Copyright   : 
 Description : 
 ============================================================================
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <geos_c.h>
#include <inttypes.h>
#include <ctype.h>
#include <math.h> 

#include "vector.h"

#define __STDC_FORMAT_MACROS

// DEFINITIONS
#define MAXWKTLEN 1047551
#define MAXBITS 31
#define MAXDIGITS 15
#define EPSILON 0.001

// CONSTANTS
static const long ONELONG = 1; 
static const int NUMELEMQTC = 4;
static const int NUMLEVELFACTOR = 0;
//static const int MAXRANGES = 8;

//using namespace std;

// GLOBAL VARIABLES
int numbits;
int numlevels;
int domain[4];
int startlevel;
uint64_t startquadcode;
uint32_t startquad[4];
int maxranges;

int compare64 (const void * a, const void * b)
{
	if ( *(uint64_t*)a <  *(uint64_t*)b ) return -1;
  	if ( *(uint64_t*)a == *(uint64_t*)b ) return 0;
  	if ( *(uint64_t*)a >  *(uint64_t*)b ) return 1;
}

char double_equals(double a, double b){
    if (abs(a - b) < EPSILON) return 1; else return 0;
}

void usage(char *me){
	fprintf(stderr, "Usage: %s <wktfile> <minx> <miny> <maxx> <maxy> <scalex> <scaley> <offsetx> <offsety> [<numlevels> <mergeconsecutive> <maxranges>] \n", me);
	exit(1);
}

void notice(const char *fmt, ...) {
    va_list ap;

    fprintf( stdout, "NOTICE: ");

    va_start (ap, fmt);
    vfprintf( stdout, fmt, ap);
    va_end(ap);
    fprintf( stdout, "\n" );
}

void log_and_exit(const char *fmt, ...) {
    va_list ap;

    fprintf( stdout, "ERROR: ");

    va_start (ap, fmt);
    vfprintf( stdout, fmt, ap);
    va_end(ap);
    fprintf( stdout, "\n" );
    exit(1);
}

char* readWKT(const char* inputfile){
	char* buffer = 0;
	long length;
	FILE* f = fopen (inputfile, "rb");
	
	if (f)
	{
	  fseek (f, 0, SEEK_END);
	  length = ftell (f);
	  fseek (f, 0, SEEK_SET);
	  buffer = malloc (length);
	  if (buffer)
	  {
	    fread (buffer, 1, length, f);
	  }
	  fclose (f);
	}
	
	return buffer;
}

char* scale(char* wkt, double scaleX, double scaleY, double offsetX, double offsetY){
    char* bufferOWKT = NULL;
	size_t bufferSizeOWKT = 0;
	FILE* streamOWKT = open_memstream(&bufferOWKT, &bufferSizeOWKT);
	
	char* bufferNumber = NULL;
	size_t bufferSizeNumber = 0;
	FILE* streamNumber = open_memstream(&bufferNumber, &bufferSizeNumber);

    int numcounter = 0; // counter of numbers
    int snum; // scaled number
    int charcounter = 0;
    int i;
    for(i = 0; wkt[i] != '\0'; ++i) {
        if (isdigit(wkt[i]) || wkt[i] == '.'){
        	charcounter++;
        	char buf[2];
        	buf[0] = wkt[i];
        	buf[1] = '\0';
        	fprintf(streamNumber, "%s", buf);
        }
        else{
            if (charcounter > 0){
            	fclose(streamNumber);
                if (numcounter % 2 == 1){
                    snum = (int)((atof(bufferNumber) - offsetY) / scaleY);
                }else{
                    snum = (int)((atof(bufferNumber) - offsetX) / scaleX);
                }
                free(bufferNumber);
                fprintf(streamOWKT, "%d", snum);
                
                numcounter++;
                charcounter = 0;
                
                bufferNumber = NULL;
				bufferSizeNumber = 0;
				streamNumber = open_memstream(&bufferNumber, &bufferSizeNumber);
             }
             char buf[2];
        	 buf[0] = wkt[i];
        	 buf[1] = '\0';
             fprintf(streamOWKT, "%s", buf);
        }
    }
    fclose(streamOWKT); //This will set buffer and bufferSize.
    return bufferOWKT;
}

int disjoint(char* ptr){
	if ((ptr[0] == 'F') && (ptr[1] == 'F') && (ptr[3] == 'F') && (ptr[4] == 'F')) return 1;
	return 0;
}

int contains(char* ptr){
	if ((ptr[0] != 'F') && (ptr[6] == 'F') && (ptr[7] == 'F')) return 1;
	return 0;
}

int excludeinteriors(char* ptr) {
	if (ptr[0] == 'F') return 1;
	return 0;
}

GEOSGeometry* createBox(uint32_t minx, uint32_t miny, uint32_t maxx, uint32_t maxy){
	GEOSCoordSequence* cs = GEOSCoordSeq_create(5, 2);
	//quads[i] is minx miny maxx maxy
	GEOSCoordSeq_setX(cs, 0, minx);
    GEOSCoordSeq_setY(cs, 0, maxy);
    
    GEOSCoordSeq_setX(cs, 1, minx);
    GEOSCoordSeq_setY(cs, 1, miny);
    
    GEOSCoordSeq_setX(cs, 2, maxx);
    GEOSCoordSeq_setY(cs, 2, miny);
    
    GEOSCoordSeq_setX(cs, 3, maxx);
    GEOSCoordSeq_setY(cs, 3, maxy);
    
    GEOSCoordSeq_setX(cs, 4, minx);
    GEOSCoordSeq_setY(cs, 4, maxy);
    
    return GEOSGeom_createPolygon(GEOSGeom_createLinearRing(cs), NULL, 0);
}

int relation(GEOSGeometry* geom1, GEOSGeometry* geom2) {
/* Returns the relationship between two geometries. 
   0 if they are disjoint, 
   1 if geom2 is completely in geom1,
   2 if geom2 is partly in geom1
*/
	char *ptr;
	int r;
	ptr = GEOSRelate(geom1, geom2);
	if ( ! GEOSRelatePattern(geom1, geom2, ptr) )
	{
		GEOSGeom_destroy(geom1);
		GEOSGeom_destroy(geom2);
		free(ptr);
		log_and_exit("! RelatePattern");
	}
	//printf("Relate: %s\n", ptr); 
	
	
	if (disjoint(ptr)) r = 0;
    else if (contains(ptr)) r = 1; 
    else // there is some overlaps
    { 
        if (excludeinteriors(ptr)) r = 0; // overlap only in boundaries, we do not count it
        else r = 2; // some interior of geom2 is in geom1
    }          
	free(ptr);
	return r;
}

int getRangesInQuadTreeCell(vector* pranges, vector* prelations, int maxdepth, int parentlevel, uint64_t parentcode, GEOSGeometry* qgeom, uint32_t minx, uint32_t miny, uint32_t maxx, uint32_t maxy){
	GEOSGeometry* geombox;
	int i, r;
	size_t j;
	long quadcode;
	// Recursive method that return morton ranges overlapping with the region for the specified domain
	uint32_t cx = minx + ((maxx - minx) >> 1);
	uint32_t cy = miny + ((maxy - miny) >> 1);
    int level = parentlevel + 1;
    
    uint32_t quad[4];
   	int counter = 0;
    for (i=0; i<NUMELEMQTC; i++) {
    	
    	if (i == 0){
    		quad[0] = minx; quad[1] = miny; quad[2] = cx; quad[3] = cy;
    	}else if (i == 1){
    		quad[0] = minx; quad[1] = cy; quad[2] =  cx; quad[3] = maxy;
    	}else if (i == 2){
    		quad[0] = cx; quad[1] = miny; quad[2] =  maxx; quad[3] = cy;
    	}else{
    		quad[0] = cx; quad[1] = cy; quad[2] =  maxx; quad[3] = maxy;
    	} 
    	
        geombox = createBox(quad[0], quad[1], quad[2], quad[3]);
        r = relation(qgeom, geombox);
        
        char toadd = 0; // 0 is not to add, 1 is to add and is fully in, 2 is to add but not fully in
        
        if (r > 0)
        {
        	quadcode = (parentcode << 2) + i;
        	if ((r == 1) || (parentlevel == maxdepth)){
        		toadd = r;
        	}
        	else
        	{
        		vector sranges;
			    vector* psranges = &sranges;
				vector_init(psranges, sizeof(uint64_t), (size_t)0, NULL);
			    vector srelations;
			    vector* psrelations = &srelations;
				vector_init(psrelations, sizeof(char), (size_t)0, NULL);
        		int scounter = getRangesInQuadTreeCell(psranges, psrelations, maxdepth, level, quadcode, qgeom, quad[0], quad[1], quad[2], quad[3]);
        		if (scounter == NUMELEMQTC){
        			toadd = 2;
        		}else{
        			
        			for (j=0; j<vector_length(psranges); j++) {
   						uint64_t e;
        				vector_get(psranges, j, &e);
        				vector_push(pranges, &e);
        			}
        			for (j=0; j<vector_length(psrelations); j++) {
        				char e;
        				vector_get(psrelations, j, &e);
        				vector_push(prelations, &e);
        			}
        		}
        		
    			vector_dispose(psranges);
        		vector_dispose(psrelations);
        	}
        	
        	if (toadd > 0){
        		uint64_t diff = (numbits - level) << 1;
        		uint64_t minr = quadcode << diff;
        		uint64_t maxr = ((quadcode+1) << diff) - 1;
                vector_push(pranges, &minr);
                vector_push(pranges, &maxr);
        		counter++;
        		vector_push(prelations, &toadd);
        	}
        }
    } 
    return counter;
}

int getRanges(vector* ranges, vector* relations, GEOSGeometry* qgeom){
	// Create first cell of Quad-Tree (the one in level 0 that covers all domain) 
	GEOSGeometry* dgeom = createBox(domain[0], domain[1], domain[2], domain[3]);

	// Set numlevels: automatic (if in init it was -1) or set in initialization
	int numberOfLevels;
	double darea;
	double qarea;
	if (numlevels < 0){
		double* parea = &darea;
		GEOSArea(dgeom, parea);
		parea = &qarea;
		GEOSArea(qgeom, parea);
		numberOfLevels = (int)(ceil(log2(darea/qarea)/2)) + NUMLEVELFACTOR;
	}else numberOfLevels = numlevels; // We use the one set in initialization

	// Check if query geometry overlaps with whole Quad-Tree
	int r = relation(dgeom, qgeom);
	if (r > 0){ // There is overlap
		// Call to recursive method to fill ranges though-in the Quad-Tree
		getRangesInQuadTreeCell(ranges, relations, numberOfLevels, startlevel, startquadcode, qgeom, startquad[0], startquad[1], startquad[2], startquad[3]);
		return 1;
	}else return 0;
}

int mergeConsecutiveRanges(vector* iranges, vector* oranges){
	unsigned int numRanges = ((unsigned int)vector_length(iranges)) / 2;
	if (numRanges == 0) return 0;

	uint64_t mrangemin;
	vector_get(iranges, 0, &mrangemin);
	uint64_t mrangemax;
	vector_get(iranges, 1, &mrangemax);
	
	unsigned int i;
	uint64_t cmrangemin;
	uint64_t cmrangemax;
	for (i=1; i<numRanges; i++) {
		vector_get(iranges, 2*i, &cmrangemin);
		vector_get(iranges, (2*i) + 1, &cmrangemax);
		if (mrangemax == cmrangemin - 1) mrangemax = cmrangemax;
		else{
			vector_push(oranges, &mrangemin);
			vector_push(oranges, &mrangemax);
			mrangemin = cmrangemin;
			mrangemax = cmrangemax;
		}
	}
	vector_push(oranges, &mrangemin);
	vector_push(oranges, &mrangemax);
	return 1;
}

int mergeRangesMaxRanges(vector* iranges, vector* oranges, int maxRanges){
    int i;
    
	unsigned int numRanges = ((unsigned int)vector_length(iranges)) / 2;
    
    if ((numRanges <= maxRanges) || (numRanges < 2)) return 0;

    int numRangesToMerge = numRanges - maxRanges;
    int numDiffs = numRanges - 1; 

    uint64_t* diffs = malloc(numDiffs*sizeof(uint64_t));
    uint64_t* cdiffs = malloc(numDiffs*sizeof(uint64_t));
	uint64_t a;
	uint64_t b;
    for (i = 0; i < numDiffs; i++){
    	vector_get(iranges, 2*(i+1), &a); 
    	vector_get(iranges, (2*i)+1, &b); 
        diffs[i] = a - b;
    }
    memcpy(cdiffs, diffs, numDiffs * sizeof *diffs);
    
    qsort (cdiffs, numDiffs, sizeof (uint64_t), compare64);
    uint64_t tDiff = cdiffs[numRangesToMerge-1];
    
    int lowerDiffs = 0;
    for (i = 0; i < numDiffs; i++){
        if (diffs[i] < tDiff) lowerDiffs++;	
    }
    
    int equalToMerge = numRangesToMerge - lowerDiffs;
    
    int equalCounter = 0;
    i = 0;
	uint64_t mrangemin;
	uint64_t mrangemax;
	vector_get(iranges, 2*i, &mrangemin); 
	int initrange = 0;
    for (i = 0; i < numRanges; i++){
    	if (initrange == 1){
			vector_get(iranges, 2*i, &mrangemin); 
    		initrange = 0;
    	}
    	if (i < numDiffs){
    		if (diffs[i] > tDiff){
    			vector_push(oranges, &mrangemin);
				vector_get(iranges, (2*i)+1, &mrangemax); 
    			vector_push(oranges, &mrangemax);
    			initrange = 1;
    		}else if (diffs[i] == tDiff){
    			equalCounter++;
    			if (equalCounter > equalToMerge){
    				vector_push(oranges, &mrangemin);
    				vector_get(iranges, (2*i)+1, &mrangemax); 
    				vector_push(oranges, &mrangemax);
    				initrange = 1;
    			}
    		}
    	}else{
    		vector_push(oranges, &mrangemin);
    		vector_get(iranges, (2*i)+1, &mrangemax); 
    		vector_push(oranges, &mrangemax);	
    	}
    }
    free(diffs);
    free(cdiffs);
    return 1;
}

//void processWKT(vector** pranges, vector** prelations, char* wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges){
void processWKT(vector* pranges, vector* prelations, char* wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges){
	size_t j;
	
	// Set values to the global variabales
	numlevels = numberlevels;
	maxranges = maximumranges;
	numbits = MAXBITS; //Set initial to MAX, can be decreased later
	// Define the domain (scaled and offseted)
    domain[0] = (int)((minx - offsetx) / scalex);
    domain[1] = (int)((miny - offsety) / scaley);
    domain[2] = (int)((maxx - offsetx) / scalex);
    domain[3] = (int)((maxy - offsety) / scaley);
    if ((domain[0] < 0) || (domain[1] < 0) || (domain[2] < 0) || (domain[3] < 0)) log_and_exit("domain must contain only positive X and Y 32bit integers!");
    // Get the maximum value axises values (max of max x and max y)
    int dmaxxy;
    if (domain[2] > domain[3]) dmaxxy = domain[2]; else dmaxxy = domain[3];
    // Compute the number of bits required to fit the domain
	int fitsdomain = 1;
    while (fitsdomain == 1){
        if ((ONELONG << numbits) > dmaxxy) numbits--;
        else {
            fitsdomain = 0;
            numbits++;
        }
    }
    // Check number of bits is less that maximum (31)
    if ((numbits > MAXBITS)) log_and_exit("required #bits: %d. Maximum: %d", numbits, MAXBITS);
	// If numlevels is given (if provided is > 0) we check that there are not less levels than num bits
    if (numlevels > 0)
    {
        if (numlevels > numbits) log_and_exit("numlevels (%d) must be lower or equal to the required #bits (%d)", numlevels, numbits);
    }
  	// Define the smallest Quad-Tree domain that can fit all the given domain
  	// Quad-Tree domains must be powers of 2
    uint32_t minsq = 0;
    uint32_t maxsq = ONELONG << numbits; //excluded
    startlevel = 0;
    startquadcode = 0;
    startquad[0] = minsq;
    startquad[1] = minsq;
    startquad[2] = maxsq;
    startquad[3] = maxsq; 
    
    /*printf("domain: %d %d %d %d\n", domain[0], domain[1], domain[2], domain[3]);
    printf("#bits: %d\n", numbits); 
    printf("#levels: %d\n", numlevels);
    printf("startlevel: %d\n", startlevel);
    printf("startquadcode: %lu\n", startquadcode);
    printf("startquad: %u %u %u %u\n", startquad[0], startquad[1], startquad[2], startquad[3]);
	*/
	char* pwkt;
	if (!double_equals(offsetx,0) || !double_equals(offsety,0) || !double_equals(scalex,1) || !double_equals(scaley,1)) pwkt = scale(wkt, scalex, scaley, offsetx, offsety);
	else pwkt = wkt;

	GEOSWKTReader* geomReader;
	geomReader = GEOSWKTReader_create();
	GEOSGeometry* qgeom = GEOSWKTReader_read(geomReader, pwkt);
    
    vector ranges1;
    vector* pranges1 = &ranges1;
	vector_init(pranges1, sizeof(uint64_t), (size_t)0, NULL);
    vector relations1;
    vector* prelations1 = &relations1;
	vector_init(prelations1, sizeof(char), (size_t)0, NULL);
    
	getRanges(pranges1, prelations1, qgeom);
	
	GEOSGeom_destroy(qgeom);
	
	vector ranges2;
    vector* pranges2 = &ranges2;
	vector_init(pranges2, sizeof(uint64_t), (size_t)0, NULL);
	vector ranges3;
    vector* pranges3 = &ranges3;
	vector_init(pranges3, sizeof(uint64_t), (size_t)0, NULL);
	int status2 = 0;
	int status3 = 0;
	if ((mergeconsecutive > 0) || (maxranges > 0)){
		status2 = mergeConsecutiveRanges(pranges1, pranges2);
		if (maxranges > 0) status3 = mergeRangesMaxRanges(pranges2, pranges3, maxranges);
	}
    
    vector* rranges;
    vector* rrelations;
	if (status3 == 1){
		//*pranges = pranges3;
		rranges = pranges3;
		vector_dispose(pranges1);
		vector_dispose(pranges2);
	}
	else if (status2 == 1){
		//*pranges = pranges2;
		rranges = pranges2;
		vector_dispose(pranges1);
		vector_dispose(pranges3);
	}
	else{
		//*pranges = pranges1;
		rranges = pranges1;
		//*prelations = prelations1;
		rrelations = prelations1;
		for (j=0; j<vector_length(rrelations); j++) {
			char e;
			vector_get(rrelations, j, &e);
			vector_push(prelations, &e);
		}
		vector_dispose(pranges2);
		vector_dispose(pranges3);
	}
	
	for (j=0; j<vector_length(rranges); j++) {
		uint64_t e;
		vector_get(rranges, j, &e);
		vector_push(pranges, &e);
	}
	//TODO: do not duplicate output vector like we are doing now...solve with pointer to pointer (tried but not worked)
}

uint64_t* getRangesWKT(int* onumranges, char* wkt, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges){
    initGEOS(notice, log_and_exit);
    //vector* pranges = NULL;
	//vector* prelations = NULL;
	//processWKT(&pranges, &prelations, wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    
	vector ranges;
    vector* pranges = &ranges;
	vector_init(pranges, sizeof(uint64_t), (size_t)0, NULL);
    vector relations;
    vector* prelations = &relations;
	vector_init(prelations, sizeof(char), (size_t)0, NULL);
    processWKT(pranges, prelations, wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    finishGEOS();
    //return *pranges;
    
    *onumranges = ((int)vector_length(pranges)) / 2;
    uint64_t* retranges = malloc(sizeof(uint64_t) * vector_length(pranges));
    int j;
    for (j=0; j<vector_length(pranges); j++) {
		uint64_t e;
		vector_get(pranges, j, &e);
		retranges[j] = e;
	}
    return retranges;
}

uint64_t* getRangesFile(int* onumranges, char* filepath, double minx, double miny, double maxx, double maxy, double scalex, double scaley, double offsetx, double offsety, int numberlevels, int mergeconsecutive, int maximumranges){
    initGEOS(notice, log_and_exit);
	char* wkt = readWKT(filepath);
	//vector* pranges = NULL;
	//vector* prelations = NULL;
	//processWKT(&pranges, &prelations, wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    
	vector ranges;
    vector* pranges = &ranges;
	vector_init(pranges, sizeof(uint64_t), (size_t)0, NULL);
    vector relations;
    vector* prelations = &relations;
	vector_init(prelations, sizeof(char), (size_t)0, NULL);
    processWKT(pranges, prelations, wkt, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    
	free(wkt);
    finishGEOS();
    //return *pranges;
    
    *onumranges = ((int)vector_length(pranges)) / 2;
    uint64_t* retranges = malloc(sizeof(uint64_t) * vector_length(pranges));
    int j;
    for (j=0; j<vector_length(pranges); j++) {
		uint64_t e;
		vector_get(pranges, j, &e);
		retranges[j] = e;
	}
    return retranges;
}

int main(int argc, char **argv){
	// Set the global variables to the default values
	char* filepath;
    double minx, miny, maxx, maxy;
    double scalex, scaley, offsetx, offsety;
    int numberlevels = -1;
    int maximumranges = -1;
    int mergeconsecutive = -1;
    
    if ((argc < 10) || (argc > 13)) usage(argv[0]);
    filepath = argv[1];
    minx = atof(argv[2]);
    miny = atof(argv[3]);
    maxx = atof(argv[4]);
    maxy = atof(argv[5]);
    scalex = atof(argv[6]);
    scaley = atof(argv[7]);
    offsetx = atof(argv[8]);
    offsety = atof(argv[9]);
    if (argc > 10) numberlevels = atoi(argv[10]);
    if (argc > 11) mergeconsecutive = atoi(argv[11]);
    if (argc > 12) maximumranges = atoi(argv[12]);
    
    int onumranges;
    uint64_t* ranges = getRangesFile(&onumranges,filepath, minx, miny, maxx, maxy, scalex, scaley, offsetx, offsety, numberlevels, mergeconsecutive, maximumranges);
    
    int j;
    for (j = 0; j < onumranges; j++){
    	//vector_get(&ranges, 2*j, &minrange);
    	//vector_get(&ranges, (2*j) + 1, &maxrange);
    	//printf("  %ld %ld\n", minrange, maxrange);
    	printf("  %ld %ld\n", ranges[2*j], ranges[(2*j)+1]);
	}
}