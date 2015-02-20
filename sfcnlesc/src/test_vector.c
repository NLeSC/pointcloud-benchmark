#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <geos_c.h>
#include <inttypes.h>
#include <ctype.h>
#include <math.h> 
#include "vector.h"

int compare (const void * a, const void * b)
{
	if ( *(uint64_t*)a <  *(uint64_t*)b ) return -1;
  	if ( *(uint64_t*)a == *(uint64_t*)b ) return 0;
  	if ( *(uint64_t*)a >  *(uint64_t*)b ) return 1;
}

int main(int argc, char **argv){
    vector v1;
    vector* pv1;
	pv1 = &v1;
    fprintf(stderr, "HERE\n");
	vector_init(pv1, sizeof(uint64_t), (size_t)0, NULL);
    fprintf(stderr, "HERE\n");
	
    uint64_t* x;
    x = malloc(sizeof(uint64_t));
    *x = 3;
    vector_push(pv1, x);
    
    x = malloc(sizeof(uint64_t));
    *x = 1;
    vector_push(pv1, x);
    
    x = malloc(sizeof(uint64_t));
    *x = 2;
    vector_push(pv1, x);

    uint64_t a = 4;
    vector_push(pv1, &a);
    

    size_t i;
    
    for (i = 0; i < vector_length(pv1); i++){
        vector_get(pv1, i, x);
        fprintf(stderr, "%ld\n", *x);
    }
    
    vector v2;
    vector* pv2 = &v2;
    vector_init(pv2, sizeof(uint64_t), (size_t)0, NULL);
    vector_copy(pv1, pv2);
    
    vector_qsort(pv2, compare);
    
    for (i = 0; i < vector_length(pv2); i++){
        vector_get(pv2, i, x);
        fprintf(stderr, "%ld\n", *x);
    }
    
    vector_dispose(pv1);
    //free(pv1);
    vector_dispose(pv2);
    //free(pv2);
}