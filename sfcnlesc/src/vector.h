/**
 * Hamid Alipour
 */
 
#ifndef __VECTORH__
#define __VECTORH__
 
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
 
#define VECTOR_INIT_SIZE    4
#define VECTOR_HASSPACE(v)  (((v)->num_elems + 1) <= (v)->num_alloc_elems)
#define VECTOR_INBOUNDS(i)	(((int) i) >= 0 && (i) < (v)->num_elems)
#define VECTOR_INDEX(i)		((char *) (v)->elems + ((v)->elem_size * (i)))
 
typedef struct _vector {
	void *elems;
	size_t elem_size;
	size_t num_elems;
	size_t num_alloc_elems;
    void (*free_func)(void *);
} vector;
 
extern void vector_init(vector *, size_t, size_t, void (*free_func)(void *));
extern void vector_dispose(vector *);
extern void vector_copy(vector *, vector *);
extern void vector_insert(vector *, void *, size_t index);
extern void vector_insert_at(vector *, void *, size_t index);
extern void vector_push(vector *, void *);
extern void vector_pop(vector *, void *);
extern void vector_shift(vector *, void *);
extern void vector_unshift(vector *, void *);
extern void vector_get(vector *, size_t, void *);
extern void vector_remove(vector *, size_t);
extern void vector_transpose(vector *, size_t, size_t);
extern size_t vector_length(vector *);
extern size_t vector_size(vector *);
extern void vector_get_all(vector *, void *);
extern void vector_cmp_all(vector *, void *, int (*cmp_func)(const void *, const void *));
extern void vector_qsort(vector *, int (*cmp_func)(const void *, const void *));
static void vector_grow(vector *, size_t);
static void vector_swap(void *, void *, size_t);
 
#endif