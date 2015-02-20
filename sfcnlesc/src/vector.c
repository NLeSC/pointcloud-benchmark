/**
 * Hamid Alipour
 */
 
#include "vector.h"
 
extern void vector_init(vector *v, size_t elem_size, size_t init_size, void (*free_func)(void *))
{
	v->elem_size = elem_size;
	v->num_alloc_elems = (int) init_size > 0 ? init_size : VECTOR_INIT_SIZE;
	v->num_elems = 0;
	v->elems = malloc(elem_size * v->num_alloc_elems);
	assert(v->elems != NULL);
	v->free_func = free_func != NULL ? free_func : NULL;
}
 
extern void vector_dispose(vector *v)
{
	size_t i;
 
	if (v->free_func != NULL) {
		for (i = 0; i < v->num_elems; i++) {
			v->free_func(VECTOR_INDEX(i));
		}
	}
 
	free(v->elems);
}
 
 
extern void vector_copy(vector *v1, vector *v2)
{
	v2->num_elems = v1->num_elems;
	v2->num_alloc_elems = v1->num_alloc_elems;
	v2->elem_size = v1->elem_size;
 
	v2->elems = realloc(v2->elems, v2->num_alloc_elems * v2->elem_size);
	assert(v2->elems != NULL);
 
	memcpy(v2->elems, v1->elems, v2->num_elems * v2->elem_size);
}
 
extern void vector_insert(vector *v, void *elem, size_t index)
{
	void *target;
 
	if ((int) index > -1) {
		if (!VECTOR_INBOUNDS(index))
			return;
		target = VECTOR_INDEX(index);
	} else {
		if (!VECTOR_HASSPACE(v))
			vector_grow(v, 0);
		target = VECTOR_INDEX(v->num_elems);
		v->num_elems++; /* Only grow when adding a new item not when inserting in a spec indx */
	}
 
	memcpy(target, elem, v->elem_size);
}
 
extern void vector_insert_at(vector *v, void *elem, size_t index)
{
	if ((int) index < 0)
		return;
 
	if (!VECTOR_HASSPACE(v))
		vector_grow(v, 0);
 
	if (index < v->num_elems)
		memmove(VECTOR_INDEX(index + 1), VECTOR_INDEX(index), (v->num_elems - index) * v->elem_size);
 
	/* 1: we are passing index so insert won't increment this 2: insert checks INBONDS... */
	v->num_elems++;
 
	vector_insert(v, elem, index);
}
 
extern void vector_push(vector *v, void *elem)
{
	vector_insert(v, elem, -1);
}
 
extern void vector_pop(vector *v, void *elem)
{
	memcpy(elem, VECTOR_INDEX(v->num_elems - 1), v->elem_size);
	v->num_elems--;
}
 
extern void vector_shift(vector *v, void *elem)
{
	memcpy(elem, v->elems, v->elem_size);
	memmove(VECTOR_INDEX(0), VECTOR_INDEX(1), v->num_elems * v->elem_size);
 
	v->num_elems--;
}
 
extern void vector_unshift(vector *v, void *elem)
{
	if (!VECTOR_HASSPACE(v))
		vector_grow(v, v->num_elems + 1);
 
	memmove(VECTOR_INDEX(1), v->elems, v->num_elems * v->elem_size);
	memcpy(v->elems, elem, v->elem_size);
 
	v->num_elems++;
}
 
extern void vector_transpose(vector *v, size_t index1, size_t index2)
{
	vector_swap(VECTOR_INDEX(index1), VECTOR_INDEX(index2), v->elem_size);
}
 
static void vector_grow(vector *v, size_t size)
{
	if (size > v->num_alloc_elems)
		v->num_alloc_elems = size;
	else
		v->num_alloc_elems *= 2;
 
	v->elems = realloc(v->elems, v->elem_size * v->num_alloc_elems);
	assert(v->elems != NULL);
}
 
extern void vector_get(vector *v, size_t index, void *elem)
{
	assert((int) index >= 0);
 
	if (!VECTOR_INBOUNDS(index)) {
		elem = NULL;
		return;
	}
 
	memcpy(elem, VECTOR_INDEX(index), v->elem_size);
}
 
extern void vector_get_all(vector *v, void *elems)
{
	memcpy(elems, v->elems, v->num_elems * v->elem_size);
}
 
extern void vector_remove(vector *v, size_t index)
{
	assert((int) index > 0);
 
	if (!VECTOR_INBOUNDS(index))
		return;
 
	memmove(VECTOR_INDEX(index), VECTOR_INDEX(index + 1), v->elem_size);
	v->num_elems--;
}
 
extern void vector_remove_all(vector *v)
{
	v->num_elems = 0;
	v->elems = realloc(v->elems, v->num_alloc_elems);
	assert(v->elems != NULL);
}
 
extern size_t vector_length(vector *v)
{
	return v->num_elems;
}
 
extern size_t vector_size(vector *v)
{
	return v->num_elems * v->elem_size;
}
 
extern void vector_cmp_all(vector *v, void *elem, int (*cmp_func)(const void *, const void *))
{
	size_t i;
	void *best_match = VECTOR_INDEX(0);
 
	for (i = 1; i < v->num_elems; i++)
		if (cmp_func(VECTOR_INDEX(i), best_match) > 0)
			best_match = VECTOR_INDEX(i);
 
	memcpy(elem, best_match, v->elem_size);
}
 
extern void vector_qsort(vector *v, int (*cmp_func)(const void *, const void *))
{
	qsort(v->elems, v->num_elems, v->elem_size, cmp_func);
}
 
static void vector_swap(void *elemp1, void *elemp2, size_t elem_size)
{
	void *tmp = malloc(elem_size);
 
	memcpy(tmp, elemp1, elem_size);
	memcpy(elemp1, elemp2, elem_size);
	memcpy(elemp2, tmp, elem_size);
 
    free(tmp); /* Thanks to gromit */
}