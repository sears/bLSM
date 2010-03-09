#include "memTreeComponent.h"
template<class TUPLE>
void memTreeComponent<TUPLE>::tearDownTree(rbtree_ptr_t tree) {
    TUPLE * t = 0;
    typename rbtree_t::iterator old;
    for(typename rbtree_t::iterator delitr  = tree->begin();
                           delitr != tree->end();
                           delitr++) {
    	if(t) {
    		tree->erase(old);
    		TUPLE::freetuple(t);
    		t = 0;
    	}
    	t = *delitr;
    	old = delitr;
    }
	if(t) {
		tree->erase(old);
		TUPLE::freetuple(t);
	}
    delete tree;
}

template class memTreeComponent<datatuple>;
