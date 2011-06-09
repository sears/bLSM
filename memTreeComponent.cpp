#include "memTreeComponent.h"
#include "datatuple.h"

void memTreeComponent::tearDownTree(rbtree_ptr_t tree) {
    datatuple * t = 0;
    typename rbtree_t::iterator old;
    for(typename rbtree_t::iterator delitr  = tree->begin();
                           delitr != tree->end();
                           delitr++) {
    	if(t) {
    		tree->erase(old);
    		datatuple::freetuple(t);
    		t = 0;
    	}
    	t = *delitr;
    	old = delitr;
    }
	if(t) {
		tree->erase(old);
		datatuple::freetuple(t);
	}
    delete tree;
}
