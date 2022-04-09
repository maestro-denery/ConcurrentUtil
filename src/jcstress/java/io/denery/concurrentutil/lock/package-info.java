/**
 * This package tests lock mechanisms in leaf's concurrent util in some cases.
 * <p>
 * Conclusions:
 * <p>
 * 1. WeakSeqLock doesn't allow multi-writer case, it stops execution, but VolatileSeqLock allows it.
 * 2. None of the implementation allow multi-reader case.
 */
package io.denery.concurrentutil.lock;