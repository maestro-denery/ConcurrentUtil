package ca.spottedleaf.concurrentutil.map.primitive;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.lock.WeakSeqLock;
import ca.spottedleaf.concurrentutil.util.ArrayUtil;

import java.lang.invoke.VarHandle;
import java.util.function.Predicate;

public class SingleWriterMultiWriterIntObjectOpenHashMapFullSearch<V> extends SingleWriterMultiReaderIntObjectOpenHashMap<V> {

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch() {
        super();
    }

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final int capacity) {
        super(capacity);
    }

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final int capacity, final float loadFactor) {
        super(capacity, loadFactor);
    }

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map) {
        super(map);
    }

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity) {
        super(map, capacity);
    }

    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity, final float loadFactor) {
        super(map, capacity, loadFactor);
    }

    protected SingleWriterMultiWriterIntObjectOpenHashMapFullSearch(final SingleWriterMultiWriterIntObjectOpenHashMapFullSearch<V> other) {
        super(other.keys.clone(), other.values.clone(), other.zeroKeyValue, other.size, other.threshold, other.loadFactor);
    }

    @Override
    public V get(final int key) {
        if (key == 0) {
            return this.getZeroKeyValueAcquire();
        }

        int[] keys;
        V[] values;
        int capacity;
        int capacityMask;

        for (;;) {
            keys = this.getKeysPlain();
            values = this.getValuesAcquire();
            capacity = keys.length;
            capacityMask = capacity - 1;
            if (capacity == values.length) {
                break;
            }
        }

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMapSeqLock.hash(key);
        final int startIndex = hash & capacityMask;

        /* First find the divisor for the group */

        int divisorIndex = startIndex;

        for (;; divisorIndex = (divisorIndex + 1) & capacityMask) {
            if (ArrayUtil.getOpaque(keys, divisorIndex) == 0) {
                break;
            }
        }

        if (divisorIndex == startIndex) {
            return null;
        }

        /* Now we need to search from the divisor index to the start index (backwards) */

        main_loop:
        for (int endIndex = (startIndex - 1) & capacityMask, currIndex = divisorIndex; currIndex != endIndex; currIndex = (currIndex - 1) & capacityMask) {
            /* Note: The read style here is the same as in the reference's forEachEntry() method */
            int lastKey = ArrayUtil.getAcquire(keys, currIndex); // acquire is needed to avoid re-ordering with later reads
            V lastVal = ArrayUtil.getAcquire(values, currIndex);
            //VarHandle.loadLoadFence(); // acquire acts as this fence here

            if (lastKey != key) {
                continue;
            }

            for (;;) {
                final int keyCheck = ArrayUtil.getOpaque(keys, currIndex);
                final V valueCheck = ArrayUtil.getAcquire(values, currIndex);
                //VarHandle.loadLoadFence(); // acquire acts as this fence here

                final int tempKey = lastKey;
                final V tempVal = lastVal;

                lastKey = keyCheck;
                lastVal = valueCheck;

                // make sure we update key/value if they change (otherwise we would loop an extra time)
                if (tempKey != keyCheck || tempVal != valueCheck || lastVal == null) {
                    ConcurrentUtil.pause();
                    continue;
                }

                if (keyCheck != key) {
                    continue main_loop;
                }

                return valueCheck;
            }
        }

        return null;
    }

    @Override
    public final V remove(final int key) {
        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();
            this.setZeroKeyValueRelease(null);
            return prev;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMapSeqLock.hash(key);
        final int index = hash & capacityMask;

        for (int currIndex = index;;) {
            /* threshold < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                this.removeFromSize(1);
                return null;
            }

            final int nextIndex = (currIndex + 1) & capacityMask;

            if (currKey != key) {
                currIndex = nextIndex;
                continue;
            }

            final V ret = ArrayUtil.getPlain(values, currIndex);

            /* We effectively need to re-insert elements below this one until a divisor is reached */

            this.removeEntryAt(keys, values, capacityMask, currIndex);

            this.removeFromSize(1);
            return ret;
        }
    }

    @Override
    public boolean remove(final int key, final V value) {
        return false; // TODO
    }

    @Override
    public boolean removeIf(final int key, final Predicate<V> predicate) {
        return false; // TODO
    }

    @Override
    public V compute(final int key, final IntObjectObjectProducer<V> function) {
        return null; // TODO
    }

    @Override
    public V computeIfAbsent(final int key, final IntObjectProducer<V> function) {
        return null; // TODO
    }

    @Override
    public V computeIfPresent(final int key, final IntObjectObjectProducer<V> function) {
        return null; // TODO
    }

    @Override
    public SingleWriterMultiWriterIntObjectOpenHashMapFullSearch<V> clone() {
        return new SingleWriterMultiWriterIntObjectOpenHashMapFullSearch<>(this);
    }
}
