package ca.spottedleaf.concurrentutil.map.primitive;

import ca.spottedleaf.concurrentutil.lock.WeakSeqLock;
import ca.spottedleaf.concurrentutil.util.ArrayUtil;
import java.util.function.Predicate;

public class SingleWriterMultiReaderIntObjectOpenHashMapSeqLock<V> extends SingleWriterMultiReaderIntObjectOpenHashMap<V> {

    /* used to implement remove()/get() */
    protected final WeakSeqLock mapLock = new WeakSeqLock();

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock() {
        super();
    }

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final int capacity) {
        super(capacity);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final int capacity, final float loadFactor) {
        super(capacity, loadFactor);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map) {
        super(map);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity) {
        super(map, capacity);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity, final float loadFactor) {
        super(map, capacity, loadFactor);
    }

    protected SingleWriterMultiReaderIntObjectOpenHashMapSeqLock(final SingleWriterMultiReaderIntObjectOpenHashMapSeqLock<V> other) {
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
        final int index = hash & capacityMask;

        int readLock;
        int currIndex;

search_loop:
        for (;;) {
            readLock = this.mapLock.acquireRead();
            for (currIndex = index;;currIndex = (currIndex + 1) & capacityMask) {
                final int currKey = ArrayUtil.getOpaque(keys, currIndex);

                if (currKey == 0) {
                    if (!this.mapLock.tryReleaseRead(readLock)) {
                        continue search_loop;
                    }
                    return null;
                }

                if (key != currKey) {
                    continue;
                }

                final V value = ArrayUtil.getAcquire(values, currIndex);
                if (!this.mapLock.tryReleaseRead(readLock)) {
                    continue search_loop;
                }

                return value;
            }
        }
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

            /* We test if the next key is empty to avoid unneeded lock contention */
            if (ArrayUtil.getPlain(keys, nextIndex) == 0) {
                // entry after is NULL, see reEvaluateEntries for why we can do this.
                ArrayUtil.setPlain(keys, currIndex, 0);
                ArrayUtil.setRelease(values, currIndex, null);
                this.removeFromSize(1);
                return ret;
            }

            this.mapLock.acquireWrite();

            if (this.removeEntryAt(keys, values, capacityMask, currIndex)) {
                this.mapLock.releaseWrite();
            } else {
                this.mapLock.abortWrite();
            }

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
    public SingleWriterMultiReaderIntObjectOpenHashMapSeqLock<V> clone() {
        return new SingleWriterMultiReaderIntObjectOpenHashMapSeqLock<>(this);
    }
}