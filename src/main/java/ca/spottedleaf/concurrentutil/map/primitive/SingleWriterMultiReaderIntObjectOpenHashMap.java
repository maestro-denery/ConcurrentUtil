package ca.spottedleaf.concurrentutil.map.primitive;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.ArrayUtil;
import ca.spottedleaf.concurrentutil.util.IntegerUtil;
import ca.spottedleaf.concurrentutil.util.Validate;

import java.lang.invoke.VarHandle;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

public abstract class SingleWriterMultiReaderIntObjectOpenHashMap<V> implements Cloneable {

    protected int[] keys;

    protected V[] values;

    protected V zeroKeyValue;

    /** size excluding zero key value */
    protected int size;

    protected int threshold;

    protected final float loadFactor;

    protected static final int DEFAULT_CAPACITY = 32;
    protected static final int MAXIMUM_CAPACITY = Integer.MIN_VALUE >>> 1;
    protected static final float DEFAULT_LOAD_FACTOR = 0.75f;

    protected static int hash(final int x) {
        return IntegerUtil.hash0(x);
    }

    protected SingleWriterMultiReaderIntObjectOpenHashMap(final int[] keys, final V[] values, final V zeroKeyValue,
                                                          final int size, final int threshold, final float loadFactor) {
        this.keys = keys;
        this.values = values;
        this.zeroKeyValue = zeroKeyValue;
        this.size = size;
        this.threshold = threshold;
        this.loadFactor = loadFactor;
        VarHandle.releaseFence();
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap() {
        this(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap(final int capacity) {
        this(capacity, DEFAULT_LOAD_FACTOR);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap(final int capacity, final float loadFactor) {
        if (capacity <= 1) {
            throw new IllegalArgumentException("Capacity is invalid (must be greater-than 1): " + capacity);
        }

        final int realCapacity;
        if (capacity >= MAXIMUM_CAPACITY) {
            realCapacity = MAXIMUM_CAPACITY;
        } else {
            realCapacity = IntegerUtil.roundCeilLog2(capacity);
        }

        if (loadFactor <= 0.0f || loadFactor > 1.0f || !Float.isFinite(loadFactor)) {
            throw new IllegalArgumentException("Invalid load factor (must be in (0.0, 1.0] : " + loadFactor);
        }
        this.threshold = realCapacity == MAXIMUM_CAPACITY ? -1 : Math.min(realCapacity - 2, (int)(loadFactor * realCapacity));

        final V[] values = (V[])new Object[realCapacity];
        final int[] keys = new int[realCapacity];

        this.loadFactor = loadFactor;
        this.setKeysPlain(keys);
        this.setValuesRelease(values);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map) {
        this(map, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity) {
        this(map, capacity, DEFAULT_LOAD_FACTOR);
    }

    public SingleWriterMultiReaderIntObjectOpenHashMap(final SingleWriterMultiReaderIntObjectOpenHashMap<V> map, final int capacity, final float loadFactor) {
        this(Math.max(capacity, map.getSizeOpaque()), loadFactor);
        map.forEachEntry(this::putRelaxed);
        VarHandle.releaseFence();
    }

    protected final Object rawClone() throws CloneNotSupportedException {
        return super.clone();
    }

    /* interfaces for subclasses */

    public abstract V get(final int key);

    public final V getOrDefault(final int key, final V dfl) {
        final V value = this.get(key);
        return value == null ? dfl : value;
    }

    public abstract V remove(final int key);
    public abstract boolean remove(final int key, final V value);
    public abstract boolean removeIf(final int key, final Predicate<V> predicate);

    public abstract V compute(final int key, final IntObjectObjectProducer<V> function);
    public abstract V computeIfAbsent(final int key, final IntObjectProducer<V> function);
    public abstract V computeIfPresent(final int key, final IntObjectObjectProducer<V> function);

    protected final void removeFromSizeRelaxed(final int num) {
        this.setSizePlain(this.getSizePlain() - num);
    }

    protected final void removeFromSize(final int num) {
        this.setSizeOpaque(this.getSizePlain() - num); // diff from relaxed: opaque write
    }

    protected final void addToSizeRelaxed(final int num) {
        final int prevSize = this.getSizePlain();
        final int newSize = prevSize + num;
        if (newSize < 0 || newSize >= MAXIMUM_CAPACITY) { // overflow detection
            throw new IllegalStateException("too many entries: curr size: " + this.getSizePlain() + ", adding: " + num);
        }
        this.setSizePlain(newSize);

        if (newSize >= (MAXIMUM_CAPACITY - 2)) {
            throw new IllegalStateException("Maximum capacity reached: " + newSize);
        }

        if (this.threshold == -1 || newSize < this.threshold) {
            return;
        }

        // now we must rebuild the table

        int nextCapacity = IntegerUtil.roundCeilLog2(newSize);

        if (nextCapacity < 0) {
            nextCapacity = MAXIMUM_CAPACITY;
        }

        final int[] currKeys = this.getKeysPlain();
        final V[] currValues = this.getValuesPlain();

        final int[] nextKeys = new int[nextCapacity];
        //noinspection unchecked
        final V[] nextValues = (V[])new Object[nextCapacity];

        SingleWriterMultiReaderIntObjectOpenHashMap.copyTable(nextKeys, nextValues, currKeys, currValues);

        if (nextCapacity == MAXIMUM_CAPACITY) {
            this.threshold = -1; /* No more resizing */
        } else {
            this.threshold = Math.min(nextCapacity - 2, (int)(nextCapacity * (double)this.loadFactor));
        }

        this.setKeysPlain(nextKeys);
        this.setValuesPlain(nextValues);
    }

    protected final void addToSize(final int num) {
        final int prevSize = this.getSizePlain();
        final int newSize = prevSize + num;
        if (newSize < 0 || newSize >= MAXIMUM_CAPACITY) { // overflow detection
            throw new IllegalStateException("too many entries: curr size: " + this.getSizePlain() + ", adding: " + num);
        }
        this.setSizePlain(newSize);

        if (newSize >= (MAXIMUM_CAPACITY - 2)) {
            throw new IllegalStateException("Maximum capacity reached: " + newSize);
        }

        if (this.threshold == -1 || newSize < this.threshold) {
            return;
        }

        // now we must rebuild the table

        int nextCapacity = IntegerUtil.roundCeilLog2(newSize);

        if (nextCapacity < 0) {
            nextCapacity = MAXIMUM_CAPACITY;
        }

        final int[] currKeys = this.getKeysPlain();
        final V[] currValues = this.getValuesPlain();

        final int[] nextKeys = new int[nextCapacity];
        //noinspection unchecked
        final V[] nextValues = (V[])new Object[nextCapacity];

        SingleWriterMultiReaderIntObjectOpenHashMap.copyTable(nextKeys, nextValues, currKeys, currValues);

        if (nextCapacity == MAXIMUM_CAPACITY) {
            this.threshold = -1; /* No more resizing */
        } else {
            this.threshold = Math.min(nextCapacity - 2, (int)(nextCapacity * (double)this.loadFactor));
        }

        this.setKeysPlain(nextKeys);
        this.setValuesRelease(nextValues); // diff from relaxed: release write
    }

    public final float getLoadFactor() {
        return this.loadFactor;
    }

    public final boolean isEmpty() {
        return this.getSizeOpaque() == 0 && this.getZeroKeyValueOpaque() == null;
    }

    public final int size() {
        if (this.getZeroKeyValueOpaque() != null) {
            return this.getSizeOpaque() + 1;
        } else {
            return this.getSizeOpaque();
        }
    }

    public final V getRelaxed(final int key) {
        if (key == 0) {
            return this.getZeroKeyValuePlain();
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

        for (int currIndex = hash & capacityMask;;currIndex = (currIndex + 1) & capacityMask) {
            /* threshold < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                return null;
            }

            if (currKey == key) {
                return ArrayUtil.getPlain(values, currIndex);
            }
        }
    }

    public final V getOrDefaultRelaxed(final int key, final V dfl) {
        final V value = this.getRelaxed(key);
        return value == null ? dfl : value;
    }

    public final V put(final int key, final V value) {
        Validate.notNull(value, "Value may not be null");

        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();
            this.setZeroKeyValueRelease(value); // diff from relaxed: release write
            return prev;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

        for (int currIndex = hash & capacityMask;;) {
            /* threshold + 1 < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                ArrayUtil.setPlain(values, currIndex, value);
                ArrayUtil.setRelease(keys, currIndex, key); // diff from relaxed: release write

                this.addToSize(1); // diff from relaxed: non-relaxed addToSize
                return null;
            }

            if (currKey == key) {
                final V prev = ArrayUtil.getPlain(values, currIndex);
                ArrayUtil.setRelease(values, currIndex, value); // diff from relaxed: release write
                return prev;
            }

            currIndex = (currIndex + 1) & capacityMask;
        }
    }

    public final V putRelaxed(final int key, final V value) {
        Validate.notNull(value, "Value may not be null");

        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();
            this.setZeroKeyValuePlain(value);
            return prev;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

        for (int currIndex = hash & capacityMask;;) {
            /* threshold + 1 < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                ArrayUtil.setPlain(values, currIndex, value);
                ArrayUtil.setPlain(keys, currIndex, key);

                this.addToSizeRelaxed(1);
                return null;
            }

            if (currKey == key) {
                final V prev = ArrayUtil.getPlain(values, currIndex);
                ArrayUtil.setPlain(values, currIndex, value);
                return prev;
            }

            currIndex = (currIndex + 1) & capacityMask;
        }
    }

    public final V putIfAbsent(final int key, final V value) {
        Validate.notNull(value, "Value may not be null");

        if (key == 0) {
            final V curr = this.getZeroKeyValuePlain();
            if (curr == null) {
                this.setZeroKeyValueRelease(value); // diff from relaxed: release write
                return curr;
            }
            return curr;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

        for (int currIndex = hash & capacityMask;;) {
            /* threshold < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                ArrayUtil.setPlain(values, currIndex, value);
                ArrayUtil.setRelease(keys, currIndex, key); // diff from relaxed: release write

                this.addToSize(1); // diff from relaxed: non-relaxed addToSize
                return null;
            }

            if (currKey == key) {
                return ArrayUtil.getPlain(values, currIndex);
            }

            currIndex = (currIndex + 1) & capacityMask;
        }
    }

    public final V putIfAbsentRelaxed(final int key, final V value) {
        Validate.notNull(value, "Value may not be null");

        if (key == 0) {
            final V curr = this.getZeroKeyValuePlain();
            if (curr == null) {
                this.setZeroKeyValuePlain(value);
                return curr;
            }
            return curr;
        }

        final int[] keys = this.getKeysPlain();
        final V[] values = this.getValuesPlain();
        final int capacity = keys.length;
        final int capacityMask = capacity - 1;

        final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

        for (int currIndex = hash & capacityMask;;) {
            /* threshold < table length so we must eventually find 0 */
            final int currKey = ArrayUtil.getPlain(keys, currIndex);

            if (currKey == 0) {
                ArrayUtil.setPlain(values, currIndex, value);
                ArrayUtil.setPlain(keys, currIndex, key);

                this.addToSizeRelaxed(1);
                return null;
            }

            if (currKey == key) {
                return ArrayUtil.getPlain(values, currIndex);
            }

            currIndex = (currIndex + 1) & capacityMask;
        }
    }

    protected final void removeEntryAtRelaxed(final int[] keys, final V[] values, final int capacityMask, final int index) {
        for (int lastMigrated = index, currIndex = ((index + 1) & capacityMask);;currIndex = ((currIndex + 1) & capacityMask)) {
            final int currKey = keys[currIndex];
            if (currKey == 0) {
                ArrayUtil.setPlain(values, lastMigrated, null);
                ArrayUtil.setPlain(keys, lastMigrated, 0);

                return;
            }

            final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(currKey);
            final int expectedIndex = hash & capacityMask;

            // Ensure that if we move this element to 'lastMigrated', that it is "reachable"
            // reachable: it's index in the map is after (including wrapped, that is, 0 is after capacityMask) or equal
            // to its expectedIndex (expectedIndex = hash & capacityMask)

            // if we're not dealing with wrapped indices:
            // reachable = map index >= expected index
            // if we are dealing with wrapped indices:
            // reachable = map index >= expected index && expectedIndex >= startIndex
            // the second condition ensures we do not move, for example:
            // lastMigrated = map index
            // currIndex < lastMigrated (wrapped)
            // currIndex is a key with expectedIndex 0
            // without the second condition we would move the key, but it would be unreachable.
            // TODO verify this
            if (lastMigrated >= expectedIndex && (currIndex >= lastMigrated || expectedIndex >= index)) {
                ArrayUtil.setPlain(values, lastMigrated, ArrayUtil.getPlain(values, currIndex));
                ArrayUtil.setPlain(keys, lastMigrated, currKey);
                lastMigrated = currIndex;
            }
            /*
            if (currIndex < lastMigrated) {
                // we've wrapped
                if (lastMigrated >= expectedIndex && expectedIndex >= index) {

                }
            } else {
                // not wrapped
                if (lastMigrated >= expectedIndex) {
                    // the element would be reachable from this position

                }

            }*/
        }
    }

    protected final boolean removeEntryAt(final int[] keys, final V[] values, final int capacityMask, final int index) {
        boolean modified = false;
        for (int lastMigrated = index, currIndex = ((index + 1) & capacityMask);;currIndex = ((currIndex + 1) & capacityMask)) {
            final int currKey = keys[currIndex];
            if (currKey == 0) {
                ArrayUtil.setPlain(keys, lastMigrated, 0);
                ArrayUtil.setRelease(values, lastMigrated, null);

                return modified;
            }

            final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(currKey);
            final int expectedIndex = hash & capacityMask;

            // Ensure that if we move this element to 'lastMigrated', that it is "reachable"
            // reachable: it's index in the map is after (including wrapped, that is, 0 is after capacityMask) or equal
            // to its expectedIndex (expectedIndex = hash & capacityMask)

            // if we're not dealing with wrapped indices:
            // reachable = map index >= expected index
            // if we are dealing with wrapped indices:
            // reachable = map index >= expected index && expectedIndex >= startIndex
            // the second condition ensures we do not move, for example:
            // lastMigrated = map index
            // currIndex < lastMigrated (wrapped)
            // currIndex is a key with expectedIndex 0
            // without the second condition we would move the key, but it would be unreachable.
            // TODO verify this
            if (lastMigrated >= expectedIndex && (currIndex >= lastMigrated || expectedIndex >= index)) {
                modified = true;
                // we write to the value as null so that iterators can spinwait on the value
                // that is, the null value is used to indicate the value is changing
                // the iterator would then wait until the null value is lifted
                ArrayUtil.setPlain(values, lastMigrated, null);
                VarHandle.storeStoreFence();
                ArrayUtil.setPlain(keys, lastMigrated, currKey);
                VarHandle.storeStoreFence();
                ArrayUtil.setRelease(values, lastMigrated, ArrayUtil.getPlain(values, currIndex));

                lastMigrated = currIndex;
            }
            /*
            if (currIndex < lastMigrated) {
                // we've wrapped
                if (lastMigrated >= expectedIndex && expectedIndex >= index) {

                }
            } else {
                // not wrapped
                if (lastMigrated >= expectedIndex) {
                    // the element would be reachable from this position

                }

            }*/
        }
    }

    public final V removeRelaxed(final int key) {
        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();
            this.setZeroKeyValuePlain(null);
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
                return null;
            }

            final int nextIndex = (currIndex + 1) & capacityMask;

            if (currKey != key) {
                currIndex = nextIndex;
                continue;
            }

            final V ret = ArrayUtil.getPlain(values, currIndex);

            this.removeEntryAtRelaxed(keys, values, capacityMask, currIndex);
            this.removeFromSize(1);

            return ret;
        }
    }

    public final boolean removeRelaxed(final int key, final V value) {
        Validate.notNull(value, "Value may not be null");

        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();

            if (prev != value && (prev == null || !prev.equals(value))) {
                return false;
            }

            this.setZeroKeyValuePlain(null);
            return true;
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
                return false;
            }

            final int nextIndex = (currIndex + 1) & capacityMask;

            if (currKey != key) {
                currIndex = nextIndex;
                continue;
            }

            final V ret = ArrayUtil.getPlain(values, currIndex);

            if (ret != value && !ret.equals(value)) {
                return false;
            }

            this.removeEntryAtRelaxed(keys, values, capacityMask, currIndex);
            this.removeFromSize(1);

            return true;
        }
    }

    public final boolean removeIfRelaxed(final int key, final Predicate<V> predicate) {
        Validate.notNull(predicate, "Predicate may not be null");

        if (key == 0) {
            final V prev = this.getZeroKeyValuePlain();

            if (prev == null || !predicate.test(prev)) {
                return false;
            }

            this.setZeroKeyValuePlain(null);
            return true;
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
                return false;
            }

            final int nextIndex = (currIndex + 1) & capacityMask;

            if (currKey != key) {
                currIndex = nextIndex;
                continue;
            }

            final V ret = ArrayUtil.getPlain(values, currIndex);

            if (!predicate.test(ret)) {
                return false;
            }

            this.removeEntryAtRelaxed(keys, values, capacityMask, currIndex);
            this.removeFromSize(1);

            return true;
        }
    }

    public final V computeRelaxed(final int key, final IntObjectObjectProducer<V> function) {
        Validate.notNull(function, "Function may not be null");

        if (key == 0) {
            final V curr = this.getZeroKeyValuePlain();
            final V newValue = function.produce(key, curr);

            this.setZeroKeyValuePlain(newValue);

            return newValue;
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
                final V newValue = function.produce(key, null);

                if (newValue == null) {
                    return null; // no change
                }

                ArrayUtil.setPlain(values, currIndex, newValue);
                ArrayUtil.setPlain(keys, currIndex, key);

                this.addToSizeRelaxed(1);

                return newValue;
            }

            if (currKey != key) {
                currIndex = (currIndex + 1) & capacityMask;
                continue;
            }

            final V newValue = function.produce(key, ArrayUtil.getPlain(values, currIndex));

            if (newValue != null) {
                ArrayUtil.setPlain(values, currIndex, newValue);
                return newValue;
            }

            this.removeEntryAtRelaxed(keys, values, capacityMask, currIndex);
            this.removeFromSizeRelaxed(1);

            return null;
        }
    }

    public final V computeIfAbsentRelaxed(final int key, final IntObjectProducer<V> function) {
        Validate.notNull(function, "Function may not be null");

        if (key == 0) {
            final V curr = this.getZeroKeyValuePlain();

            if (curr != null) {
                return curr;
            }

            final V newValue = function.produce(key);
            this.setZeroKeyValuePlain(newValue);

            return newValue;
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
                final V newValue = function.produce(key);

                if (newValue == null) {
                    return null; // no change
                }

                ArrayUtil.setPlain(values, currIndex, newValue);
                ArrayUtil.setPlain(keys, currIndex, key);

                this.addToSizeRelaxed(1);

                return newValue;
            }

            if (currKey != key) {
                currIndex = (currIndex + 1) & capacityMask;
                continue;
            }

            return ArrayUtil.getPlain(values, currIndex);
        }
    }

    public final V computeIfPresentRelaxed(final int key, final IntObjectObjectProducer<V> function) {
        Validate.notNull(function, "Function may not be null");

        if (key == 0) {
            final V curr = this.getZeroKeyValuePlain();

            if (curr == null) {
                return null;
            }

            final V newValue = function.produce(key, curr);
            this.setZeroKeyValuePlain(newValue);

            return newValue;
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
                return null;
            }

            if (currKey != key) {
                currIndex = (currIndex + 1) & capacityMask;
                continue;
            }

            final V newValue = function.produce(key, ArrayUtil.getPlain(values, currIndex));

            if (newValue != null) {
                ArrayUtil.setPlain(values, currIndex, newValue);
                return newValue;
            }

            this.removeEntryAtRelaxed(keys, values, capacityMask, currIndex);
            this.removeFromSizeRelaxed(1);

            return null;
        }
    }

    /*
     * On iteration:
     *
     * Since removals can shift elements to previous indices, we have to iterate over the array backwards starting at
     * the end of the first grouped entry. We could miss new additions this way, however that is OK since
     * we only guarantee that we see all entries in the map that the reader has already seen. However it
     * opens the possibility that a duplicate entry is iterated. The reason iterating backwards will work is because
     * entries are never moved forwards, only backwards by remove() (so we could never skip over one).
     */

    /* note: this can hit hard if the map is almost full... */
    protected final int findFirstGroupOpaque(final int[] keys) {
        int mask = keys.length - 1;

        for (;;) {
            if (this.getSizeOpaque() == 0) {
                return -1;
            }
            for (int i = mask; i >= 0; --i) {
                final int key = ArrayUtil.getOpaque(keys, i);
                if (key == 0) {
                    return i;
                }
            }
        }
    }

    protected final int findFirstGroupOpaque(final V[] values) {
        int mask = values.length - 1;

        for (;;) {
            if (this.getSizeOpaque() == 0) {
                return -1;
            }
            for (int i = mask; i >= 0; --i) {
                final V value = ArrayUtil.getOpaque(values, i);
                if (value == null) {
                    return i;
                }
            }
        }
    }

    public final void forEachKey(final IntConsumer action) {
        final int[] keys = this.getKeysAcquire();
        final int capacityMask = keys.length - 1;

        final int start = findFirstGroupOpaque(keys);

        if (start != -1) {
            for (int i = (start - 1) & capacityMask; i != start; i = (i - 1) & capacityMask) {
                final int value = ArrayUtil.getAcquire(keys, i);
                if (value != 0) {
                    action.accept(value);
                }
            }
        }

        final V zeroKeyValue = this.getZeroKeyValueOpaque();
        if (zeroKeyValue != null) {
            action.accept(0);
        }
    }

    public final void forEachValue(final Consumer<V> action) {
        final V[] values = this.getValuesAcquire();
        final int capacityMask = values.length - 1;

        final int start = findFirstGroupOpaque(values);

        if (start != -1) {
            for (int i = (start - 1) & capacityMask; i != start; i = (i - 1) & capacityMask) {
                final V value = ArrayUtil.getAcquire(values, i);
                if (value != null) {
                    action.accept(value);
                }
            }
        }

        final V zeroKeyValue = this.getZeroKeyValueAcquire();
        if (zeroKeyValue != null) {
            action.accept(zeroKeyValue);
        }
    }

    public final void forEachEntry(final IntObjectConsumer<V> action) {
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

        /* In order to correctly read an element, since removal is possible, we could use the mapLock */
        /* However instead we spin-wait until we read a consistent key */

        final int start = findFirstGroupOpaque(keys);

        main_loop:
        for (int i = (start - 1) & capacityMask; i != start; i = (i - 1) & capacityMask) {
            int lastKey = ArrayUtil.getOpaque(keys, i);
            V lastVal = ArrayUtil.getAcquire(values, i);
            //VarHandle.loadLoadFence(); // acquire acts as this fence here

            for (;lastKey != 0;) {
                final int keyCheck = ArrayUtil.getOpaque(keys, i);
                final V valueCheck = ArrayUtil.getAcquire(values, i);
                //VarHandle.loadLoadFence(); // acquire acts as this fence here

                final int tempKey = lastKey;
                final V tempVal = lastVal;

                lastKey = keyCheck;
                lastVal = valueCheck;

                // make sure we update key/value if they change (otherwise we would loop an extra time)
                if (tempKey != keyCheck || tempVal != valueCheck || lastVal == null) {
                    ConcurrentUtil.pause(); // TODO count failures?
                    continue;
                }

                action.accept(keyCheck, valueCheck);

                continue main_loop;
            }
        }

        final V zeroKeyValue = this.getZeroKeyValueAcquire();
        if (zeroKeyValue != null) {
            action.accept(0, zeroKeyValue);
        }
    }

    public final void forEachKeyRelaxed(final IntConsumer action) {
        final int[] keys = this.getKeysPlain();

        for (int i = 0, len = keys.length; i < len; ++i) {
            final int value = ArrayUtil.getPlain(keys, i);
            if (value != 0) {
                action.accept(value);
            }
        }

        final V zeroKeyValue = this.getZeroKeyValuePlain();
        if (zeroKeyValue != null) {
            action.accept(0);
        }
    }

    public final void forEachValueRelaxed(final Consumer<V> action) {
        final V[] values = this.getValuesPlain();

        for (int i = 0, len = values.length; i < len; ++i) {
            final V val = ArrayUtil.getPlain(values, i);
            if (val != null) {
                action.accept(val);
            }
        }

        final V zeroKeyValue = this.getZeroKeyValuePlain();
        if (zeroKeyValue != null) {
            action.accept(zeroKeyValue);
        }
    }

    public final void forEachEntryRelaxed(final IntObjectConsumer<V> action) {
        final V[] values = this.getValuesPlain();
        final int[] keys = this.getKeysPlain();

        for (int i = 0, len = values.length; i < len; ++i) {
            final int key = ArrayUtil.getPlain(keys, i);
            if (key != 0) {
                action.accept(key, ArrayUtil.getPlain(values, i));
            }
        }

        final V zeroKeyValue = this.getZeroKeyValuePlain();
        if (zeroKeyValue != null) {
            action.accept(0, zeroKeyValue);
        }
    }

    // relaxed operation
    protected static <V> void copyTable(final int[] dstKeys, final V[] dstValues, final int[] srcKeys, final V[] srcValues) {
        // presume dstKeys.length >= srcKeys.length
        final int srcLen = srcKeys.length;
        final int dstMask = dstKeys.length - 1;

        for (int i = 0; i < srcLen; ++i) {
            final int key = srcKeys[i];
            final V value = srcValues[i];
            if (key == 0) {
                continue;
            }

            final int hash = SingleWriterMultiReaderIntObjectOpenHashMap.hash(key);

            for (int currIndex = hash & dstMask;;currIndex = (currIndex + 1) & dstMask) {
                final int currKey = dstKeys[currIndex];
                if (currKey == 0) {
                    dstKeys[currIndex] = key;
                    dstValues[currIndex] = value;
                    break;
                }
            }
        }
    }

    protected static final VarHandle KEYS_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiReaderIntObjectOpenHashMap.class, "keys", int[].class);

    protected static final VarHandle VALUES_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiReaderIntObjectOpenHashMap.class, "values", Object[].class);

    protected static final VarHandle SIZE_HANDLE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiReaderIntObjectOpenHashMap.class, "size", int.class);

    protected static final VarHandle ZERO_KEY_VALUE =
            ConcurrentUtil.getVarHandle(SingleWriterMultiReaderIntObjectOpenHashMap.class, "zeroKeyValue", Object.class);

    /* keys */

    protected final int[] getKeysPlain() {
        return (int[])KEYS_HANDLE.get(this);
    }

    protected final int[] getKeysOpaque() {
        return (int[])KEYS_HANDLE.getOpaque(this);
    }

    protected final int[] getKeysAcquire() {
        return (int[])KEYS_HANDLE.getAcquire(this);
    }

    protected final void setKeysPlain(final int[] keys) {
        KEYS_HANDLE.set(this, keys);
    }

    /* values */

    @SuppressWarnings("unchecked")
    protected final V[] getValuesPlain() {
        return (V[])VALUES_HANDLE.get(this);
    }

    @SuppressWarnings("unchecked")
    protected final V[] getValuesAcquire() {
        return (V[])VALUES_HANDLE.getAcquire(this);
    }

    protected final void setValuesPlain(final V[] values) {
        VALUES_HANDLE.set(this, values);
    }

    protected final void setValuesRelease(final V[] values) {
        VALUES_HANDLE.setRelease(this, values);
    }

    /* size */

    protected final int getSizePlain() {
        return (int)SIZE_HANDLE.get(this);
    }

    protected final int getSizeOpaque() {
        return (int)SIZE_HANDLE.getOpaque(this);
    }

    protected final void setSizePlain(final int size) {
        SIZE_HANDLE.set(this, size);
    }

    protected final void setSizeOpaque(final int size) {
        SIZE_HANDLE.setOpaque(this, size);
    }

    /* zero key value */

    @SuppressWarnings("unchecked")
    protected final V getZeroKeyValuePlain() {
        return (V)ZERO_KEY_VALUE.get(this);
    }

    @SuppressWarnings("unchecked")
    protected final V getZeroKeyValueOpaque() {
        return (V)ZERO_KEY_VALUE.getOpaque(this);
    }

    @SuppressWarnings("unchecked")
    protected final V getZeroKeyValueAcquire() {
        return (V)ZERO_KEY_VALUE.getAcquire(this);
    }

    protected final void setZeroKeyValuePlain(final V value) {
        ZERO_KEY_VALUE.set(this, (Object)value);
    }

    protected final void setZeroKeyValueRelease(final V value) {
        ZERO_KEY_VALUE.setRelease(this, (Object)value);
    }

    @FunctionalInterface
    static interface IntObjectProducer<T> {

        T produce(final int value);

    }

    @FunctionalInterface
    static interface IntObjectConsumer<T> {

        void accept(final int value, final T object);

    }

    @FunctionalInterface
    static interface IntObjectObjectProducer<T> {

        T produce(final int value, final T object);

    }
}
