package io.denery.concurrentutil.lock;

import ca.spottedleaf.concurrentutil.lock.SeqLock;
import ca.spottedleaf.concurrentutil.lock.VolatileSeqLock;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.III_Result;

@JCStressTest
@Outcome(expect = Expect.FORBIDDEN)
@Outcome(id = "1, 3, 6", expect = Expect.ACCEPTABLE_INTERESTING, desc = "full read happened after write.")
@Outcome(id = "0, 0, 0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "full read happened before write.")
@State
public class VolatileSeqLockConsistencyTest {
    public final SeqLock volatileSeqLock = new VolatileSeqLock();
    int a, b, c;

    @Actor
    public void reader(III_Result r) {
        var trio = safeRead();
        r.r1 = trio.a;
        r.r2 = trio.b;
        r.r3 = trio.c;
    }

    @Actor
    public void writer(III_Result r) {
        safeWrite(1, 3, 6);
    }

    public void safeWrite(int a, int b, int c) {
        volatileSeqLock.acquireWrite();
        try {
            this.a = a;
            this.b = b;
            this.c = c;
        } finally {
            volatileSeqLock.releaseWrite();
        }
    }

    public Trio<Integer, Integer, Integer> safeRead() {
        int readlock;
        Trio<Integer, Integer, Integer> ret;
        do {
            readlock = volatileSeqLock.acquireRead();
            ret = new Trio<>(this.a, this.b, this.c);
        } while (!volatileSeqLock.tryReleaseRead(readlock));
        return ret;
    }

    private record Trio<A, B, C>(A a, B b, C c) {}
}
