package io.denery.concurrentutil.lock;

import ca.spottedleaf.concurrentutil.lock.SeqLock;
import ca.spottedleaf.concurrentutil.lock.WeakSeqLock;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.III_Result;

@JCStressTest
@Outcome(expect = Expect.FORBIDDEN)
@Outcome(id = "1, 3, 6", expect = Expect.ACCEPTABLE, desc = "full read happened after write.")
@Outcome(id = "0, 0, 0", expect = Expect.ACCEPTABLE, desc = "full read happened before write.")
public class WeakSeqLockConsistencyTest {
    public final SeqLock weakSeqLock = new WeakSeqLock();

    @Actor
    public void reader(SharedData sharedData, III_Result r) {
        var trio = sharedData.safeRead(weakSeqLock);
        r.r1 = trio.a();
        r.r2 = trio.b();
        r.r3 = trio.c();
    }

    @Actor
    public void writer(SharedData sharedData) {
        sharedData.safeWrite(weakSeqLock, 1, 3, 6);
    }
}
