package io.denery.concurrentutil.lock;

import ca.spottedleaf.concurrentutil.lock.SeqLock;
import ca.spottedleaf.concurrentutil.lock.VolatileSeqLock;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.III_Result;

@JCStressTest
@Outcome(expect = Expect.FORBIDDEN)
@Outcome(id = "0, 0, 0", expect = Expect.ACCEPTABLE, desc = "read happened before any writes.")
@Outcome(id = "1, 3, 6", expect = Expect.ACCEPTABLE, desc = "first writer happened before read.")
@Outcome(id = "2, 4, 8", expect = Expect.ACCEPTABLE, desc = "second writer happened before read.")
public class VolatileSeqLockMultiWriterTest {
    public final SeqLock volatileSeqLock = new VolatileSeqLock();
    @Actor
    public void writer(SharedData sharedData) {
        sharedData.safeWrite(volatileSeqLock,1, 3, 6);
    }

    @Actor
    public void writer1(SharedData sharedData) {
        sharedData.safeWrite(volatileSeqLock,2, 4, 8);
    }

    @Actor
    public void reader(SharedData sharedData, III_Result r) {
        var trio = sharedData.safeRead(volatileSeqLock);
        r.r1 = trio.a();
        r.r2 = trio.b();
        r.r3 = trio.c();
    }
}

