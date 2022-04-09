package io.denery.concurrentutil.lock;

import ca.spottedleaf.concurrentutil.lock.SeqLock;
import org.openjdk.jcstress.annotations.State;

@State
public class SharedData {
    int a, b, c;

    public void safeWrite(SeqLock seqLock, int a, int b, int c) {
        seqLock.acquireWrite();
        try {
            this.a = a;
            this.b = b;
            this.c = c;
        } finally {
            seqLock.releaseWrite();
        }
    }

    public Trio<Integer, Integer, Integer> safeRead(SeqLock seqLock) {
        int readlock;
        Trio<Integer, Integer, Integer> ret;
        do {
            readlock = seqLock.acquireRead();
            ret = new Trio<>(this.a, this.b, this.c);
        } while (!seqLock.tryReleaseRead(readlock));
        return ret;
    }

    public record Trio<A, B, C>(A a, B b, C c) {}
}
