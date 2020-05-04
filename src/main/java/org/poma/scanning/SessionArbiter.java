package org.poma.scanning;

public interface SessionArbiter {

    boolean canRun(ScannerChunk chunk);
}
