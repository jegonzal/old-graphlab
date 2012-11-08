package org.graphlab;

/**
 *
 */
public class ExecutionPhase {
    public static final int GATHER = 0;
    public static final int APPLY  = 1;
    public static final int SCATTER = 2;

    private int phaseNum;

    public ExecutionPhase(int phaseNum) {
        this.phaseNum = phaseNum;
    }

    public int getPhaseNum() {
        return phaseNum;
    }
}
