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
        if (phaseNum > 2) throw new IllegalArgumentException("Illegal phase " + phaseNum);
    }

    public int getPhaseNum() {
        return phaseNum;
    }

    public String toString() {
        if (phaseNum == GATHER) return "GATHER";
        else if (phaseNum == APPLY) return "APPLY";
        else if (phaseNum == SCATTER) return "SCATTER";
        return "????";
    }
}
