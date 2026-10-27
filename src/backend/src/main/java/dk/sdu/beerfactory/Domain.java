package dk.sdu.beerfactory;

import java.util.List;
import java.util.Set;

/** Invariants shared by orchestration and dependency-free regression tests. */
public final class Domain {
    public static final List<String> STAGES = List.of("mash", "lauter", "kettle", "whirlpool", "unitank", "filter");
    public static final Set<String> TERMINAL = Set.of("COMPLETED", "DISCARDED");
    private Domain() {}
    public static boolean terminal(String status) { return TERMINAL.contains(status); }
    public static boolean canAdmit(String policy, int wip, int cap, boolean stopped, boolean free, boolean stock) {
        if (!Set.of("PUSH", "CONWIP").contains(policy) || cap <= 0 || wip < 0) throw new IllegalArgumentException("invalid policy/cap");
        return !stopped && free && stock && (policy.equals("PUSH") || wip < cap);
    }
    public static boolean validOwner(String actualMachine, String execution, String machine, String incomingExecution) {
        return actualMachine != null && actualMachine.equals(machine) && execution != null && execution.equals(incomingExecution);
    }
    public static boolean canComplete(int stage, String status, boolean lost) {
        return stage == 5 && !terminal(status) && !lost;
    }
    public static boolean canCommitTransfer(boolean sourceReady, boolean targetReady, String status, boolean lost) {
        return sourceReady && targetReady && status.equals("EXECUTING") && !lost;
    }
    public static void positiveFinite(double x, String name) {
        if (!Double.isFinite(x) || x <= 0) throw new IllegalArgumentException(name + " must be finite and positive");
    }
    public static void nonnegativeFinite(double x, String name) {
        if (!Double.isFinite(x) || x < 0) throw new IllegalArgumentException(name + " must be finite and nonnegative");
    }
}
