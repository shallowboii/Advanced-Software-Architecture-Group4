package dk.sdu.beerfactory;

import java.util.List;
import java.util.Set;

/** Invariants shared by orchestration and dependency-free regression tests. */
public final class Domain {
  public static final List<String> STAGES =
      List.of("mash", "lauter", "kettle", "whirlpool", "unitank", "filter");
  public static final Set<String> TERMINAL = Set.of("COMPLETED", "DISCARDED");

  /** Prevents instantiation of this collection of pure domain predicates. */
  private Domain() {}

  /** Identifies an already completed or discarded batch whose outcome cannot change. */
  public static boolean terminal(String status) {
    return TERMINAL.contains(status);
  }

  /** Applies line, machine, stock and policy admission guards without side effects. */
  public static boolean canAdmit(
      String policy, int wip, int cap, boolean stopped, boolean free, boolean stock) {
    if (!Set.of("PUSH", "CONWIP").contains(policy) || cap <= 0 || wip < 0)
      throw new IllegalArgumentException("invalid policy/cap");
    return !stopped && free && stock && (policy.equals("PUSH") || wip < cap);
  }

  /**
   * Checks both central machine ownership and stage execution identity before applying a delayed
   * result.
   */
  public static boolean validOwner(
      String actualMachine, String execution, String machine, String incomingExecution) {
    return actualMachine != null
        && actualMachine.equals(machine)
        && execution != null
        && execution.equals(incomingExecution);
  }

  /** Allows final output only at the last stage of a viable nonterminal batch. */
  public static boolean canComplete(int stage, String status, boolean lost) {
    return stage == 5 && !terminal(status) && !lost;
  }

  /** Requires both physical transfer ends to be ready before the durable commit decision. */
  public static boolean canCommitTransfer(
      boolean sourceReady, boolean targetReady, String status, boolean lost) {
    return sourceReady && targetReady && status.equals("EXECUTING") && !lost;
  }

  /** Validates a strictly positive finite duration or scale. */
  public static void positiveFinite(double x, String name) {
    if (!Double.isFinite(x) || x <= 0)
      throw new IllegalArgumentException(name + " must be finite and positive");
  }

  /** Validates a finite nonnegative ingredient quantity. */
  public static void nonnegativeFinite(double x, String name) {
    if (!Double.isFinite(x) || x < 0)
      throw new IllegalArgumentException(name + " must be finite and nonnegative");
  }
}
