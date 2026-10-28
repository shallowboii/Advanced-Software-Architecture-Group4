import { useState } from "react";
import { ApiError, errorMessage, mutation } from "./api";
import type { Perform } from "./actions";

interface PendingRequest {
  path: string;
  body: unknown;
  success: string;
  operator: string;
  key: string;
}
/**
 * Own operator-request state independently of the currently visible screen.
 * Network failures and server errors may follow a committed action, so retain
 * the exact key/body/operator until an explicit retry establishes its outcome.
 * This queue is browser memory only; it is not a durable command transport.
 */
export function useCommands(
  operator: string,
  onSuccess: () => void,
  onUncertain: () => void,
) {
  const [busy, setBusy] = useState(0);
  const [notice, setNotice] = useState<{ text: string; error: boolean } | null>(
    null,
  );
  const [retries, setRetries] = useState<PendingRequest[]>([]);

  /** Retry an uncertain result with the original key; never mint a new request. */
  async function execute(command: PendingRequest): Promise<boolean> {
    setBusy((value) => value + 1);
    try {
      await mutation(command.path, command.body, command.operator, command.key);
      setRetries((previous) =>
        previous.filter((item) => item.key !== command.key),
      );
      setNotice({ text: command.success, error: false });
      onSuccess();
      return true;
    } catch (error) {
      const uncertain = !(error instanceof ApiError) || error.status >= 500;
      if (uncertain) {
        setRetries((previous) =>
          previous.some((item) => item.key === command.key)
            ? previous
            : [...previous, command],
        );
        onUncertain();
      } else
        setRetries((previous) =>
          previous.filter((item) => item.key !== command.key),
        );
      setNotice({
        text: `${errorMessage(error)}${uncertain ? " Use Retry to safely repeat this exact operation." : ""}`,
        error: true,
      });
      return false;
    } finally {
      setBusy((value) => value - 1);
    }
  }
  const perform: Perform = (path, body, success) =>
    execute({ path, body, success, operator, key: crypto.randomUUID() });
  return { busy, notice, setNotice, retries, execute, perform };
}
