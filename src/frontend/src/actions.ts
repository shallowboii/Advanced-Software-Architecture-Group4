/** Shared command boundary: views request actions; the app owns retries and notices. */
export type Perform = (
  path: string,
  body: unknown,
  success: string,
) => Promise<boolean>;

/** Ask the operator to confirm a concrete action before its callback runs. */
export type Confirm = (
  title: string,
  message: string,
  label: string,
  action: () => Promise<boolean>,
  destructive?: boolean,
) => void;
