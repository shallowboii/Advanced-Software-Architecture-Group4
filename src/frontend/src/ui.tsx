import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";

const paths: Record<string, ReactNode> = {
  grid: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </>
  ),
  orders: (
    <>
      <path d="M8 4H5v17h14V4h-3" />
      <rect x="8" y="2" width="8" height="4" rx="1" />
      <path d="M8 11h8M8 16h6" />
    </>
  ),
  recipe: (
    <>
      <path d="M9 3h6M10 3v6l-6 10a1.5 1.5 0 0 0 1.3 2h13.4a1.5 1.5 0 0 0 1.3-2L14 9V3M7 15h10" />
    </>
  ),
  chart: (
    <>
      <path d="M3 3v18h18M6 15l4-5 4 3 6-8" />
    </>
  ),
  box: (
    <>
      <path d="m12 3 9 5v9l-9 5-9-5V8l9-5Z" />
      <path d="m3 8 9 5 9-5M12 13v9M7 5.7l9 5" />
    </>
  ),
  experiment: (
    <>
      <path d="M12 3v18M3 12h18" />
      <circle cx="12" cy="12" r="7" />
      <circle cx="12" cy="12" r="2" />
    </>
  ),
  plus: <path d="M12 5v14M5 12h14" />,
  arrow: <path d="M5 12h14m-6-6 6 6-6 6" />,
  close: <path d="m6 6 12 12M18 6 6 18" />,
  refresh: (
    <>
      <path d="M20 7v5h-5M4 17v-5h5" />
      <path d="M6 7a7 7 0 0 1 11.5-2L20 8M4 16l2.5 3A7 7 0 0 0 18 17" />
    </>
  ),
  stop: <rect x="5" y="5" width="14" height="14" rx="2" />,
  play: <path d="m8 4 12 8-12 8V4Z" />,
  warning: (
    <>
      <path d="m12 3 10 18H2L12 3Z" />
      <path d="M12 9v5M12 17v.1" />
    </>
  ),
  drop: <path d="M12 2C9 6 5 10 5 15a7 7 0 0 0 14 0c0-5-4-9-7-13Z" />,
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  check: <path d="m5 12 4 4L19 6" />,
  download: (
    <>
      <path d="M12 3v12m-4-4 4 4 4-4M4 17v4h16v-4" />
    </>
  ),
};

/** Render local SVG symbols without loading third-party assets or altering button accessible names. */
export function Icon({ name, size = 18 }: { name: string; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {paths[name] ?? paths.grid}
    </svg>
  );
}

/** Explain a missing-data state without inventing production values. */
export function Empty({
  title,
  children,
}: {
  title: string;
  children?: ReactNode;
}) {
  return (
    <div className="empty-state">
      <span className="empty-icon">
        <Icon name="grid" size={24} />
      </span>
      <strong>{title}</strong>
      {children && <p>{children}</p>}
    </div>
  );
}

/** Use a native modal for focus containment and explicit escape/close handling. */
export function Dialog({
  title,
  description,
  children,
  onClose,
  wide = false,
  sheet = false,
}: {
  title: string;
  description?: string;
  children: ReactNode;
  onClose: () => void;
  wide?: boolean;
  sheet?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const id = useId();
  useEffect(() => {
    ref.current?.showModal();
  }, []);
  return (
    <dialog
      ref={ref}
      className={`dialog ${wide ? "wide" : ""} ${sheet ? "sheet" : ""}`}
      aria-labelledby={id}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      <div className="dialog-heading">
        <div>
          <h2 id={id}>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        <button
          className="icon-button"
          aria-label="Close dialog"
          onClick={onClose}
        >
          <Icon name="close" />
        </button>
      </div>
      {children}
    </dialog>
  );
}

/** Share numeric entry rules; a blank field remains invalid instead of silently becoming zero. */
export function NumberField({
  label,
  value,
  onChange,
  min = 0,
  max,
  step = "any",
  suffix,
}: {
  label: string;
  value: number;
  onChange: (value: number) => void;
  min?: number;
  max?: number;
  step?: string;
  suffix?: string;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <span className="input-with-suffix">
        <input
          required
          type="number"
          min={min}
          max={max}
          step={step}
          value={Number.isNaN(value) ? "" : value}
          onChange={(event) =>
            onChange(
              event.target.value === "" ? NaN : Number(event.target.value),
            )
          }
        />
        {suffix && <small>{suffix}</small>}
      </span>
    </label>
  );
}

/** Render recorded wall-clock timestamps and distinguish missing values from an actual date. */
export function timestamp(value: string | undefined): string {
  if (!value || !Number.isFinite(Date.parse(value))) return "Not recorded";
  return new Date(value).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** Abbreviate long display identifiers; callers retain the original ID for actions and tooltips. */
export function shortId(value: string | null | undefined): string {
  if (!value) return "None";
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}
