import { Empty, timestamp } from "../ui";
import type { Telemetry } from "../types";

/** Plot a chronological sample window with a shared value scale and accessible summary. */
export function TelemetryChart({ samples }: { samples: Telemetry[] }) {
  if (!samples.length)
    return (
      <Empty title="No sensor readings in this window">
        Choose a machine and time range. Samples appear while a machine is
        processing.
      </Empty>
    );
  const width = 900,
    height = 230,
    left = 58,
    right = 20,
    top = 25,
    bottom = 35;
  const values = samples.map((sample) => sample.value);
  const low = Math.min(...values),
    high = Math.max(...values),
    pad = Math.max((high - low) * 0.15, 0.5);
  const minimum = low - pad,
    maximum = high + pad;
  const firstTime = Date.parse(samples[0].ts_utc),
    lastTime = Date.parse(samples[samples.length - 1].ts_utc);
  const x = (sample: Telemetry, index: number) =>
    left +
    (lastTime > firstTime
      ? (Date.parse(sample.ts_utc) - firstTime) / (lastTime - firstTime)
      : index / Math.max(1, samples.length - 1)) *
      (width - left - right);
  const y = (value: number) =>
    top +
    (1 - (value - minimum) / (maximum - minimum)) * (height - top - bottom);
  const points = samples
    .map((sample, index) => `${x(sample, index)},${y(sample.value)}`)
    .join(" ");
  const latest = samples[samples.length - 1];
  return (
    <div className="telemetry-chart">
      <div className="chart-reading">
        <strong>
          {latest.value.toFixed(2)} <small>{latest.unit}</small>
        </strong>
        <span>Latest reading · {timestamp(latest.ts_utc)}</span>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${latest.sensor} for ${latest.machine_id}: ${samples.length} samples, minimum ${low}, maximum ${high} ${latest.unit}`}
      >
        <defs>
          <linearGradient id="chart-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#81c4ac" stopOpacity="0.2" />
            <stop offset="100%" stopColor="#81c4ac" stopOpacity="0" />
          </linearGradient>
        </defs>
        {Array.from({ length: 5 }, (_, index) => {
          const value = minimum + ((maximum - minimum) * index) / 4;
          return (
            <g key={index}>
              <line
                x1={left}
                x2={width - right}
                y1={y(value)}
                y2={y(value)}
                stroke="#2b3531"
                strokeDasharray="4 5"
              />
              <text
                x={left - 12}
                y={y(value) + 4}
                textAnchor="end"
                fill="#7e9188"
                fontSize="11"
              >
                {value.toFixed(1)}
              </text>
            </g>
          );
        })}
        <polygon
          points={`${left},${height - bottom} ${points} ${x(latest, samples.length - 1)},${height - bottom}`}
          fill="url(#chart-area)"
        />
        <polyline
          points={points}
          fill="none"
          stroke="#8bcfb5"
          strokeWidth="2.5"
          strokeLinejoin="round"
        />
        <circle
          cx={x(latest, samples.length - 1)}
          cy={y(latest.value)}
          r="4"
          fill="#8bcfb5"
        />
        <text x={left} y={height - 6} fill="#7e9188" fontSize="11">
          {timestamp(samples[0].ts_utc)}
        </text>
        <text
          x={width - right}
          y={height - 6}
          textAnchor="end"
          fill="#7e9188"
          fontSize="11"
        >
          {timestamp(latest.ts_utc)}
        </text>
      </svg>
    </div>
  );
}
