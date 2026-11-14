import type { Schemas } from "#lib/api/client.js";

/**
 * The fills of the dots that tell a state by their color: online or a free slot, and away or no
 * free slot. They take the icon tones, which reach 3:1 on every surface and row in both themes
 */
export const DOTS = { success: "bg-success-icon", warning: "bg-warning-icon" } as const;

/** A presence as a dot's fill: online, away, or gray when offline or unknown */
export const presenceDot = (presence: Schemas["UserPresence"] | null | undefined) =>
  presence === "Online" ? DOTS.success : presence === "Away" ? DOTS.warning : "bg-fg-subtle/50";
