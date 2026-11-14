import { badgeCount, formatCount, formatSpeed, NONE } from "#lib/format.js";
import type { Field } from "#lib/inspector/inspector.svelte.js";

import { QUEUE_MOST } from "./list";
import type { ResultFile, SearchResponse } from "./results";

/** A result's file as the inspector shows it, with what its user offers */
export type Picked = { file: ResultFile; username: string; facts: Field[] };

/**
 * A user's queue as the results write it: past `QUEUE_MOST` it is "99,999+", and one the user
 * doesn't say is a dash
 */
export const queueText = (queue: number | null | undefined) =>
  queue === null || queue === undefined ? NONE : badgeCount(queue, QUEUE_MOST);

/**
 * What a response says of its user's uploads: "1.2 MB/s · free slot · 3 queued", the queue
 * written as the results list's column writes it, or "queue unknown"
 */
export const userFacts = (response: SearchResponse) => {
  const queue = response.queueLength;
  const queued =
    queue === null || queue === undefined ? "queue unknown" : `${queueText(queue)} queued`;
  return `${formatSpeed(response.uploadSpeed)} · ${response.hasFreeUploadSlot ? "free slot" : "no free slot"} · ${queued}`;
};

/** What a user offers with a file, as the inspector lists it */
export const offerFields = (
  speed: number,
  free: boolean,
  queued: number | null | undefined
): Field[] => [
  { label: "Upload speed", value: formatSpeed(speed) },
  { label: "Free slot", value: free ? "Yes" : "No" },
  {
    label: "Queued",
    value: queued === null || queued === undefined ? "Unknown" : formatCount(queued)
  }
];
