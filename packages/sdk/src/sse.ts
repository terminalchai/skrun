import { SkrunApiError } from "./errors.js";
import type { RunEvent } from "./types.js";

/**
 * Parse an SSE response body into an async generator of RunEvent objects.
 * Reads the response stream as text, splits on double newlines, and yields
 * parsed events. Stops after run_complete or run_error.
 */
export async function* parseSSEStream(response: Response): AsyncGenerator<RunEvent> {
  const body = response.body;
  if (!body) {
    throw SkrunApiError.streamInterrupted();
  }

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // Process complete SSE blocks (separated by \n\n)
      const blocks = buffer.split("\n\n");
      // Keep the last incomplete block in the buffer
      buffer = blocks.pop() ?? "";

      for (const block of blocks) {
        if (!block.trim()) continue;

        let eventName = "";
        let dataStr = "";

        for (const line of block.split("\n")) {
          if (line.startsWith("event: ")) eventName = line.slice(7).trim();
          else if (line.startsWith("data: ")) dataStr = line.slice(6).trim();
        }

        if (!eventName || !dataStr) continue;

        try {
          const event = JSON.parse(dataStr) as RunEvent;
          yield event;

          // Stop iteration after terminal events
          if (event.type === "run_complete" || event.type === "run_error") {
            return;
          }
        } catch {
          // Skip malformed events
        }
      }
    }

    // Process any remaining data in the buffer
    if (buffer.trim()) {
      let eventName = "";
      let dataStr = "";
      for (const line of buffer.split("\n")) {
        if (line.startsWith("event: ")) eventName = line.slice(7).trim();
        else if (line.startsWith("data: ")) dataStr = line.slice(6).trim();
      }
      if (eventName && dataStr) {
        try {
          const event = JSON.parse(dataStr) as RunEvent;
          yield event;
          if (event.type === "run_complete" || event.type === "run_error") {
            return;
          }
        } catch {
          // Skip malformed
        }
      }
    }

    // If we exit without a terminal event, the stream was interrupted
    throw SkrunApiError.streamInterrupted();
  } finally {
    reader.releaseLock();
  }
}
