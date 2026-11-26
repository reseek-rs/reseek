import { describe, expect, it } from "vitest";

import { call, reason, UNREACHABLE } from "./request";

describe("reason", () => {
  it("prefers the daemon's message and adds its detail", () => {
    expect(reason(400, { error: "invalid configuration", detail: "web.port: too big" })).toBe(
      "invalid configuration: web.port: too big"
    );
    expect(reason(409, { error: "config file changed on disk", detail: null })).toBe(
      "config file changed on disk"
    );
  });

  it("never shows an object", () => {
    expect(reason(400, { errors: { a: 1 } })).toBe("HTTP 400");
  });
});

describe("call", () => {
  it("tells a refused request from one that got no answer", async () => {
    const refused = await call(async () => ({
      error: { error: "nope" },
      response: new Response(null, { status: 403 })
    }));
    expect(refused).toEqual({ ok: false, error: "nope", status: 403, body: { error: "nope" } });
    const lost = await call(() => Promise.reject(new TypeError("Failed to fetch")));
    expect(lost).toEqual({ ok: false, error: UNREACHABLE, status: 0 });
  });

  it("takes a plain text body as the message, and never a proxy's HTML page", async () => {
    const answer = (body: string, type: string) =>
      call(async () => ({
        error: body,
        response: new Response(body, { status: 502, headers: { "Content-Type": type } })
      }));
    expect(await answer("upstream went away", "text/plain; charset=utf-8")).toMatchObject({
      error: "upstream went away"
    });
    const page = "<html><head><title>502 Bad Gateway</title></head></html>";
    expect(await answer(page, "text/html")).toMatchObject({
      error: "Server error (HTTP 502)",
      body: page
    });
  });
});
