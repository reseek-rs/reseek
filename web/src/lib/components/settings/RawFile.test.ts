import { EditorView } from "@codemirror/view";
import { flushSync, mount, unmount } from "svelte";
import { afterEach, describe, expect, it, vi } from "vitest";

import fixture from "#lib/config/fixtures/view.json";
import { written } from "#lib/testing/fixtures.js";
import { flush } from "#lib/testing/flush.js";
import { fakeNetwork } from "#lib/testing/network.js";

import RawFile from "./RawFile.svelte";

vi.mock("$app/navigation", () => ({ beforeNavigate: () => {} }));

describe("the raw configuration file", () => {
  let component: ReturnType<typeof mount> | null = null;
  afterEach(() => {
    if (component) unmount(component);
    component = null;
    vi.unstubAllGlobals();
    document.body.innerHTML = "";
  });

  const button = (text: string) =>
    [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === text);

  const start = async (put: () => unknown) => {
    const view = structuredClone(fixture) as typeof fixture;
    (view.values as Record<string, unknown>).remote_configuration = true;
    for (const file of view.files) file.writable = true;
    const net = fakeNetwork((url, method) => {
      if (url.pathname.endsWith("/options/file") && method === "GET") {
        return {
          name: "config.toml",
          path: "/etc/reseek/config.toml",
          text: "a = 1\n",
          revision: "sha256:one"
        };
      }
      if (url.pathname.endsWith("/options/file/validate"))
        return { valid: true, errors: [], warnings: [] };
      if (url.pathname.endsWith("/options/file") && method === "PUT") return put();
      return undefined;
    });
    component = mount(RawFile, { target: document.body, props: { view: view as never } });
    await flush();
    const editor = EditorView.findFromDOM(document.querySelector<HTMLElement>(".cm-editor")!)!;
    editor.dispatch({ changes: { from: editor.state.doc.length, insert: "b = 2\n" } });
    flushSync();
    [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === "Save")!.click();
    await flush();
    return net;
  };

  it("saves the text against the revision it read and says what the reload did", async () => {
    const net = await start(() =>
      written({ revision: "sha256:two", applied: ["b"], restartRequired: ["web.port"] })
    );
    const put = net.requests.find((r) => r.method === "PUT");
    expect(put?.body).toEqual({
      text: "a = 1\nb = 2\n",
      revision: "sha256:one",
      file: "config.toml"
    });
    expect(document.body.textContent?.replace(/\s+/g, " ")).toContain(
      "Saved. 1 setting applied live."
    );
    expect(document.body.textContent).toContain("Restart reseek to apply: web.port.");
  });

  it("asks to reload a file changed on disk, and clears the refusal once it is read again", async () => {
    await start(() => ({ status: 409, body: { error: "config file changed on disk" } }));
    expect(document.body.textContent).toContain("changed on disk. Reload it");
    button("Reload")!.click();
    await flush();
    expect(document.body.textContent).not.toContain("changed on disk");
    expect(button("Reload")).toBeUndefined();
  });

  it("keeps what is typed while a save runs as a change still to save", async () => {
    let answer!: () => void;
    const net = await start(() => new Promise((resolve) => (answer = () => resolve(written()))));
    const editor = EditorView.findFromDOM(document.querySelector<HTMLElement>(".cm-editor")!)!;
    editor.dispatch({ changes: { from: editor.state.doc.length, insert: "c = 3\n" } });
    flushSync();
    answer();
    await flush();
    expect(net.requests.find((r) => r.method === "PUT")?.body).toMatchObject({
      text: "a = 1\nb = 2\n"
    });
    expect(button("Discard")!.disabled).toBe(false);
  });

  it("shows the file picked last, writable or not, whichever read answers first", async () => {
    const view = structuredClone(fixture) as typeof fixture;
    (view.values as Record<string, unknown>).remote_configuration = true;
    view.files.push({ ...view.files[0], name: "extra.toml", writable: false });
    const held: Record<string, () => void> = {};
    fakeNetwork((url, method) => {
      if (!url.pathname.endsWith("/options/file") || method !== "GET") return undefined;
      const name = url.searchParams.get("file")!;
      const file = { name, path: `/etc/reseek/${name}`, text: `# ${name}\n`, revision: name };
      // the first read of extra.toml answers only when the test says
      if (name === "extra.toml" && !held.extra) {
        return new Promise((resolve) => (held.extra = () => resolve(file)));
      }
      return file;
    });
    component = mount(RawFile, { target: document.body, props: { view: view as never } });
    await flush();
    const editor = () =>
      EditorView.findFromDOM(document.querySelector<HTMLElement>(".cm-editor")!)!;
    const pick = async (name: string) => {
      const trigger = document.querySelector<HTMLElement>('[aria-label="File"]')!;
      trigger.focus();
      trigger.dispatchEvent(new KeyboardEvent("keydown", { bubbles: true, key: "Enter" }));
      await flush();
      const option = [...document.querySelectorAll<HTMLElement>("[role=option]")].find(
        (o) => o.textContent?.trim() === name
      )!;
      option.dispatchEvent(new PointerEvent("pointerup", { bubbles: true, button: 0 }));
      option.click();
      await flush();
    };
    expect(editor().state.readOnly).toBe(false);
    await pick("extra.toml");
    await pick("config.toml");
    held.extra();
    await flush();
    expect(editor().state.doc.toString()).toBe("# config.toml\n");
    expect(editor().state.readOnly).toBe(false);
    // the read-only file once its read lands: the editor refuses typing and says which file it is
    await pick("extra.toml");
    expect(editor().state.doc.toString()).toBe("# extra.toml\n");
    expect(editor().state.readOnly).toBe(true);
    expect(editor().contentDOM.getAttribute("aria-label")).toBe("Contents of extra.toml");
    expect(button("Save")).toBeUndefined();
  });
});
