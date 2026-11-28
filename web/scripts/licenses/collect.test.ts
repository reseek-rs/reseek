import { describe, expect, it } from "vitest";

import { type CargoAbout, collect, linkedCrates } from "./collect.ts";

const CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index";

const crate = (name: string, version: string, source: string | null = CRATES_IO) => ({
  crate: { name, version, source }
});

describe("the crates cargo tree lists", () => {
  it("are every registry crate once, without the workspace's own", () => {
    const tree = [
      "reseek-bin v0.1.0 (/src/reseek/crates/reseek-bin)",
      "reseek v0.1.0 (/src/reseek/crates/reseek)",
      "tokio v1.48.0",
      "",
      "tokio v1.48.0 (*)",
      "syn v2.0.119",
      "git-crate v0.3.0 (https://github.com/someone/git-crate#0123abcd)"
    ].join("\n");
    expect([...linkedCrates(tree)]).toEqual(["tokio@1.48.0", "syn@2.0.119", "git-crate@0.3.0"]);
  });
});

describe("the notices", () => {
  const mit = "Copyright (c) someone\n\nPermission is hereby granted";
  const cargo: CargoAbout = {
    licenses: [
      { id: "Apache-2.0", text: "Apache License", used_by: [crate("ryu", "1.0.0")] },
      {
        id: "MIT",
        text: mit,
        used_by: [crate("anyhow", "1.0.0"), crate("serde_derive", "1.0.0"), crate("rand", "0.10.0")]
      },
      // the same text as the others', written on windows and after a blank line
      {
        id: "MIT",
        text: `\n${mit.replaceAll("\n", "\r\n")}\r\n`,
        used_by: [crate("rand", "0.9.0")]
      },
      { id: "Unicode-3.0", text: "UNICODE LICENSE V3", used_by: [crate("anyhow", "1.0.0")] }
    ]
  };
  const linked = new Set(["anyhow@1.0.0", "rand@0.9.0", "rand@0.10.0", "ryu@1.0.0"]);
  const npm = [
    { name: "clsx", version: "2.1.1", license: "MIT", texts: [mit, "NOTICE"] },
    { name: "d3-path", version: "3.1.1", license: "ISC", texts: ["ISC License"] }
  ];

  it("group the linked crates and the bundled packages by license, keeping each text once", () => {
    expect(collect(cargo, linked, npm)).toEqual({
      texts: [mit, "NOTICE", "Apache License", "ISC License", "UNICODE LICENSE V3"],
      licenses: [
        {
          id: "MIT",
          packages: [
            { name: "anyhow", version: "1.0.0", registry: "crates.io", texts: [0] },
            { name: "clsx", version: "2.1.1", registry: "npm", texts: [0, 1] },
            // by the value of their numbers, so 0.9 comes before 0.10
            { name: "rand", version: "0.9.0", registry: "crates.io", texts: [0] },
            { name: "rand", version: "0.10.0", registry: "crates.io", texts: [0] }
          ]
        },
        {
          id: "Apache-2.0",
          packages: [{ name: "ryu", version: "1.0.0", registry: "crates.io", texts: [2] }]
        },
        {
          id: "ISC",
          packages: [{ name: "d3-path", version: "3.1.1", registry: "npm", texts: [3] }]
        },
        {
          id: "Unicode-3.0",
          packages: [{ name: "anyhow", version: "1.0.0", registry: "crates.io", texts: [4] }]
        }
      ]
    });
  });

  it("fail for a linked crate cargo-about has no license for", () => {
    expect(() => collect(cargo, new Set([...linked, "libc@0.2.0"]), [])).toThrow(/libc@0\.2\.0/);
  });

  it("fail for a crate from another source than crates.io", () => {
    const git = {
      licenses: [{ id: "MIT", text: mit, used_by: [crate("anyhow", "1.0.0", "git+https://x")] }]
    };
    expect(() => collect(git, new Set(["anyhow@1.0.0"]), [])).toThrow(/not crates\.io/);
  });
});
