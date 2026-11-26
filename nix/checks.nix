# the flake's checks besides the package's build and the Home Manager module's; `just ci` runs the
# rest, the Rust, the UI and the API contract
{
  lib,
  runCommand,
  nixfmt,
}:
let
  fs = lib.fileset;
  moduleLib = import ./module-lib.nix { inherit lib; };
  # the module helpers whose output systemd and the daemon parse
  moduleLibFailures = lib.runTests {
    testDropInNameAddsToml = {
      expr = moduleLib.dropInName "/run/agenix/reseek-oidc";
      expected = "reseek-oidc.toml";
    };
    testDropInNameKeepsToml = {
      expr = moduleLib.dropInName "/run/secrets/oidc.toml";
      expected = "oidc.toml";
    };
    testOptionalPathsQuotes = {
      expr = moduleLib.optionalPaths [
        "/mnt/media/Soulseek Downloads"
        ''/srv/a"b\c%d''
      ];
      expected = [
        ''"-/mnt/media/Soulseek Downloads"''
        ''"-/srv/a\"b\\c%%d"''
      ];
    };
  };
in
{
  module-lib = runCommand "reseek-module-lib" { } (
    if moduleLibFailures == [ ] then
      "touch $out"
    else
      "echo ${lib.escapeShellArg (builtins.toJSON moduleLibFailures)} >&2; exit 1"
  );

  fmt =
    runCommand "reseek-fmt"
      {
        src = fs.toSource {
          root = ../.;
          fileset = fs.unions [
            ../flake.nix
            ../nix
          ];
        };
        nativeBuildInputs = [ nixfmt ];
      }
      ''
        cd $src
        nixfmt --check flake.nix $(find nix -name '*.nix')
        touch $out
      '';
}
