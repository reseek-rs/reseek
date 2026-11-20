# the Soulseek server the client and daemon test suites run against; nothing in reseek links to it
{
  lib,
  buildDubPackage,
  fetchFromGitHub,
  pkg-config,
  sqlite,
}:
buildDubPackage {
  pname = "soulfind";
  version = "0-unstable-2026-11-19";

  src = fetchFromGitHub {
    owner = "soulfind-dev";
    repo = "soulfind";
    rev = "e5e99617ab9ff4dc0dd26a3f3fb314103ff35777";
    hash = "sha256-yL/9UX2NFQAUwKQi2x72S95fXYsvP6PBtVVM7AUmGa8=";
  };

  dubLock.dependencies = { };

  nativeBuildInputs = [ pkg-config ];
  buildInputs = [ sqlite ];

  installPhase = ''
    runHook preInstall
    install -Dm755 bin/soulfind bin/soulsetup -t $out/bin
    runHook postInstall
  '';

  meta = {
    description = "Soulseek server for local testing";
    homepage = "https://github.com/soulfind-dev/soulfind";
    license = lib.licenses.gpl3Plus;
    mainProgram = "soulfind";
  };
}
