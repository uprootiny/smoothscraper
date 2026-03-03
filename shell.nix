{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  name = "smoothscraper-shell";
  buildInputs = [
    pkgs.rustfmt
    pkgs.cargo
    pkgs.rustc
    pkgs."pkg-config"
    pkgs.openssl
    pkgs.zlib
    pkgs.libssh
  ];
  shellHook = ''
    echo "Entering smoothscraper nix-shell"
    export CARGO_TARGET_DIR=$(pwd)/target
    export RUSTFLAGS="-Ctarget-cpu=native"
  '';
}
