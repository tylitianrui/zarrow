{
  description = "Development environment for zarrow";

  inputs = {
    zig-overlay.url = "github:mitchellh/zig-overlay";
    nixpkgs.follows = "zig-overlay/nixpkgs";
  };

  outputs = { nixpkgs, zig-overlay, ... }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
    in
    {
      devShells = forAllSystems (system: {
        default = nixpkgs.legacyPackages.${system}.mkShell {
          packages = [ zig-overlay.packages.${system}."0.16.0" ];
        };
      });
    };
}
