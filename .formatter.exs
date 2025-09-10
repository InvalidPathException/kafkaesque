[
  import_deps: [:phoenix],
  subdirectories: ["apps/*"],
  plugins: [Phoenix.LiveView.HTMLFormatter],
  inputs: [
    "*.{heex,ex,exs}",
    "{config,lib,test}/**/*.{heex,ex,exs}",
    "apps/*/lib/**/*.{heex,ex,exs}"
  ]
]
