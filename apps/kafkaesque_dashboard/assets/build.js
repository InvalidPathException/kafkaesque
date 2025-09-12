const esbuild = require("esbuild");
const sveltePlugin = require("esbuild-svelte");
const sveltePreprocess = require("svelte-preprocess");
const importGlobPlugin = require("esbuild-plugin-import-glob").default;
const path = require("path");

const postcss = require("postcss");
const tailwindcss = require("tailwindcss");
const autoprefixer = require("autoprefixer");
const fs = require("fs");

const args = process.argv.slice(2);
const watch = args.includes("--watch");
const deploy = args.includes("--deploy");

const postcssPlugin = {
  name: "postcss",
  setup(build) {
    build.onLoad({ filter: /\.css$/ }, async (args) => {
      const contents = await fs.promises.readFile(args.path, "utf8");
      const result = await postcss([
        tailwindcss,
        autoprefixer
      ]).process(contents, { from: args.path });
      return {
        contents: result.css,
        loader: "css",
      };
    });
  },
};

let clientOpts = {
  entryPoints: ["js/app.js"],
  bundle: true,
  target: "es2017",
  conditions: ["svelte"],
  outdir: "../priv/static/assets",
  external: ["fonts/*", "images/*"],
  format: "iife",
  publicPath: "/assets",
  logLevel: "info",
  loader: {
    ".js": "jsx",
    ".svg": "file",
    ".png": "file",
    ".jpg": "file",
    ".gif": "file",
    ".woff": "file",
    ".woff2": "file",
    ".ttf": "file",
    ".eot": "file"
  },
  plugins: [
    postcssPlugin,
    importGlobPlugin(),
    sveltePlugin({
      preprocess: sveltePreprocess(),
      compilerOptions: {
        dev: !deploy,
        css: "injected",
        generate: "client"
      }
    })
  ]
};

let serverOpts = {
  entryPoints: ["js/server.js"],
  bundle: true,
  platform: "node",
  target: "node16",
  conditions: ["svelte", "node"],
  outdir: "../priv/server",
  format: "cjs",
  logLevel: "info",
  plugins: [
    sveltePlugin({
      preprocess: sveltePreprocess(),
      compilerOptions: {
        dev: !deploy,
        css: "injected",
        generate: "server"
      }
    })
  ]
};

if (deploy) {
  clientOpts.minify = true;
  serverOpts.minify = true;
}

if (watch) {
  clientOpts.sourcemap = "inline";
  serverOpts.sourcemap = "inline";
  
  Promise.all([
    esbuild.context(clientOpts).then((ctx) => ctx.watch()),
    esbuild.context(serverOpts).then((ctx) => ctx.watch())
  ]).catch((error) => {
    console.error(error);
    process.exit(1);
  });
} else {
  Promise.all([
    esbuild.build(clientOpts),
    esbuild.build(serverOpts)
  ]).catch((error) => {
    console.error(error);
    process.exit(1);
  });
}